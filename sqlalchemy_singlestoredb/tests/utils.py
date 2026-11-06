"""Helpers shared by the test fixtures."""
from __future__ import annotations

import logging
import time
from typing import Any
from typing import Callable
from typing import TypeVar

import singlestoredb
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.engine import URL


logger = logging.getLogger(__name__)

T = TypeVar('T')

# A connect to the shared CI cluster occasionally times out (2003, can't
# connect) while the next one, a second later, succeeds. Retrying keeps that
# from failing a whole smoke-test job. Only opening a connection is retried,
# and only on OperationalError and InterfaceError; those include a rejected
# password, which then fails a few seconds later than it would have.
CONNECT_TRIES = 5
CONNECT_RETRY_DELAY = 2.0


def connect_with_retry(connect: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Call ``connect(*args, **kwargs)``, retrying connection errors."""
    for attempt in range(1, CONNECT_TRIES + 1):
        try:
            return connect(*args, **kwargs)
        except (singlestoredb.OperationalError, singlestoredb.InterfaceError) as exc:
            if attempt == CONNECT_TRIES:
                raise
            logger.warning(
                'Connect attempt %d of %d failed, retrying: %s',
                attempt, CONNECT_TRIES, exc,
            )
            time.sleep(CONNECT_RETRY_DELAY)
    raise AssertionError('unreachable')


def create_test_engine(url: str | URL) -> Engine:
    """Create an engine whose new connections are retried on connect errors."""
    engine = create_engine(url)

    # do_connect wraps every DBAPI connection the pool opens, and sees the
    # driver's own exception before SQLAlchemy wraps it
    @event.listens_for(engine, 'do_connect')
    def retry_connect(
        dialect: Any, conn_rec: Any, cargs: Any, cparams: Any,
    ) -> Any:
        return connect_with_retry(dialect.connect, *cargs, **cparams)

    return engine
