"""Tests for the connection retries the test fixtures rely on."""
from __future__ import annotations

from typing import Any

import pytest
import singlestoredb
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.engine import Engine

from sqlalchemy_singlestoredb.tests import utils
from sqlalchemy_singlestoredb.tests.utils import CONNECT_TRIES
from sqlalchemy_singlestoredb.tests.utils import connect_with_retry
from sqlalchemy_singlestoredb.tests.utils import create_test_engine


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(utils, 'CONNECT_RETRY_DELAY', 0)


def flaky(failures: int, result: Any = 'connected') -> Any:
    """Return a connect function that fails ``failures`` times, then succeeds."""
    calls = []

    def connect(*args: Any, **kwargs: Any) -> Any:
        calls.append((args, kwargs))
        if len(calls) <= failures:
            raise singlestoredb.OperationalError(2003, "Can't connect (timed out)")
        return result

    connect.calls = calls  # type: ignore[attr-defined]
    return connect


def test_retries_until_connected() -> None:
    connect = flaky(2)
    assert connect_with_retry(connect, 'url', timeout=5) == 'connected'
    assert connect.calls == [(('url',), {'timeout': 5})] * 3


def test_gives_up_after_last_try() -> None:
    connect = flaky(CONNECT_TRIES)
    with pytest.raises(singlestoredb.OperationalError):
        connect_with_retry(connect)
    assert len(connect.calls) == CONNECT_TRIES


def test_does_not_retry_other_errors() -> None:
    calls = []

    def connect() -> None:
        calls.append(1)
        raise singlestoredb.ProgrammingError(1064, 'syntax error')

    with pytest.raises(singlestoredb.ProgrammingError):
        connect_with_retry(connect)
    assert len(calls) == 1


def test_engine_retries_dropped_connect(
    test_engine: Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_test_engine(test_engine.url)
    try:
        real_connect = engine.dialect.connect
        calls = []

        def connect(*cargs: Any, **cparams: Any) -> Any:
            calls.append(1)
            if len(calls) <= 2:
                raise singlestoredb.OperationalError(2003, "Can't connect (timed out)")
            return real_connect(*cargs, **cparams)

        monkeypatch.setattr(engine.dialect, 'connect', connect)
        with engine.connect() as conn:
            assert conn.execute(text('SELECT 1')).scalar() == 1
        assert len(calls) == 3
    finally:
        engine.dispose()


def test_engine_gives_up_on_unreachable_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Nothing listens on port 1, so every attempt fails the way a dropped
    # connect does in CI: 2003, can't connect
    engine = create_test_engine('singlestoredb://user:pw@127.0.0.1:1/db')
    try:
        real_connect = engine.dialect.connect
        calls = []

        def connect(*cargs: Any, **cparams: Any) -> Any:
            calls.append(1)
            return real_connect(*cargs, **cparams)

        monkeypatch.setattr(engine.dialect, 'connect', connect)
        with pytest.raises(sqlalchemy.exc.OperationalError):
            engine.connect()
        assert len(calls) == CONNECT_TRIES
    finally:
        engine.dispose()
