"""Tests for transaction handling and session settings of the dialect."""
from __future__ import annotations

from typing import Any
from typing import List
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine import make_url

from sqlalchemy_singlestoredb.base import SingleStoreDBDialect


def _connect_params(url: str) -> dict[str, Any]:
    return SingleStoreDBDialect().create_connect_args(make_url(url))[1]


class TestConnectArgs:
    """The DB-API connection must start outside autocommit mode."""

    def test_mysql_protocol_disables_autocommit(self) -> None:
        params = _connect_params('singlestoredb://user:pw@db.example.com:3306/x')
        assert params['autocommit'] is False

    def test_explicit_autocommit_in_url_is_kept(self) -> None:
        params = _connect_params(
            'singlestoredb://user:pw@db.example.com:3306/x?autocommit=true',
        )
        assert params['autocommit'] is True

    @pytest.mark.parametrize('scheme', ['http', 'https'])
    def test_http_data_api_keeps_driver_default(self, scheme: str) -> None:
        params = _connect_params(
            f'singlestoredb+{scheme}://user:pw@db.example.com:9000/x',
        )
        assert params['driver'] == scheme
        assert params['autocommit'] is True


class TestIsolationLevel:
    """``isolation_level='AUTOCOMMIT'`` toggles the DB-API autocommit flag."""

    def test_autocommit_is_a_valid_level(self) -> None:
        dialect = SingleStoreDBDialect()
        assert 'AUTOCOMMIT' in dialect.get_isolation_level_values(None)

    def test_autocommit_level(self) -> None:
        conn = MagicMock(connection_params={'driver': 'mysql', 'host': 'h'})
        SingleStoreDBDialect().set_isolation_level(conn, 'AUTOCOMMIT')
        conn.autocommit.assert_called_once_with(True)
        conn.cursor.assert_not_called()

    def test_other_level_disables_autocommit(self) -> None:
        conn = MagicMock(connection_params={'driver': 'mysql', 'host': 'h'})
        SingleStoreDBDialect().set_isolation_level(conn, 'READ COMMITTED')
        conn.autocommit.assert_called_once_with(False)
        executed = [c.args[0] for c in conn.cursor.return_value.execute.call_args_list]
        assert executed[0] == 'SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED'


class TestOnConnect:
    """The dialect must not replace the server's configured session state."""

    def test_on_connect_keeps_server_sql_mode(self) -> None:
        on_connect = SingleStoreDBDialect().on_connect()
        conn = MagicMock()
        if on_connect is not None:
            on_connect(conn)
        executed: List[str] = [
            c.args[0] for c in conn.cursor.return_value.execute.call_args_list
        ]
        assert not [s for s in executed if 'sql_mode' in s.lower()]


def _is_http(engine: Engine) -> bool:
    return engine.url.get_driver_name() in ('http', 'https')


@pytest.fixture
def tx_table(
    test_engine: Engine, table_name_prefix: str, clean_tables: None,
) -> str:
    if _is_http(test_engine):
        pytest.skip('The HTTP Data API does not support transactions')
    name = f'{table_name_prefix}tx'
    with test_engine.begin() as conn:
        conn.execute(text(f'CREATE ROWSTORE TABLE {name} (id INT PRIMARY KEY)'))
    return name


def _ids(conn: Any, table: str) -> List[int]:
    return list(conn.execute(text(f'SELECT id FROM {table} ORDER BY id')).scalars())


class TestTransactions:
    """Live transaction semantics over the MySQL protocol."""

    def test_rollback_discards_changes(self, test_engine: Engine, tx_table: str) -> None:
        with test_engine.connect() as conn:
            conn.execute(text(f'INSERT INTO {tx_table} VALUES (1)'))
            with test_engine.connect() as other:
                assert _ids(other, tx_table) == []
            conn.rollback()
        with test_engine.connect() as conn:
            assert _ids(conn, tx_table) == []

    def test_failed_begin_block_is_rolled_back(
        self, test_engine: Engine, tx_table: str,
    ) -> None:
        with pytest.raises(Exception):
            with test_engine.begin() as conn:
                conn.execute(text(f'INSERT INTO {tx_table} VALUES (1)'))
                conn.execute(text(f'INSERT INTO {tx_table} VALUES (1)'))
        with test_engine.connect() as conn:
            assert _ids(conn, tx_table) == []

    def test_commit_persists_changes(self, test_engine: Engine, tx_table: str) -> None:
        with test_engine.begin() as conn:
            conn.execute(text(f'INSERT INTO {tx_table} VALUES (1)'))
        with test_engine.connect() as conn:
            assert _ids(conn, tx_table) == [1]

    def test_autocommit_isolation_level(
        self, test_engine: Engine, tx_table: str,
    ) -> None:
        auto = test_engine.execution_options(isolation_level='AUTOCOMMIT')
        with auto.connect() as conn:
            conn.execute(text(f'INSERT INTO {tx_table} VALUES (1)'))
            with test_engine.connect() as other:
                assert _ids(other, tx_table) == [1]
        # The pooled connection is back in transactional mode afterwards.
        with test_engine.connect() as conn:
            conn.execute(text(f'INSERT INTO {tx_table} VALUES (2)'))
            conn.rollback()
            assert _ids(conn, tx_table) == [1]


def test_session_sql_mode_matches_server(test_engine: Engine) -> None:
    if _is_http(test_engine):
        pytest.skip('The HTTP Data API applies its own per-request sql_mode')
    with test_engine.connect() as conn:
        session, server = conn.execute(
            text('SELECT @@SESSION.sql_mode, @@GLOBAL.sql_mode'),
        ).one()
    assert session == server


def test_engine_isolation_level_autocommit(test_engine: Engine) -> None:
    if _is_http(test_engine):
        pytest.skip('The HTTP Data API does not support transactions')
    engine = create_engine(test_engine.url, isolation_level='AUTOCOMMIT')
    try:
        with engine.connect() as conn:
            assert conn.exec_driver_sql('SELECT @@autocommit').scalar() == 1
    finally:
        engine.dispose()
