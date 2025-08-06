#!/usr/bin/env python
"""Basic SingleStoreDB connection testing using pytest fixtures."""
from __future__ import annotations

import os
from typing import Generator

import pytest
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.engine import Engine


@pytest.fixture(scope='function')
def test_data_loaded(
    test_engine: Engine, table_name_prefix: str,
) -> Generator[dict[str, str], None, None]:
    """Load test data from test.sql file with randomized table names."""
    # Get path to test.sql file
    sql_file = os.path.join(os.path.dirname(__file__), 'test.sql')

    # Read and execute test.sql with table name replacements
    with open(sql_file) as f:
        sql_content = f.read()

    # Create mapping of original table names to randomized names
    table_mapping = {
        'data': f'{table_name_prefix}data',
        'alltypes': f'{table_name_prefix}alltypes',
    }

    # Replace table names in SQL content
    modified_sql = sql_content
    for original_name, new_name in table_mapping.items():
        # Replace table references in various contexts
        modified_sql = modified_sql.replace(f'TABLE {original_name}', f'TABLE {new_name}')
        modified_sql = modified_sql.replace(f'INTO {original_name}', f'INTO {new_name}')
        modified_sql = modified_sql.replace(f'FROM {original_name}', f'FROM {new_name}')
        modified_sql = modified_sql.replace(
            f'TABLE IF NOT EXISTS {original_name}', f'TABLE IF NOT EXISTS {new_name}',
        )

    # Execute the SQL statements
    with test_engine.connect() as conn:
        with conn.begin():
            # Split on COMMIT statements and execute each block
            statements = modified_sql.split('COMMIT;')
            for stmt_block in statements:
                if stmt_block.strip():
                    # Split individual statements and execute them
                    for stmt in stmt_block.split(';'):
                        stmt = stmt.strip()
                        if stmt and not stmt.startswith('--'):
                            try:
                                conn.execute(text(stmt))
                            except Exception:
                                # Some statements might fail, continue
                                pass

    yield table_mapping

    # Cleanup is handled by clean_tables fixture


class TestBasics:
    """Basic SingleStoreDB connection tests using pytest fixtures."""

    def test_connection(self, test_engine: Engine, test_database: str) -> None:
        """Test that we can connect to the test database."""
        with test_engine.connect() as conn:
            dbs = [x[0] for x in list(conn.execute(text('show databases')))]
            assert test_database in dbs, f'Database {test_database} not found in {dbs}'

    def test_deferred_connection(self, base_connection_url: str) -> None:
        """Test deferred connection functionality."""
        # Skip this test if using Docker (no external URL to defer to)
        if not os.environ.get('SINGLESTOREDB_URL'):
            pytest.skip('Test requires SINGLESTOREDB_URL for deferred connection')

        url = os.environ['SINGLESTOREDB_URL']
        if '://' in url:
            scheme = url.split('://', 1)[0]
            if 'singlestoredb' not in scheme:
                scheme = f'singlestoredb+{scheme}'
        else:
            scheme = 'singlestoredb'

        # Temporarily remove URL to test deferred connection
        try:
            del os.environ['SINGLESTOREDB_URL']
            eng = sa.create_engine(f'{scheme}://:@singlestore.com')
            conn = eng.connect()
            with pytest.raises(sa.exc.InterfaceError):
                conn.execute(text('show databases'))

            # Restore URL and test connection works
            os.environ['SINGLESTOREDB_URL'] = url
            out = conn.execute(text('show databases'))
            assert len(out.fetchall()) > 0
            conn.close()
        finally:
            # Ensure URL is restored
            os.environ['SINGLESTOREDB_URL'] = url

    def test_alltypes(
        self,
        test_engine: Engine,
        test_data_loaded: dict[str, str],
        clean_tables: None,
    ) -> None:
        """Test reflection of all SingleStore data types."""
        # Get the randomized table name
        alltypes_table_name = test_data_loaded['alltypes']

        meta = sa.MetaData()
        tbl = sa.Table(alltypes_table_name, meta)
        insp = sa.inspect(test_engine)
        insp.reflect_table(tbl, None)

        cols = {col.name: col for col in tbl.columns}

        def dtype(col: sa.Column) -> str:
            return col.type.__class__.__name__.lower()

        assert dtype(cols['id']) == 'integer', dtype(cols['id'])
        assert cols['id'].nullable is True
        assert cols['id'].type.unsigned is False

        assert dtype(cols['tinyint']) == 'tinyint', dtype(cols['tinyint'])
        assert cols['tinyint'].nullable is True
        assert cols['tinyint'].type.unsigned is False

        assert dtype(cols['unsigned_tinyint']) == 'tinyint', dtype(
            cols['unsigned_tinyint'],
        )
        assert cols['unsigned_tinyint'].nullable is True
        assert cols['unsigned_tinyint'].type.unsigned is True

        assert dtype(cols['bool']) == 'tinyint', dtype(cols['bool'])
        assert cols['bool'].nullable is True
        assert cols['bool'].type.unsigned is False
        assert cols['bool'].type.display_width == 1

        assert dtype(cols['boolean']) == 'tinyint', dtype(cols['boolean'])
        assert cols['boolean'].nullable is True
        assert cols['boolean'].type.unsigned is False
        assert cols['boolean'].type.display_width == 1

        assert dtype(cols['smallint']) == 'smallint', dtype(cols['smallint'])
        assert cols['smallint'].nullable is True
        assert cols['smallint'].type.unsigned is False

        assert dtype(cols['unsigned_smallint']) == 'smallint', dtype(
            cols['unsigned_smallint'],
        )
        assert cols['unsigned_smallint'].nullable is True
        assert cols['unsigned_smallint'].type.unsigned is True

        assert dtype(cols['mediumint']) == 'mediumint', dtype(cols['mediumint'])
        assert cols['mediumint'].nullable is True
        assert cols['mediumint'].type.unsigned is False

        assert dtype(cols['unsigned_mediumint']) == 'mediumint', dtype(
            cols['unsigned_mediumint'],
        )
        assert cols['unsigned_mediumint'].nullable is True
        assert cols['unsigned_mediumint'].type.unsigned is True

        assert dtype(cols['int24']) == 'mediumint', dtype(cols['int24'])
        assert cols['int24'].nullable is True
        assert cols['int24'].type.unsigned is False

        assert dtype(cols['unsigned_int24']) == 'mediumint', dtype(cols['unsigned_int24'])
        assert cols['unsigned_int24'].nullable is True
        assert cols['unsigned_int24'].type.unsigned is True

        assert dtype(cols['int']) == 'integer', dtype(cols['int'])
        assert cols['int'].nullable is True
        assert cols['int'].type.unsigned is False

        assert dtype(cols['unsigned_int']) == 'integer', dtype(cols['unsigned_int'])
        assert cols['unsigned_int'].nullable is True
        assert cols['unsigned_int'].type.unsigned is True

        assert dtype(cols['integer']) == 'integer', dtype(cols['integer'])
        assert cols['integer'].nullable is True
        assert cols['integer'].type.unsigned is False

        assert dtype(cols['unsigned_integer']) == 'integer', dtype(
            cols['unsigned_integer'],
        )
        assert cols['unsigned_integer'].nullable is True
        assert cols['unsigned_integer'].type.unsigned is True

        assert dtype(cols['bigint']) == 'bigint', dtype(cols['bigint'])
        assert cols['bigint'].nullable is True
        assert cols['bigint'].type.unsigned is False

        assert dtype(cols['unsigned_bigint']) == 'bigint', dtype(cols['unsigned_bigint'])
        assert cols['unsigned_bigint'].nullable is True
        assert cols['unsigned_bigint'].type.unsigned is True

        assert dtype(cols['float']) == 'float', dtype(cols['float'])
        assert cols['float'].nullable is True

        assert dtype(cols['double']) == 'double', dtype(cols['double'])
        assert cols['double'].nullable is True

        assert dtype(cols['real']) == 'double', dtype(cols['real'])
        assert cols['real'].nullable is True

        assert dtype(cols['decimal']) == 'decimal', dtype(cols['decimal'])
        assert cols['decimal'].nullable is True

        assert dtype(cols['dec']) == 'decimal', dtype(cols['dec'])
        assert cols['dec'].nullable is True

        assert dtype(cols['fixed']) == 'decimal', dtype(cols['fixed'])
        assert cols['fixed'].nullable is True

        assert dtype(cols['numeric']) == 'decimal', dtype(cols['numeric'])
        assert cols['numeric'].nullable is True

        assert dtype(cols['date']) == 'date', dtype(cols['date'])
        assert cols['date'].nullable is True

        assert dtype(cols['time']) == 'time', dtype(cols['time'])
        assert cols['time'].nullable is True
        assert cols['time'].type.fsp is None, cols['time'].type.fsp

        assert dtype(cols['time_6']) == 'time', dtype(cols['time_6'])
        assert cols['time_6'].nullable is True
        assert cols['time_6'].type.fsp == 6, cols['time_6'].type.fsp

        assert dtype(cols['datetime']) == 'datetime', dtype(cols['datetime'])
        assert cols['datetime'].nullable is True
        assert cols['datetime'].type.fsp is None, cols['datetime'].type.fsp

        assert dtype(cols['datetime_6']) == 'datetime', dtype(cols['datetime_6'])
        assert cols['datetime_6'].nullable is True
        assert cols['datetime_6'].type.fsp == 6, cols['datetime_6'].type.fsp

        assert dtype(cols['timestamp']) == 'timestamp', dtype(cols['timestamp'])
        assert cols['timestamp'].nullable is True
        assert cols['timestamp'].type.fsp is None, cols['timestamp'].type.fsp

        assert dtype(cols['timestamp_6']) == 'timestamp', dtype(cols['timestamp_6'])
        assert cols['timestamp_6'].nullable is True
        assert cols['timestamp_6'].type.fsp == 6, cols['timestamp_6'].type.fsp

        assert dtype(cols['year']) == 'year', dtype(cols['year'])
        assert cols['year'].nullable is True

        assert dtype(cols['char_100']) == 'char', dtype(cols['char_100'])
        assert cols['char_100'].nullable is True
        assert cols['char_100'].type.length == 100, cols['char_100'].type.length

        assert dtype(cols['binary_100']) == 'binary', dtype(cols['binary_100'])
        assert cols['binary_100'].nullable is True
        assert cols['binary_100'].type.length == 100, cols['binary_100'].type.length

        assert dtype(cols['varchar_200']) == 'varchar', dtype(cols['varchar_200'])
        assert cols['varchar_200'].nullable is True
        assert cols['varchar_200'].type.length == 200, cols['varchar_200'].type.length

        assert dtype(cols['varbinary_200']) == 'varbinary', dtype(cols['varbinary_200'])
        assert cols['varbinary_200'].nullable is True
        assert cols['varbinary_200'].type.length == 200, cols['varbinary_200'].type.length

        assert dtype(cols['longtext']) == 'longtext', dtype(cols['longtext'])
        assert cols['longtext'].nullable is True

        assert dtype(cols['mediumtext']) == 'mediumtext', dtype(cols['mediumtext'])
        assert cols['mediumtext'].nullable is True

        assert dtype(cols['text']) == 'text', dtype(cols['text'])
        assert cols['text'].nullable is True

        assert dtype(cols['tinytext']) == 'tinytext', dtype(cols['tinytext'])
        assert cols['tinytext'].nullable is True

        assert dtype(cols['longblob']) == 'longblob', dtype(cols['longblob'])
        assert cols['longblob'].nullable is True

        assert dtype(cols['mediumblob']) == 'mediumblob', dtype(cols['mediumblob'])
        assert cols['mediumblob'].nullable is True

        assert dtype(cols['blob']) == 'blob', dtype(cols['blob'])
        assert cols['blob'].nullable is True

        assert dtype(cols['tinyblob']) == 'tinyblob', dtype(cols['tinyblob'])
        assert cols['tinyblob'].nullable is True

        assert dtype(cols['json']) == 'json', dtype(cols['json'])
        assert cols['json'].nullable is True

        assert dtype(cols['enum']) == 'enum', dtype(cols['enum'])
        assert cols['enum'].nullable is True
        assert cols['enum'].type.enums == ['one', 'two', 'three'], cols['enum'].type.enums

        assert dtype(cols['set']) == 'set', dtype(cols['set'])
        assert cols['set'].nullable is True
        assert cols['set'].type.values == ('one', 'two', 'three'), cols['set'].type.values

    def test_double_percents(self, test_engine: Engine) -> None:
        """Test handling of double percent signs in SQL queries."""
        with test_engine.connect() as conn:
            # Direct to driver, no params
            out = list(conn.exec_driver_sql('select 21 % 2, 101 % 2'))
            assert out == [(1, 1)]

            # Direct to driver, positional params
            out = list(conn.exec_driver_sql('select 21 %% 2, %s %% 2', (101,)))
            assert out == [(1, 1)]

            # Direct to driver, dict params
            out = list(
                conn.exec_driver_sql(
                    'select 21 %% 2, %(num)s %% 2', dict(num=101),
                ),
            )
            assert out == [(1, 1)]

            with pytest.raises(ValueError):
                conn.exec_driver_sql('select 21 % 2, %(num)s % 2', dict(foo=101))

            # Direct to driver, no params (with dummy param)
            out = list(conn.exec_driver_sql('select 21 %% 2, 101 %% 2', dict(foo=100)))
            assert out == [(1, 1)]

            with pytest.raises(ValueError):
                conn.exec_driver_sql('select 21 % 2, 101 % 2', dict(foo=100))

            # Text clause, no params
            out = list(conn.execute(sa.text('select 21 % 2, 101 % 2')))
            assert out == [(1, 1)]

            # Text clause, dict params
            out = list(conn.execute(sa.text('select 21 % 2, :num % 2'), dict(num=101)))
            assert out == [(1, 1)]

            # Text clause, dict params (with dummy param)
            out = list(conn.execute(sa.text('select 21 % 2, 101 % 2'), dict(foo=100)))
            assert out == [(1, 1)]
