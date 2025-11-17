"""Tests for SingleStore table options."""
from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.engine.mock import create_mock_engine


class TestTableOptions:
    """Test SingleStore-specific table options."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.ddl_statements: list[str] = []

        def dump_ddl(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.ddl_statements.append(str(sql.compile(dialect=self.mock_engine.dialect)))

        self.mock_engine = create_mock_engine('singlestoredb://', dump_ddl)
        self.metadata = MetaData()

    def test_autostats_enabled_option(self) -> None:
        """Test AUTOSTATS_ENABLED table option."""
        table = Table(
            'test_autostats', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            singlestoredb_autostats_enabled='TRUE',
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AUTOSTATS_ENABLED = TRUE' in self.ddl_statements[-1]

    def test_autostats_cardinality_mode_option(self) -> None:
        """Test AUTOSTATS_CARDINALITY_MODE table option."""
        table = Table(
            'test_cardinality', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_autostats_cardinality_mode='INCREMENTAL',
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AUTOSTATS_CARDINALITY_MODE = INCREMENTAL' in self.ddl_statements[-1]

    def test_autostats_histogram_mode_option(self) -> None:
        """Test AUTOSTATS_HISTOGRAM_MODE table option."""
        table = Table(
            'test_histogram', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_autostats_histogram_mode='CREATE',
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AUTOSTATS_HISTOGRAM_MODE = CREATE' in self.ddl_statements[-1]

    def test_autostats_sampling_option(self) -> None:
        """Test AUTOSTATS_SAMPLING table option."""
        table = Table(
            'test_sampling', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_autostats_sampling='ON',
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AUTOSTATS_SAMPLING = ON' in self.ddl_statements[-1]

    def test_compression_option(self) -> None:
        """Test COMPRESSION table option."""
        table = Table(
            'test_compression', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_compression='SPARSE',
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'COMPRESSION = SPARSE' in self.ddl_statements[-1]

    def test_multiple_table_options(self) -> None:
        """Test multiple SingleStore table options together."""
        table = Table(
            'test_multiple', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_autostats_enabled='FALSE',
            singlestoredb_compression='SPARSE',
            singlestoredb_autostats_sampling='OFF',
        )
        table.create(self.mock_engine, checkfirst=False)

        ddl = self.ddl_statements[-1]
        assert 'AUTOSTATS_ENABLED = FALSE' in ddl
        assert 'COMPRESSION = SPARSE' in ddl
        assert 'AUTOSTATS_SAMPLING = OFF' in ddl

    def test_case_insensitive_values(self) -> None:
        """Test that table option values are case insensitive."""
        table = Table(
            'test_case', self.metadata,
            Column('id', Integer, primary_key=True),
            singlestoredb_autostats_enabled='true',  # lowercase
            singlestoredb_autostats_sampling='on',    # lowercase
        )
        table.create(self.mock_engine, checkfirst=False)

        ddl = self.ddl_statements[-1]
        assert 'AUTOSTATS_ENABLED = TRUE' in ddl
        assert 'AUTOSTATS_SAMPLING = ON' in ddl

    def test_all_autostats_cardinality_modes(self) -> None:
        """Test all valid AUTOSTATS_CARDINALITY_MODE values."""
        modes = ['INCREMENTAL', 'PERIODIC', 'OFF']

        for mode in modes:
            table = Table(
                f'test_cardinality_{mode.lower()}', self.metadata,
                Column('id', Integer, primary_key=True),
                singlestoredb_autostats_cardinality_mode=mode,
            )
            table.create(self.mock_engine, checkfirst=False)

            assert f'AUTOSTATS_CARDINALITY_MODE = {mode}' in self.ddl_statements[-1]

    def test_all_autostats_histogram_modes(self) -> None:
        """Test all valid AUTOSTATS_HISTOGRAM_MODE values."""
        modes = ['CREATE', 'UPDATE', 'OFF']

        for mode in modes:
            table = Table(
                f'test_histogram_{mode.lower()}', self.metadata,
                Column('id', Integer, primary_key=True),
                singlestoredb_autostats_histogram_mode=mode,
            )
            table.create(self.mock_engine, checkfirst=False)

            assert f'AUTOSTATS_HISTOGRAM_MODE = {mode}' in self.ddl_statements[-1]

    def test_invalid_autostats_enabled_value(self) -> None:
        """Test invalid AUTOSTATS_ENABLED value raises ValueError."""
        match_pattern = 'Invalid value "INVALID" for AUTOSTATS_ENABLED'
        with pytest.raises(ValueError, match=match_pattern):
            table = Table(
                'test_invalid', self.metadata,
                Column('id', Integer, primary_key=True),
                singlestoredb_autostats_enabled='INVALID',
            )
            table.create(self.mock_engine, checkfirst=False)

    def test_invalid_cardinality_mode_value(self) -> None:
        """Test invalid AUTOSTATS_CARDINALITY_MODE value raises ValueError."""
        match_pattern = 'Invalid value "INVALID" for AUTOSTATS_CARDINALITY_MODE'
        with pytest.raises(ValueError, match=match_pattern):
            table = Table(
                'test_invalid', self.metadata,
                Column('id', Integer, primary_key=True),
                singlestoredb_autostats_cardinality_mode='INVALID',
            )
            table.create(self.mock_engine, checkfirst=False)

    def test_invalid_histogram_mode_value(self) -> None:
        """Test invalid AUTOSTATS_HISTOGRAM_MODE value raises ValueError."""
        match_pattern = 'Invalid value "INVALID" for AUTOSTATS_HISTOGRAM_MODE'
        with pytest.raises(ValueError, match=match_pattern):
            table = Table(
                'test_invalid', self.metadata,
                Column('id', Integer, primary_key=True),
                singlestoredb_autostats_histogram_mode='INVALID',
            )
            table.create(self.mock_engine, checkfirst=False)

    def test_invalid_sampling_value(self) -> None:
        """Test invalid AUTOSTATS_SAMPLING value raises ValueError."""
        match_pattern = 'Invalid value "INVALID" for AUTOSTATS_SAMPLING'
        with pytest.raises(ValueError, match=match_pattern):
            table = Table(
                'test_invalid', self.metadata,
                Column('id', Integer, primary_key=True),
                singlestoredb_autostats_sampling='INVALID',
            )
            table.create(self.mock_engine, checkfirst=False)

    def test_invalid_compression_value(self) -> None:
        """Test invalid COMPRESSION value raises ValueError."""
        match_pattern = 'Invalid value "INVALID" for COMPRESSION'
        with pytest.raises(ValueError, match=match_pattern):
            table = Table(
                'test_invalid', self.metadata,
                Column('id', Integer, primary_key=True),
                singlestoredb_compression='INVALID',
            )
            table.create(self.mock_engine, checkfirst=False)

    def test_table_options_with_shard_key(self) -> None:
        """Test table options work with other SingleStore features."""
        from sqlalchemy_singlestoredb import ShardKey

        table = Table(
            'test_with_shard', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_shard_key=ShardKey('id'),
            singlestoredb_autostats_enabled='FALSE',
            singlestoredb_compression='SPARSE',
        )
        table.create(self.mock_engine, checkfirst=False)

        ddl = self.ddl_statements[-1]
        assert 'SHARD KEY (id)' in ddl
        assert 'AUTOSTATS_ENABLED = FALSE' in ddl
        assert 'COMPRESSION = SPARSE' in ddl

    def test_autostats_enabled_boolean_true(self) -> None:
        """Test AUTOSTATS_ENABLED with Python boolean True."""
        table = Table(
            'test_bool_true', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            singlestoredb_autostats_enabled=True,  # Boolean True
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AUTOSTATS_ENABLED = TRUE' in self.ddl_statements[-1]

    def test_autostats_enabled_boolean_false(self) -> None:
        """Test AUTOSTATS_ENABLED with Python boolean False."""
        table = Table(
            'test_bool_false', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            singlestoredb_autostats_enabled=False,  # Boolean False
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AUTOSTATS_ENABLED = FALSE' in self.ddl_statements[-1]

    def test_autostats_sampling_boolean_true(self) -> None:
        """Test AUTOSTATS_SAMPLING with Python boolean True."""
        table = Table(
            'test_sampling_bool_true', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            singlestoredb_autostats_sampling=True,  # Boolean True
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AUTOSTATS_SAMPLING = ON' in self.ddl_statements[-1]

    def test_autostats_sampling_boolean_false(self) -> None:
        """Test AUTOSTATS_SAMPLING with Python boolean False."""
        table = Table(
            'test_sampling_bool_false', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            singlestoredb_autostats_sampling=False,  # Boolean False
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AUTOSTATS_SAMPLING = OFF' in self.ddl_statements[-1]

    def test_boolean_and_string_values_mixed(self) -> None:
        """Test mixing boolean and string values for table options."""
        table = Table(
            'test_mixed_types', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_autostats_enabled=True,      # Boolean
            singlestoredb_autostats_sampling='OFF',    # String
            singlestoredb_compression='SPARSE',        # String
        )
        table.create(self.mock_engine, checkfirst=False)

        ddl = self.ddl_statements[-1]
        assert 'AUTOSTATS_ENABLED = TRUE' in ddl   # Boolean True -> TRUE
        assert 'AUTOSTATS_SAMPLING = OFF' in ddl   # String value
        assert 'COMPRESSION = SPARSE' in ddl       # String value

    def test_all_boolean_combinations(self) -> None:
        """Test all combinations of boolean values for boolean options."""
        test_cases = [
            (True, True, 'TRUE', 'ON'),
            (True, False, 'TRUE', 'OFF'),
            (False, True, 'FALSE', 'ON'),
            (False, False, 'FALSE', 'OFF'),
        ]

        for enabled, sampling, expected_enabled, expected_sampling in test_cases:
            table_name = f'test_combo_{str(enabled).lower()}_{str(sampling).lower()}'
            table = Table(
                table_name, self.metadata,
                Column('id', Integer, primary_key=True),
                singlestoredb_autostats_enabled=enabled,
                singlestoredb_autostats_sampling=sampling,
            )
            table.create(self.mock_engine, checkfirst=False)

            ddl = self.ddl_statements[-1]
            assert f'AUTOSTATS_ENABLED = {expected_enabled}' in ddl
            assert f'AUTOSTATS_SAMPLING = {expected_sampling}' in ddl

    def test_autostats_cardinality_mode_boolean_false(self) -> None:
        """Test AUTOSTATS_CARDINALITY_MODE with Python boolean False."""
        table = Table(
            'test_cardinality_bool', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            singlestoredb_autostats_cardinality_mode=False,  # Boolean False -> OFF
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AUTOSTATS_CARDINALITY_MODE = OFF' in self.ddl_statements[-1]

    def test_autostats_histogram_mode_boolean_false(self) -> None:
        """Test AUTOSTATS_HISTOGRAM_MODE with Python boolean False."""
        table = Table(
            'test_histogram_bool', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            singlestoredb_autostats_histogram_mode=False,  # Boolean False -> OFF
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AUTOSTATS_HISTOGRAM_MODE = OFF' in self.ddl_statements[-1]

    def test_boolean_true_for_off_only_options(self) -> None:
        """Test that boolean True for OFF-only options gets converted to string."""
        # These options only support False->OFF mapping, True should be converted to
        # string and should raise ValueError since 'True' is not a valid value
        pattern = 'Invalid value "True" for AUTOSTATS_CARDINALITY_MODE'
        with pytest.raises(ValueError, match=pattern):
            table = Table(
                'test_invalid_true', self.metadata,
                Column('id', Integer, primary_key=True),
                singlestoredb_autostats_cardinality_mode=True,
            )
            table.create(self.mock_engine, checkfirst=False)

        pattern2 = 'Invalid value "True" for AUTOSTATS_HISTOGRAM_MODE'
        with pytest.raises(ValueError, match=pattern2):
            table = Table(
                'test_invalid_true2', self.metadata,
                Column('id', Integer, primary_key=True),
                singlestoredb_autostats_histogram_mode=True,
            )
            table.create(self.mock_engine, checkfirst=False)

    def test_all_off_accepting_options_with_false(self) -> None:
        """Test that all options accepting OFF work with Python False."""
        # Test all options that accept OFF with False
        table = Table(
            'test_all_off', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_autostats_cardinality_mode=False,  # -> OFF
            singlestoredb_autostats_histogram_mode=False,    # -> OFF
            singlestoredb_autostats_sampling=False,          # -> OFF
        )
        table.create(self.mock_engine, checkfirst=False)

        ddl = self.ddl_statements[-1]
        assert 'AUTOSTATS_CARDINALITY_MODE = OFF' in ddl
        assert 'AUTOSTATS_HISTOGRAM_MODE = OFF' in ddl
        assert 'AUTOSTATS_SAMPLING = OFF' in ddl

    def test_mixed_boolean_and_string_for_off_options(self) -> None:
        """Test mixing boolean False and string values for OFF-accepting options."""
        table = Table(
            'test_mixed_off', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_autostats_cardinality_mode=False,        # Boolean -> OFF
            singlestoredb_autostats_histogram_mode='CREATE',       # String
            singlestoredb_autostats_sampling='ON',                 # String
        )
        table.create(self.mock_engine, checkfirst=False)

        ddl = self.ddl_statements[-1]
        assert 'AUTOSTATS_CARDINALITY_MODE = OFF' in ddl   # Boolean False
        assert 'AUTOSTATS_HISTOGRAM_MODE = CREATE' in ddl  # String value
        assert 'AUTOSTATS_SAMPLING = ON' in ddl            # String value


class TestTableOptionsReflection:
    """Test table options reflection from actual database tables."""

    def test_reflect_table_with_autostats_options(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of table with AUTOSTATS options."""
        table_name = 'test_autostats_reflection'

        with test_engine.connect() as conn:
            try:
                with conn.begin():
                    # Create table with AUTOSTATS options
                    # (cannot mix AUTOSTATS_ENABLED with others)
                    conn.execute(
                        text(f"""
                        CREATE TABLE {table_name} (
                            id INT PRIMARY KEY,
                            data VARCHAR(100),
                            metrics JSON,
                            created_at TIMESTAMP
                        ) AUTOSTATS_CARDINALITY_MODE = INCREMENTAL
                        AUTOSTATS_HISTOGRAM_MODE = CREATE
                        AUTOSTATS_SAMPLING = ON
                    """),
                    )
            except Exception as e:
                if 'autostats' in str(e).lower() or 'not supported' in str(e).lower():
                    pytest.skip(f'AUTOSTATS options not supported or incompatible: {e}')
                else:
                    raise

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print(f'\nGenerated CREATE TABLE for {table_name}:')
            print(create_sql)

            # Verify reflection works
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) == 4
            assert 'id' in reflected_table.columns
            assert 'data' in reflected_table.columns
            assert 'metrics' in reflected_table.columns
            assert 'created_at' in reflected_table.columns

    def test_reflect_table_with_compression_option(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of table with COMPRESSION option."""
        table_name = 'test_compression_reflection'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with COMPRESSION option
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        user_id INT,
                        log_data TEXT,
                        event_time TIMESTAMP,
                        metadata JSON,
                        PRIMARY KEY (user_id, event_time)
                    ) COMPRESSION = SPARSE
                """),
                )

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print(f'\nGenerated CREATE TABLE for {table_name}:')
            print(create_sql)

            # Verify reflection works
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) == 4
            assert set(col.name for col in reflected_table.columns) == {
                'user_id', 'log_data', 'event_time', 'metadata',
            }

            # Should have composite primary key
            pk_columns = {col.name for col in reflected_table.primary_key.columns}
            assert pk_columns == {'user_id', 'event_time'}

    def test_reflect_table_with_multiple_options(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of table with multiple table options."""
        table_name = 'test_multiple_options_reflection'

        with test_engine.connect() as conn:
            try:
                with conn.begin():
                    # Create table with multiple options
                    # (cannot mix AUTOSTATS_ENABLED with others)
                    conn.execute(
                        text(f"""
                        CREATE TABLE {table_name} (
                            doc_id INT PRIMARY KEY,
                            title VARCHAR(200),
                            content TEXT,
                            tags JSON,
                            score DECIMAL(5,2)
                        ) COMPRESSION = SPARSE,
                        AUTOSTATS_CARDINALITY_MODE = PERIODIC
                    """),
                    )
            except Exception as e:
                if (
                    'autostats' in str(e).lower() or
                    'compression' in str(e).lower() or
                    'not supported' in str(e).lower()
                ):
                    pytest.skip(f'Table options not supported or incompatible: {e}')
                else:
                    raise

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print(f'\nGenerated CREATE TABLE for {table_name}:')
            print(create_sql)

            # Verify reflection works
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) == 5
            assert set(col.name for col in reflected_table.columns) == {
                'doc_id', 'title', 'content', 'tags', 'score',
            }

    def test_reflect_table_with_options_and_keys(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of table with both table options and keys."""
        table_name = 'test_options_and_keys_reflection'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with options and keys combined
                # Cannot mix AUTOSTATS_ENABLED with other autostats options
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        user_id INT,
                        session_id VARCHAR(64),
                        activity_data JSON,
                        timestamp_col TIMESTAMP,
                        PRIMARY KEY (user_id, session_id),
                        SHARD KEY (user_id),
                        SORT KEY (timestamp_col),
                        KEY idx_timestamp (timestamp_col)
                    ) COMPRESSION = SPARSE
                    AUTOSTATS_CARDINALITY_MODE = PERIODIC
                """),
                )

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print(f'\nGenerated CREATE TABLE for {table_name}:')
            print(create_sql)

            # Verify reflection works
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) == 4
            assert set(col.name for col in reflected_table.primary_key.columns) == {
                'user_id', 'session_id',
            }

            # Should have regular indexes
            indexes = reflected_table.indexes
            index_names = {idx.name for idx in indexes}
            print(f'\nReflected indexes: {index_names}')
            assert 'idx_timestamp' in index_names

    def test_reflect_table_with_boolean_options(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of table with boolean-style options."""
        table_name = 'test_boolean_options_reflection'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with boolean options (ON/OFF vs TRUE/FALSE)
                # Cannot mix AUTOSTATS_ENABLED with other autostats options
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        record_id INT PRIMARY KEY,
                        payload TEXT,
                        created_date DATE
                    ) AUTOSTATS_ENABLED = TRUE
                """),
                )

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print(f'\nGenerated CREATE TABLE for {table_name}:')
            print(create_sql)

            # Verify reflection works
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) == 3
            assert 'record_id' in reflected_table.columns
            assert 'payload' in reflected_table.columns
            assert 'created_date' in reflected_table.columns
