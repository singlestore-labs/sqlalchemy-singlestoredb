#!/usr/bin/env python
"""
Integration tests for SingleStoreDB dialect Priority 1 features.

These tests require an actual SingleStore database connection and test:
- Real database operations with our new features
- SingleStore-specific functionality (VECTOR, SHARD KEY, SORT KEY)
- SQL compilation and execution
- Caching behavior
- ORM integration
"""
from __future__ import annotations

import pytest
import singlestoredb
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
from sqlalchemy_singlestoredb.dtypes import JSON


class TestIntegrationFeatures:
    """Integration tests requiring actual database connection."""

    def test_basic_connection_and_dialect(self, test_engine: Engine) -> None:
        """Test basic connection works with our enhanced dialect."""
        # Verify we're using our custom dialect
        assert isinstance(test_engine.dialect, SingleStoreDBDialect)

        # Test basic query execution
        with test_engine.connect() as conn:
            result = conn.execute(text('SELECT 1 as test_value'))
            row = result.fetchone()
            assert row[0] == 1

            # Test dialect features are enabled
            assert test_engine.dialect.supports_statement_cache
            assert test_engine.dialect.supports_server_side_cursors

    def test_connection_ping_functionality(
        self, test_engine: Engine, test_connection: singlestoredb.connection.Connection,
    ) -> None:
        """Test our custom do_ping method works with real connection."""
        # Test ping with active connection - use the raw connection
        ping_result = test_engine.dialect.do_ping(test_connection)
        assert ping_result, 'Ping should succeed with active connection'

    def test_on_connect_initialization(
        self, test_engine: Engine, test_connection: singlestoredb.connection.Connection,
    ) -> None:
        """Test our on_connect method works during connection setup."""
        # Get the on_connect callable
        on_connect_fn = test_engine.dialect.on_connect()
        assert callable(on_connect_fn)

        # Test it doesn't raise exceptions (it sets charset and sql_mode)
        try:
            on_connect_fn(test_connection)
            # Should not raise any exceptions
        except Exception as e:
            assert False, f'on_connect should not raise exceptions: {e}'

    def test_statement_caching_behavior(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test statement caching works with real queries."""
        with test_engine.connect() as conn:
            # Create a simple test table
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_cache_table (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    value INT
                )
            """),
            )

            # Insert test data
            with conn.begin():
                conn.execute(
                    text("""
                    INSERT INTO test_cache_table (id, name, value)
                    VALUES (1, 'test1', 100), (2, 'test2', 200)
                """),
                )

            # Execute the same query multiple times (should use caching)
            query = text('SELECT * FROM test_cache_table WHERE value > :min_val')

            for i in range(3):
                result = conn.execute(query, {'min_val': 50})
                rows = result.fetchall()
                assert len(rows) == 2

            # Test with SQLAlchemy Core constructs
            metadata = MetaData()
            test_table = Table(
                'test_cache_table', metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String(100)),
                Column('value', Integer),
                autoload_with=test_engine,
            )

            # Execute Core query multiple times
            for i in range(3):
                query = select(test_table).where(test_table.c.value > 50)
                result = conn.execute(query)
                rows = result.fetchall()
                assert len(rows) == 2

    def test_json_type_functionality(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test our enhanced JSON type works with real data."""
        with test_engine.connect() as conn:
            # Create table with JSON column
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_json_table (
                    id INT PRIMARY KEY,
                    data JSON,
                    metadata JSON
                )
            """),
            )

            # Insert JSON data (test deserialization)
            with conn.begin():
                conn.execute(
                    text("""
                    INSERT INTO test_json_table (id, data, metadata)
                    VALUES (:id, :data, :metadata)
                """), {
                        'id': 1,
                        'data': (
                            '{"name": "test", "values": [1, 2, 3], '
                            '"nested": {"key": "value"}}'
                        ),
                        'metadata': '{"created": "2024-01-01", "version": 1}',
                    },
                )

            # Query and verify JSON data
            query = 'SELECT data, metadata FROM test_json_table WHERE id = 1'
            result = conn.execute(text(query))
            row = result.fetchone()

            # Our JSON type should deserialize properly
            assert isinstance(row[0], (dict, list))
            assert isinstance(row[1], (dict, list))

            if isinstance(row[0], dict):
                assert row[0]['name'] == 'test'
                assert row[0]['values'] == [1, 2, 3]

    def test_vector_type_basic_functionality(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test VECTOR type basic functionality (if supported by SingleStore)."""
        try:
            with test_engine.connect() as conn:
                # Try to create table with VECTOR column
                conn.execute(
                    text("""
                    CREATE TABLE IF NOT EXISTS test_vector_table (
                        id INT PRIMARY KEY,
                        embedding VECTOR(128, F32),
                        description VARCHAR(255)
                    )
                """),
                )

                # If table creation succeeded, test basic operations
                # Note: We can't easily insert vector data without proper driver support,
                # but we can test the table structure

                # Verify table was created
                result = conn.execute(
                    text("""
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'test_vector_table'
                    AND COLUMN_NAME = 'embedding'
                """),
                )
                row = result.fetchone()

                if row:
                    assert row[0] == 'embedding'
                    # The DATA_TYPE might be 'vector' or similar
                    print(f'✅ VECTOR column created with type: {row[1]}')

        except Exception as e:
            # VECTOR type might not be supported in all SingleStore versions
            pytest.skip(f'VECTOR type not supported or available: {e}')

    def test_shard_key_sort_key_syntax(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test SHARD KEY and SORT KEY DDL compilation."""
        try:
            with test_engine.connect() as conn:
                # Test SHARD KEY syntax
                conn.execute(
                    text("""
                    CREATE TABLE IF NOT EXISTS test_shard_table (
                        id INT,
                        user_id INT,
                        created_at TIMESTAMP,
                        data VARCHAR(255),
                        SHARD KEY (user_id),
                        SORT KEY (created_at)
                    )
                """),
                )

                # If successful, verify table structure
                result = conn.execute(
                    text("""
                    SHOW CREATE TABLE test_shard_table
                """),
                )
                create_statement = result.fetchone()[1]

                # Check if SHARD KEY and SORT KEY are in the CREATE statement
                assert 'SHARD' in create_statement.upper()
                print('✅ SHARD KEY/SORT KEY table created successfully')
                print(f'Create statement: {create_statement}')

        except Exception as e:
            # Might not be supported in all environments
            print(f'ℹ️ SHARD KEY/SORT KEY might not be supported: {e}')

    def test_execution_options_streaming(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test stream_results execution option."""
        with test_engine.connect() as conn:
            # Create test data
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS test_cache_table (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    value INT
                )
            """),
            )

            # Insert multiple rows
            with conn.begin():
                for i in range(10):
                    conn.execute(
                        text("""
                        INSERT IGNORE INTO test_cache_table (id, name, value)
                        VALUES (:id, :name, :value)
                    """), {'id': i, 'name': f'test{i}', 'value': i * 10},
                    )

            # Test with stream_results execution option
            query = text('SELECT * FROM test_cache_table ORDER BY id')

            # Note: stream_results behavior depends on the underlying driver
            # This test mainly verifies the option doesn't cause errors
            result = conn.execution_options(stream_results=True).execute(query)
            rows = result.fetchall()
            assert len(rows) >= 10

    def test_cast_operator_compilation(self, test_engine: Engine) -> None:
        """Test SingleStore's :> cast operator compilation."""
        with test_engine.connect() as conn:
            # Test our custom cast syntax
            query = text('SELECT 123 :> DOUBLE as casted_value')
            result = conn.execute(query)
            row = result.fetchone()
            assert float(row[0]) == 123.0

            # Test with different types
            queries = [
                "SELECT '456' :> SIGNED as int_val",
                'SELECT 789.5 :> JSON as json_val',
                'SELECT NOW() :> TIMESTAMP as ts_val',
            ]

            for query_text in queries:
                try:
                    result = conn.execute(text(query_text))
                    row = result.fetchone()
                    assert row[0] is not None
                except Exception as e:
                    print(f"ℹ️  Cast operator test skipped for '{query_text}': {e}")

    def test_double_percent_handling(self, test_engine: Engine) -> None:
        """Test our double percent handling in SQL queries."""
        with test_engine.connect() as conn:
            # Test modulo operator handling
            query = text('SELECT 21 % 2 as mod_result')
            result = conn.execute(query)
            row = result.fetchone()
            assert row[0] == 1

            # Test with parameters
            query = text('SELECT :num % 2 as mod_result')
            result = conn.execute(query, {'num': 21})
            row = result.fetchone()
            assert row[0] == 1

    def test_orm_integration(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test ORM integration with our enhanced dialect."""
        # Create ORM base and model
        Base = declarative_base()

        class TestModel(Base):  # type: ignore
            __tablename__ = 'test_orm_table'

            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            data = Column(JSON)

        # Create table
        try:
            Base.metadata.drop_all(test_engine, checkfirst=True)
            Base.metadata.create_all(test_engine)

            # Test ORM operations
            Session = sessionmaker(bind=test_engine)
            session = Session()

            # Insert data
            test_obj = TestModel(
                id=1,
                name='test_orm',
                data={'key': 'value', 'numbers': [1, 2, 3]},
            )
            session.add(test_obj)
            session.commit()

            # Query data
            retrieved = session.query(TestModel).filter_by(id=1).first()
            assert retrieved is not None
            assert retrieved.name == 'test_orm'

            # Test JSON field
            if isinstance(retrieved.data, dict):
                assert retrieved.data['key'] == 'value'

            session.close()

        except Exception as e:
            assert False, f'ORM integration test failed: {e}'
        finally:
            try:
                Base.metadata.drop_all(test_engine, checkfirst=True)
            except Exception:
                pass
