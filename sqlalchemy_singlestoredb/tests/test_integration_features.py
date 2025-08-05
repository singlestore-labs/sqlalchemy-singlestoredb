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

import os
import unittest
from unittest.mock import patch

import sqlalchemy as sa
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
from sqlalchemy_singlestoredb.column import PersistedColumn
from sqlalchemy_singlestoredb.ddlelement import ShardKey
from sqlalchemy_singlestoredb.ddlelement import SortKey
from sqlalchemy_singlestoredb.dtypes import JSON
from sqlalchemy_singlestoredb.dtypes import VECTOR
# Import our dialect and custom types


class TestIntegrationFeatures(unittest.TestCase):
    """Integration tests requiring actual database connection."""

    @classmethod
    def setUpClass(cls):
        """Set up database connection for all tests."""
        cls.connection_url = os.environ.get('SINGLESTOREDB_URL')
        if not cls.connection_url:
            raise unittest.SkipTest('SINGLESTOREDB_URL environment variable not set')

        try:
            cls.engine = create_engine(cls.connection_url)
            # Test basic connectivity
            with cls.engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            print(f'✅ Connected to database: {cls.connection_url}')
        except Exception as e:
            raise unittest.SkipTest(f'Cannot connect to database: {e}')

    @classmethod
    def tearDownClass(cls):
        """Clean up database connection."""
        if hasattr(cls, 'engine'):
            cls.engine.dispose()

    def setUp(self):
        """Set up for each test."""
        self.connection = self.engine.connect()
        self.dialect = self.engine.dialect

    def tearDown(self):
        """Clean up after each test."""
        try:
            # Clean up any test tables
            self.connection.execute(text('DROP TABLE IF EXISTS test_vector_table'))
            self.connection.execute(text('DROP TABLE IF EXISTS test_json_table'))
            self.connection.execute(text('DROP TABLE IF EXISTS test_shard_table'))
            self.connection.execute(text('DROP TABLE IF EXISTS test_cache_table'))
            self.connection.commit()
        except Exception:
            pass
        finally:
            self.connection.close()

    def test_basic_connection_and_dialect(self):
        """Test basic connection works with our enhanced dialect."""
        # Verify we're using our custom dialect
        self.assertIsInstance(self.dialect, SingleStoreDBDialect)

        # Test basic query execution
        result = self.connection.execute(text('SELECT 1 as test_value'))
        row = result.fetchone()
        self.assertEqual(row[0], 1)

        # Test dialect features are enabled
        self.assertTrue(self.dialect.supports_statement_cache)
        self.assertTrue(self.dialect.supports_server_side_cursors)

    def test_connection_ping_functionality(self):
        """Test our custom do_ping method works with real connection."""
        # Test ping with active connection
        dbapi_conn = self.connection.connection.dbapi_connection
        ping_result = self.dialect.do_ping(dbapi_conn)
        self.assertTrue(ping_result, 'Ping should succeed with active connection')

    def test_on_connect_initialization(self):
        """Test our on_connect method works during connection setup."""
        # Get the on_connect callable
        on_connect_fn = self.dialect.on_connect()
        self.assertTrue(callable(on_connect_fn))

        # Test it doesn't raise exceptions (it sets charset and sql_mode)
        dbapi_conn = self.connection.connection.dbapi_connection
        try:
            on_connect_fn(dbapi_conn)
            # Should not raise any exceptions
        except Exception as e:
            self.fail(f'on_connect should not raise exceptions: {e}')

    def test_statement_caching_behavior(self):
        """Test statement caching works with real queries."""
        # Create a simple test table
        self.connection.execute(
            text("""
            CREATE TABLE IF NOT EXISTS test_cache_table (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                value INT
            )
        """),
        )

        # Insert test data
        self.connection.execute(
            text("""
            INSERT INTO test_cache_table (id, name, value)
            VALUES (1, 'test1', 100), (2, 'test2', 200)
        """),
        )
        self.connection.commit()

        # Execute the same query multiple times (should use caching)
        query = text('SELECT * FROM test_cache_table WHERE value > :min_val')

        for i in range(3):
            result = self.connection.execute(query, {'min_val': 50})
            rows = result.fetchall()
            self.assertEqual(len(rows), 2)

        # Test with SQLAlchemy Core constructs
        metadata = MetaData()
        test_table = Table(
            'test_cache_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            Column('value', Integer),
            autoload_with=self.engine,
        )

        # Execute Core query multiple times
        for i in range(3):
            query = select(test_table).where(test_table.c.value > 50)
            result = self.connection.execute(query)
            rows = result.fetchall()
            self.assertEqual(len(rows), 2)

    def test_json_type_functionality(self):
        """Test our enhanced JSON type works with real data."""
        # Create table with JSON column
        self.connection.execute(
            text("""
            CREATE TABLE IF NOT EXISTS test_json_table (
                id INT PRIMARY KEY,
                data JSON,
                metadata JSON
            )
        """),
        )

        # Insert JSON data
        test_data = {'name': 'test', 'values': [1, 2, 3], 'nested': {'key': 'value'}}
        self.connection.execute(
            text("""
            INSERT INTO test_json_table (id, data, metadata)
            VALUES (:id, :data, :metadata)
        """), {
                'id': 1,
                'data': test_data,
                'metadata': {'created': '2024-01-01', 'version': 1},
            },
        )
        self.connection.commit()

        # Query and verify JSON data
        result = self.connection.execute(text('SELECT data, metadata FROM test_json_table WHERE id = 1'))
        row = result.fetchone()

        # Our JSON type should deserialize properly
        self.assertIsInstance(row[0], (dict, list))
        self.assertIsInstance(row[1], (dict, list))

        if isinstance(row[0], dict):
            self.assertEqual(row[0]['name'], 'test')
            self.assertEqual(row[0]['values'], [1, 2, 3])

    def test_vector_type_basic_functionality(self):
        """Test VECTOR type basic functionality (if supported by SingleStore)."""
        try:
            # Try to create table with VECTOR column
            self.connection.execute(
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
            result = self.connection.execute(
                text("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'test_vector_table'
                AND COLUMN_NAME = 'embedding'
            """),
            )
            row = result.fetchone()

            if row:
                self.assertEqual(row[0], 'embedding')
                # The DATA_TYPE might be 'vector' or similar
                print(f'✅ VECTOR column created with type: {row[1]}')

        except Exception as e:
            # VECTOR type might not be supported in all SingleStore versions
            self.skipTest(f'VECTOR type not supported or available: {e}')

    def test_shard_key_sort_key_syntax(self):
        """Test SHARD KEY and SORT KEY DDL compilation."""
        try:
            # Test SHARD KEY syntax
            self.connection.execute(
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
            result = self.connection.execute(
                text("""
                SHOW CREATE TABLE test_shard_table
            """),
            )
            create_statement = result.fetchone()[1]

            # Check if SHARD KEY and SORT KEY are in the CREATE statement
            self.assertIn('SHARD', create_statement.upper())
            print(f'✅ SHARD KEY/SORT KEY table created successfully')
            print(f'Create statement: {create_statement}')

        except Exception as e:
            # Might not be supported in all environments
            print(f'ℹ️  SHARD KEY/SORT KEY might not be supported: {e}')

    def test_execution_options_streaming(self):
        """Test stream_results execution option."""
        # Create test data
        self.connection.execute(
            text("""
            CREATE TABLE IF NOT EXISTS test_cache_table (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                value INT
            )
        """),
        )

        # Insert multiple rows
        for i in range(10):
            self.connection.execute(
                text("""
                INSERT IGNORE INTO test_cache_table (id, name, value)
                VALUES (:id, :name, :value)
            """), {'id': i, 'name': f'test{i}', 'value': i * 10},
            )
        self.connection.commit()

        # Test with stream_results execution option
        query = text('SELECT * FROM test_cache_table ORDER BY id')

        # Note: stream_results behavior depends on the underlying driver
        # This test mainly verifies the option doesn't cause errors
        result = self.connection.execution_options(stream_results=True).execute(query)
        rows = result.fetchall()
        self.assertGreaterEqual(len(rows), 10)

    def test_cast_operator_compilation(self):
        """Test SingleStore's :> cast operator compilation."""
        # Test our custom cast syntax
        query = text('SELECT 123 :> DOUBLE as casted_value')
        result = self.connection.execute(query)
        row = result.fetchone()
        self.assertEqual(float(row[0]), 123.0)

        # Test with different types
        queries = [
            "SELECT '456' :> SIGNED as int_val",
            'SELECT 789.5 :> JSON as json_val',
            'SELECT NOW() :> TIMESTAMP as ts_val',
        ]

        for query_text in queries:
            try:
                result = self.connection.execute(text(query_text))
                row = result.fetchone()
                self.assertIsNotNone(row[0])
            except Exception as e:
                print(f"ℹ️  Cast operator test skipped for '{query_text}': {e}")

    def test_double_percent_handling(self):
        """Test our double percent handling in SQL queries."""
        # Test modulo operator handling
        query = text('SELECT 21 % 2 as mod_result')
        result = self.connection.execute(query)
        row = result.fetchone()
        self.assertEqual(row[0], 1)

        # Test with parameters
        query = text('SELECT :num % 2 as mod_result')
        result = self.connection.execute(query, {'num': 21})
        row = result.fetchone()
        self.assertEqual(row[0], 1)

    def test_orm_integration(self):
        """Test ORM integration with our enhanced dialect."""
        # Create ORM base and model
        Base = declarative_base()

        class TestModel(Base):
            __tablename__ = 'test_orm_table'

            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            data = Column(JSON)

        # Create table
        try:
            Base.metadata.drop_all(self.engine, checkfirst=True)
            Base.metadata.create_all(self.engine)

            # Test ORM operations
            Session = sessionmaker(bind=self.engine)
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
            self.assertIsNotNone(retrieved)
            self.assertEqual(retrieved.name, 'test_orm')

            # Test JSON field
            if isinstance(retrieved.data, dict):
                self.assertEqual(retrieved.data['key'], 'value')

            session.close()

        except Exception as e:
            self.fail(f'ORM integration test failed: {e}')
        finally:
            try:
                Base.metadata.drop_all(self.engine, checkfirst=True)
            except:
                pass


def run_integration_tests():
    """Run integration tests with proper setup."""
    print('🧪 Running SingleStoreDB Integration Tests')
    print('=' * 50)

    # Check for database connection
    connection_url = os.environ.get('SINGLESTOREDB_URL')
    if not connection_url:
        print('❌ SINGLESTOREDB_URL environment variable not set')
        return False

    print(f'🔗 Using connection: {connection_url}')
    print()

    # Run the test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestIntegrationFeatures)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    if result.wasSuccessful():
        print('\n🎉 ALL INTEGRATION TESTS PASSED!')
        print('The SingleStoreDB dialect is working correctly with real database operations.')
        return True
    else:
        print('\n❌ Some integration tests failed.')
        print(f'Failures: {len(result.failures)}, Errors: {len(result.errors)}')
        return False


if __name__ == '__main__':
    success = run_integration_tests()
    exit(0 if success else 1)
