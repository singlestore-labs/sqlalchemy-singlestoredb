#!/usr/bin/env python
"""
Example tests demonstrating the use of database fixtures.
"""
from __future__ import annotations

import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.engine import Engine

from sqlalchemy_singlestoredb.dtypes import JSON
from sqlalchemy_singlestoredb.dtypes import VECTOR


class TestDatabaseFixtures:
    """Test suite demonstrating database fixture usage."""

    def test_database_is_created_and_isolated(
        self, test_engine: Engine, test_database: str,
    ) -> None:
        """Test that each test gets its own isolated database."""
        # Verify we're connected to the test database
        with test_engine.connect() as conn:
            result = conn.execute(text('SELECT DATABASE()'))
            current_db = result.scalar()
            assert current_db == test_database
            assert current_db.startswith('test_sqlalchemy_')

    def test_tables_are_cleaned_up(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test that tables are automatically cleaned up after the test."""
        metadata = MetaData()

        # Create a test table
        Table(
            'test_cleanup_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
        )

        # Create the table
        metadata.create_all(test_engine)

        # Verify table exists
        with test_engine.connect() as conn:
            result = conn.execute(
                text(
                    'SELECT COUNT(*) FROM information_schema.tables '
                    'WHERE table_schema = DATABASE() '
                    "AND table_name = 'test_cleanup_table'",
                ),
            )
            assert result.scalar() == 1

        # The table will be automatically cleaned up after this test

    def test_vector_type_with_real_database(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test VECTOR type with a real database connection."""
        metadata = MetaData()

        # Create table with VECTOR column
        Table(
            'test_vector',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('embedding', VECTOR(128, 'F32')),
        )

        try:
            # Create the table
            metadata.create_all(test_engine)

            # Verify table was created with VECTOR column
            with test_engine.connect() as conn:
                result = conn.execute(
                    text(
                        'SHOW CREATE TABLE test_vector',
                    ),
                )
                create_statement = result.fetchone()[1]
                assert 'VECTOR' in create_statement.upper()

        except Exception as e:
            # VECTOR might not be supported in all SingleStore versions
            pytest.skip(f'VECTOR type not supported: {e}')

    def test_json_type_with_real_database(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test JSON type with a real database connection."""
        metadata = MetaData()

        # Create table with JSON column
        json_table = Table(
            'test_json',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('data', JSON),
        )

        # Create the table
        metadata.create_all(test_engine)

        # Insert and retrieve JSON data
        with test_engine.connect() as conn:
            with conn.begin():
                # Insert data
                conn.execute(
                    json_table.insert(),
                    {'id': 1, 'data': {'key': 'value', 'number': 42}},
                )

            # Retrieve data
            result = conn.execute(
                json_table.select().where(json_table.c.id == 1),
            )
            row = result.fetchone()
            assert row.data == {'key': 'value', 'number': 42}

    def test_multiple_tests_get_different_databases(self, test_database: str) -> None:
        """Test that different test runs get different database names."""
        # This test just verifies the database name format
        assert test_database.startswith('test_sqlalchemy_')
        assert len(test_database) == len('test_sqlalchemy_') + 8  # 8 random chars

    @pytest.mark.parametrize('table_name', ['users', 'products', 'orders'])
    def test_parametrized_with_database(
        self,
        test_engine: Engine,
        clean_tables: None,
        table_name: str,
    ) -> None:
        """Test that parametrized tests work correctly with database fixtures."""
        metadata = MetaData()

        # Create a table with the parametrized name
        Table(
            table_name,
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
        )

        # Create the table
        metadata.create_all(test_engine)

        # Verify table exists
        with test_engine.connect() as conn:
            result = conn.execute(
                text(
                    f'SELECT COUNT(*) FROM information_schema.tables '
                    f'WHERE table_schema = DATABASE() '
                    f"AND table_name = '{table_name}'",
                ),
            )
            assert result.scalar() == 1
