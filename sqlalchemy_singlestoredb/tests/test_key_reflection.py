#!/usr/bin/env python
"""Test reflection of various SingleStore KEY patterns."""
from __future__ import annotations

from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.engine import Engine


class TestKeyReflection:
    """Test reflection of SHARD KEY, SORT KEY, and PRIMARY KEY patterns."""

    def test_shard_key_empty_parentheses(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test reflection of SHARD KEY with empty parentheses."""
        table_name = 'test_shard_empty'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table without explicit SHARD KEY (SingleStore adds one)
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT,
                        name VARCHAR(100)
                    )
                """),
                )

            # Verify the table can be reflected without warnings
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) == 2
            assert 'id' in reflected_table.columns
            assert 'name' in reflected_table.columns

    def test_shard_key_with_columns(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test reflection of SHARD KEY with columns."""
        table_name = 'test_shard_columns'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with explicit SHARD KEY
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        user_id INT,
                        order_id INT,
                        amount DECIMAL(10,2),
                        PRIMARY KEY (user_id, order_id),
                        SHARD KEY (user_id)
                    )
                """),
                )

            # Verify the table can be reflected without warnings
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) == 3
            assert 'user_id' in reflected_table.columns
            assert 'order_id' in reflected_table.columns
            assert 'amount' in reflected_table.columns

            # Should have composite primary key
            pk_columns = [col.name for col in reflected_table.primary_key.columns]
            assert set(pk_columns) == {'user_id', 'order_id'}

    def test_sort_key_with_columns(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test reflection of SORT KEY with columns."""
        table_name = 'test_sort_columns'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with SORT KEY
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        user_id INT,
                        timestamp DATETIME,
                        event_type VARCHAR(50),
                        PRIMARY KEY (user_id, timestamp),
                        SHARD KEY (user_id),
                        SORT KEY (timestamp)
                    )
                """),
                )

            # Verify the table can be reflected without warnings
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) == 3
            assert 'user_id' in reflected_table.columns
            assert 'timestamp' in reflected_table.columns
            assert 'event_type' in reflected_table.columns

    def test_sort_key_empty_parentheses(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test reflection of SORT KEY with empty parentheses."""
        table_name = 'test_sort_empty'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table that will get automatic SORT KEY
                conn.execute(
                    text(f"""
                    CREATE ROWSTORE TABLE {table_name} (
                        id INT,
                        data TEXT
                    )
                """),
                )

            # Show CREATE TABLE to see what SingleStore generated
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print(f'Generated CREATE TABLE for {table_name}:')
            print(create_sql)

            # Verify the table can be reflected without warnings
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) == 2
            assert 'id' in reflected_table.columns
            assert 'data' in reflected_table.columns

    def test_named_sort_key_empty_parentheses(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test reflection of named SORT KEY with empty parentheses."""
        table_name = 'test_named_sort_empty'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with explicit empty SORT KEY
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        name VARCHAR(100),
                        SORT KEY custom_sort ()
                    )
                """),
                )

            # Verify the table can be reflected without warnings
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) == 2
            assert 'id' in reflected_table.columns
            assert 'name' in reflected_table.columns

    def test_primary_key_single_column(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test reflection of single column PRIMARY KEY."""
        table_name = 'test_pk_single'

        with test_engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        name VARCHAR(100)
                    )
                """),
                )

            # Verify the table can be reflected without warnings
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have single column primary key
            pk_columns = [col.name for col in reflected_table.primary_key.columns]
            assert pk_columns == ['id']

    def test_primary_key_composite(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test reflection of composite PRIMARY KEY."""
        table_name = 'test_pk_composite'

        with test_engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        tenant_id INT,
                        user_id INT,
                        name VARCHAR(100),
                        PRIMARY KEY (tenant_id, user_id),
                        SHARD KEY (tenant_id)
                    )
                """),
                )

            # Verify the table can be reflected without warnings
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have composite primary key
            pk_columns = [col.name for col in reflected_table.primary_key.columns]
            assert set(pk_columns) == {'tenant_id', 'user_id'}

    def test_complex_key_combination(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test reflection of complex KEY combinations."""
        table_name = 'test_complex_keys'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with multiple key types and regular indexes
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        tenant_id INT,
                        user_id INT,
                        order_id INT,
                        created_at DATETIME,
                        status VARCHAR(20),
                        amount DECIMAL(10,2),

                        PRIMARY KEY (tenant_id, user_id, order_id),
                        SHARD KEY (tenant_id),
                        SORT KEY (created_at),

                        KEY idx_status (status),
                        KEY idx_amount (amount),
                        KEY idx_user_created (user_id, created_at)
                    )
                """),
                )

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print(f'\\nGenerated CREATE TABLE for {table_name}:')
            print(create_sql)

            # Verify the table can be reflected without warnings
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have all columns
            expected_columns = {
                'tenant_id', 'user_id', 'order_id', 'created_at',
                'status', 'amount',
            }
            actual_columns = {col.name for col in reflected_table.columns}
            assert actual_columns == expected_columns

            # Should have composite primary key
            pk_columns = [col.name for col in reflected_table.primary_key.columns]
            assert set(pk_columns) == {'tenant_id', 'user_id', 'order_id'}

            # Should have regular indexes (not SHARD/SORT keys)
            indexes = reflected_table.indexes
            index_names = {idx.name for idx in indexes}

            # Should include the regular indexes we created
            expected_indexes = {'idx_status', 'idx_amount', 'idx_user_created'}
            assert expected_indexes.issubset(index_names)

    def test_table_with_using_hash_key(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test reflection of KEY with USING HASH."""
        table_name = 'test_using_hash'

        with test_engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id VARCHAR(100) NOT NULL,
                        name VARCHAR(255),
                        value BIGINT,
                        PRIMARY KEY (id) USING HASH
                    )
                """),
                )

            # Verify the table can be reflected without warnings
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) == 3
            assert 'id' in reflected_table.columns
            assert 'name' in reflected_table.columns
            assert 'value' in reflected_table.columns

            # Should have primary key
            pk_columns = [col.name for col in reflected_table.primary_key.columns]
            assert pk_columns == ['id']

    def test_key_with_comment_and_parser(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test reflection of KEY with various options."""
        table_name = 'test_key_options'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with keys that have various options
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT,
                        title VARCHAR(255),
                        content TEXT,
                        tags VARCHAR(500),

                        PRIMARY KEY (id),
                        KEY idx_title (title),
                        FULLTEXT KEY idx_content (content),
                        KEY idx_tags (tags) COMMENT 'Index for tag searches'
                    )
                """),
                )

            # Show what SingleStore actually generates
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print(f'\\nGenerated CREATE TABLE for {table_name}:')
            print(create_sql)

            # Verify the table can be reflected without warnings
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have all columns
            assert len(reflected_table.columns) == 4
            expected_columns = {'id', 'title', 'content', 'tags'}
            actual_columns = {col.name for col in reflected_table.columns}
            assert actual_columns == expected_columns

            # Should have indexes
            indexes = reflected_table.indexes
            index_names = {idx.name for idx in indexes}
            print(f'\\nReflected indexes: {index_names}')

            # Should include all indexes including FULLTEXT
            expected_indexes = {'idx_title', 'idx_content', 'idx_tags'}
            assert expected_indexes.issubset(index_names)
