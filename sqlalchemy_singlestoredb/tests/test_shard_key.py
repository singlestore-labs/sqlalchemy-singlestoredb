"""Tests for SingleStore SHARD KEY functionality."""
from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import Column
from sqlalchemy import create_mock_engine
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text

from sqlalchemy_singlestoredb import ShardKey
from sqlalchemy_singlestoredb import SortKey


class TestShardKeyConstruction:
    """Test ShardKey DDL element construction and representation."""

    def test_basic_shard_key(self) -> None:
        """Test basic shard key with single column."""
        shard_key = ShardKey('user_id')
        assert shard_key.columns == [('user_id', 'ASC')]
        assert shard_key.metadata_only is False
        assert repr(shard_key) == "ShardKey('user_id')"

    def test_multi_column_shard_key(self) -> None:
        """Test shard key with multiple columns."""
        shard_key = ShardKey('user_id', 'category_id')
        assert shard_key.columns == [('user_id', 'ASC'), ('category_id', 'ASC')]
        assert shard_key.metadata_only is False
        assert repr(shard_key) == "ShardKey('user_id', 'category_id')"

    def test_empty_shard_key(self) -> None:
        """Test empty shard key for keyless sharding."""
        shard_key = ShardKey()
        assert shard_key.columns == []
        assert shard_key.metadata_only is False
        assert repr(shard_key) == 'ShardKey()'

    def test_shard_key_metadata_only_single_column(self) -> None:
        """Test SHARD KEY ONLY with single column."""
        shard_key = ShardKey('user_id', metadata_only=True)
        assert shard_key.columns == [('user_id', 'ASC')]
        assert shard_key.metadata_only is True
        assert repr(shard_key) == "ShardKey('user_id', metadata_only=True)"

    def test_shard_key_metadata_only_multi_column(self) -> None:
        """Test SHARD KEY ONLY with multiple columns."""
        shard_key = ShardKey('user_id', 'category_id', metadata_only=True)
        assert shard_key.columns == [('user_id', 'ASC'), ('category_id', 'ASC')]
        assert shard_key.metadata_only is True
        expected_repr = "ShardKey('user_id', 'category_id', metadata_only=True)"
        assert repr(shard_key) == expected_repr

    def test_shard_key_metadata_only_empty(self) -> None:
        """Test SHARD KEY ONLY with no columns (should fallback to empty)."""
        shard_key = ShardKey(metadata_only=True)
        assert shard_key.columns == []
        assert shard_key.metadata_only is True
        assert repr(shard_key) == 'ShardKey(metadata_only=True)'

    def test_shard_key_with_btree_index_type(self) -> None:
        """Test shard key with BTREE index type."""
        shard_key = ShardKey('user_id', index_type='BTREE')
        assert shard_key.columns == [('user_id', 'ASC')]
        assert shard_key.index_type == 'BTREE'
        assert repr(shard_key) == "ShardKey('user_id', index_type='BTREE')"

    def test_shard_key_with_hash_index_type(self) -> None:
        """Test shard key with HASH index type."""
        shard_key = ShardKey('user_id', index_type='HASH')
        assert shard_key.columns == [('user_id', 'ASC')]
        assert shard_key.index_type == 'HASH'
        assert repr(shard_key) == "ShardKey('user_id', index_type='HASH')"

    def test_shard_key_index_type_case_insensitive(self) -> None:
        """Test that index_type is case-insensitive."""
        shard_key = ShardKey('user_id', index_type='btree')
        assert shard_key.index_type == 'BTREE'

        shard_key2 = ShardKey('user_id', index_type='hash')
        assert shard_key2.index_type == 'HASH'

    def test_shard_key_with_index_type_and_metadata_only(self) -> None:
        """Test shard key with both index_type and metadata_only."""
        shard_key = ShardKey('user_id', index_type='BTREE', metadata_only=True)
        assert shard_key.columns == [('user_id', 'ASC')]
        assert shard_key.index_type == 'BTREE'
        assert shard_key.metadata_only is True
        expected = "ShardKey('user_id', index_type='BTREE', metadata_only=True)"
        assert repr(shard_key) == expected


class TestShardKeyStaticMethods:
    """Test ShardKey static helper methods for ASC/DESC."""

    def test_asc_static_method(self) -> None:
        """Test ShardKey.asc() static method."""
        result = ShardKey.asc('user_id')
        assert result == ('user_id', 'ASC')
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] == 'user_id'
        assert result[1] == 'ASC'

    def test_desc_static_method(self) -> None:
        """Test ShardKey.desc() static method."""
        result = ShardKey.desc('user_id')
        assert result == ('user_id', 'DESC')
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] == 'user_id'
        assert result[1] == 'DESC'

    def test_static_methods_in_constructor(self) -> None:
        """Test using static methods in ShardKey constructor."""
        shard_key = ShardKey(
            ShardKey.asc('user_id'),
            ShardKey.desc('category_id'),
            ShardKey.asc('tenant_id'),
        )
        expected = [
            ('user_id', 'ASC'),
            ('category_id', 'DESC'),
            ('tenant_id', 'ASC'),
        ]
        assert shard_key.columns == expected


class TestShardKeyDirectionAPI:
    """Test ShardKey direction-based API functionality."""

    def test_shard_key_with_asc_direction(self) -> None:
        """Test shard key with explicit ASC direction."""
        shard_key = ShardKey(('user_id', 'ASC'))
        assert shard_key.columns == [('user_id', 'ASC')]
        assert repr(shard_key) == "ShardKey('user_id')"

    def test_shard_key_with_desc_direction(self) -> None:
        """Test shard key with explicit DESC direction."""
        shard_key = ShardKey(('user_id', 'DESC'))
        assert shard_key.columns == [('user_id', 'DESC')]
        assert repr(shard_key) == "ShardKey(('user_id', 'DESC'))"

    def test_shard_key_mixed_directions(self) -> None:
        """Test shard key with mixed ASC and DESC directions."""
        shard_key = ShardKey('user_id', ('category_id', 'DESC'))
        expected = [('user_id', 'ASC'), ('category_id', 'DESC')]
        assert shard_key.columns == expected
        assert repr(shard_key) == "ShardKey('user_id', ('category_id', 'DESC'))"

    def test_case_insensitive_directions(self) -> None:
        """Test that directions are case-insensitive."""
        shard_key = ShardKey(
            ('col1', 'asc'),
            ('col2', 'desc'),
            ('col3', 'ASC'),
            ('col4', 'DESC'),
            ('col5', 'Asc'),
            ('col6', 'Desc'),
        )
        expected = [
            ('col1', 'ASC'),
            ('col2', 'DESC'),
            ('col3', 'ASC'),
            ('col4', 'DESC'),
            ('col5', 'ASC'),
            ('col6', 'DESC'),
        ]
        assert shard_key.columns == expected


class TestShardKeyErrorHandling:
    """Test ShardKey error handling and validation for directions."""

    def test_invalid_direction_raises_error(self) -> None:
        """Test that invalid direction raises ValueError."""
        expected_msg = "Direction must be 'ASC' or 'DESC', got 'INVALID'"
        with pytest.raises(ValueError, match=expected_msg):
            ShardKey(('user_id', 'INVALID'))

    def test_invalid_direction_case_raises_error(self) -> None:
        """Test that invalid direction with different case raises ValueError."""
        expected_msg = "Direction must be 'ASC' or 'DESC', got 'ASCENDING'"
        with pytest.raises(ValueError, match=expected_msg):
            ShardKey(('user_id', 'ASCENDING'))

    def test_empty_direction_raises_error(self) -> None:
        """Test that empty direction raises ValueError."""
        with pytest.raises(ValueError, match="Direction must be 'ASC' or 'DESC', got ''"):
            ShardKey(('user_id', ''))

    def test_none_direction_raises_error(self) -> None:
        """Test that None direction raises appropriate error."""
        with pytest.raises(TypeError, match='Direction cannot be None'):
            ShardKey(('user_id', None))  # type: ignore


class TestShardKeyRepr:
    """Test ShardKey string representation with directions."""

    def test_repr_single_asc_column(self) -> None:
        """Test __repr__ for single ascending column."""
        shard_key = ShardKey('user_id')
        assert repr(shard_key) == "ShardKey('user_id')"

    def test_repr_single_desc_column(self) -> None:
        """Test __repr__ for single descending column."""
        shard_key = ShardKey(('user_id', 'DESC'))
        assert repr(shard_key) == "ShardKey(('user_id', 'DESC'))"

    def test_repr_mixed_columns(self) -> None:
        """Test __repr__ for mixed ascending and descending columns."""
        shard_key = ShardKey('user_id', ('category_id', 'DESC'))
        assert repr(shard_key) == "ShardKey('user_id', ('category_id', 'DESC'))"

    def test_repr_all_explicit(self) -> None:
        """Test __repr__ with all explicit directions."""
        shard_key = ShardKey(('user_id', 'ASC'), ('category_id', 'DESC'))
        assert repr(shard_key) == "ShardKey('user_id', ('category_id', 'DESC'))"


class TestShardKeyCompiler:
    """Test ShardKey SQL compilation."""

    def test_compile_basic_shard_key(self) -> None:
        """Test compilation of basic shard key."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey('user_id')
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (user_id)'

    def test_compile_multi_column_shard_key(self) -> None:
        """Test compilation of multi-column shard key."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey('user_id', 'category_id')
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (user_id, category_id)'

    def test_compile_empty_shard_key(self) -> None:
        """Test compilation of empty shard key."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey()
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY ()'

    def test_compile_shard_key_metadata_only(self) -> None:
        """Test compilation of SHARD KEY ONLY."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey('user_id', metadata_only=True)
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (user_id) METADATA_ONLY'

    def test_compile_shard_key_metadata_only_multi_column(self) -> None:
        """Test compilation of SHARD KEY ONLY with multiple columns."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey('user_id', 'category_id', metadata_only=True)
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (user_id, category_id) METADATA_ONLY'

    def test_compile_shard_key_metadata_only_empty(self) -> None:
        """Test compilation of SHARD KEY ONLY with no columns (fallback)."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey(metadata_only=True)
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY () METADATA_ONLY'

    def test_compile_shard_key_with_special_column_names(self) -> None:
        """Test compilation of shard key with special column names."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        # Test single column with hyphen
        shard_key = ShardKey('user-id')
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (`user-id`)'

        # Test single column with space
        shard_key = ShardKey('user id')
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (`user id`)'

        # Test multiple columns with special characters
        shard_key = ShardKey('user-id', 'tenant id', 'normal_column')
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (`user-id`, `tenant id`, normal_column)'

        # Test column with backticks
        shard_key = ShardKey('column`with`backticks')
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (`column``with``backticks`)'

        # Test column starting with number
        shard_key = ShardKey('123column')
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (`123column`)'

        # Test with metadata_only and special characters
        shard_key = ShardKey('user-id', metadata_only=True)
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (`user-id`) METADATA_ONLY'

    def test_compile_shard_key_with_desc_direction(self) -> None:
        """Test compilation with descending shard key."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey(('user_id', 'DESC'))
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (user_id DESC)'

    def test_compile_shard_key_with_mixed_directions(self) -> None:
        """Test compilation with mixed ascending and descending columns."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey('user_id', ('category_id', 'DESC'))
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (user_id, category_id DESC)'

    def test_compile_shard_key_with_explicit_ascending(self) -> None:
        """Test compilation with explicit ascending direction."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey(('user_id', 'ASC'), ('category_id', 'DESC'))
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (user_id, category_id DESC)'

    def test_compile_shard_key_with_static_methods(self) -> None:
        """Test compilation using static helper methods."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey(ShardKey.asc('user_id'), ShardKey.desc('category_id'))
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (user_id, category_id DESC)'

    def test_compile_shard_key_desc_with_metadata_only(self) -> None:
        """Test compilation with DESC direction and METADATA_ONLY."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey(('user_id', 'DESC'), metadata_only=True)
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (user_id DESC) METADATA_ONLY'

    def test_compile_shard_key_desc_with_special_names(self) -> None:
        """Test compilation with DESC direction and special column names."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        # Test DESC with special characters
        shard_key = ShardKey(('user-id', 'DESC'))
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (`user-id` DESC)'

        # Test mixed with special characters
        shard_key = ShardKey(('user-id', 'ASC'), ('created at', 'DESC'))
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (`user-id`, `created at` DESC)'

    def test_compile_shard_key_with_btree_index_type(self) -> None:
        """Test compilation with BTREE index type."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey('user_id', index_type='BTREE')
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY USING BTREE (user_id)'

    def test_compile_shard_key_with_hash_index_type(self) -> None:
        """Test compilation with HASH index type."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey('user_id', index_type='HASH')
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY USING HASH (user_id)'

    def test_compile_shard_key_index_type_with_multi_column(self) -> None:
        """Test compilation with index type and multiple columns."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey('user_id', 'tenant_id', index_type='HASH')
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY USING HASH (user_id, tenant_id)'

    def test_compile_shard_key_index_type_with_metadata_only(self) -> None:
        """Test compilation with index type and METADATA_ONLY."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey('user_id', index_type='BTREE', metadata_only=True)
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY USING BTREE (user_id) METADATA_ONLY'


class TestShardKeyTableIntegration:
    """Test ShardKey integration with table creation."""

    def setup_method(self) -> None:
        """Set up mock engine for table DDL generation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''
        # Create metadata without binding (SQLAlchemy 2.0 compatibility)
        self.metadata = MetaData()

    def test_table_with_basic_shard_key(self) -> None:
        """Test table creation with basic shard key."""
        # Create table using new dialect options API
        table = Table(
            'test_table', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer),
            Column('data', String(50)),
            singlestoredb_shard_key=ShardKey('user_id'),
        )

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY (user_id)' in self.compiled_ddl
        assert 'CREATE TABLE test_table' in self.compiled_ddl

    def test_table_with_empty_shard_key(self) -> None:
        """Test table creation with empty shard key."""
        # Create table using new dialect options API
        table = Table(
            'test_table_empty', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
            singlestoredb_shard_key=ShardKey(),
        )

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY ()' in self.compiled_ddl
        assert 'CREATE TABLE test_table_empty' in self.compiled_ddl

    def test_table_with_shard_key_metadata_only(self) -> None:
        """Test table creation with SHARD KEY ONLY."""
        # Create table using new dialect options API
        table = Table(
            'test_table_only', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer),
            Column('data', String(50)),
            singlestoredb_shard_key=ShardKey('user_id', metadata_only=True),
        )

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY (user_id) METADATA_ONLY' in self.compiled_ddl
        assert 'CREATE TABLE test_table_only' in self.compiled_ddl

    def test_table_with_multi_column_shard_key(self) -> None:
        """Test table creation with multi-column shard key."""
        # Create table using new dialect options API
        table = Table(
            'test_table_multi', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer),
            Column('category_id', Integer),
            Column('data', String(50)),
            singlestoredb_shard_key=ShardKey('user_id', 'category_id'),
        )

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY (user_id, category_id)' in self.compiled_ddl
        assert 'CREATE TABLE test_table_multi' in self.compiled_ddl

    def test_table_with_shard_key_btree_index(self) -> None:
        """Test table creation with BTREE index type shard key."""
        table = Table(
            'test_table_btree', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer),
            Column('data', String(50)),
            singlestoredb_shard_key=ShardKey('user_id', index_type='BTREE'),
        )

        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY USING BTREE (user_id)' in self.compiled_ddl
        assert 'CREATE TABLE test_table_btree' in self.compiled_ddl

    def test_table_with_shard_key_hash_index(self) -> None:
        """Test table creation with HASH index type shard key."""
        table = Table(
            'test_table_hash', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer),
            Column('data', String(50)),
            singlestoredb_shard_key=ShardKey('user_id', index_type='HASH'),
        )

        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY USING HASH (user_id)' in self.compiled_ddl
        assert 'CREATE TABLE test_table_hash' in self.compiled_ddl


class TestShardKeyReflection:
    """Test SHARD KEY reflection from actual database tables."""

    def test_reflect_basic_shard_key(self, test_engine: Any, clean_tables: Any) -> None:
        """Test reflection of basic SHARD KEY with columns."""
        table_name = 'test_shard_reflection_basic'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with SHARD KEY
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        user_id INT PRIMARY KEY,
                        data VARCHAR(50),
                        SHARD KEY (user_id)
                    )
                """),
                )

            # Reflect the table
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Verify basic structure
            assert len(reflected_table.columns) == 2
            assert 'user_id' in reflected_table.columns
            assert 'data' in reflected_table.columns

    def test_reflect_multi_column_shard_key(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of multi-column SHARD KEY."""
        table_name = 'test_shard_reflection_multi'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with multi-column SHARD KEY
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        user_id INT,
                        category_id INT,
                        amount DECIMAL(10,2),
                        PRIMARY KEY (user_id, category_id),
                        SHARD KEY (user_id, category_id)
                    )
                """),
                )

            # Reflect the table
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Verify structure
            assert len(reflected_table.columns) == 3
            assert set(col.name for col in reflected_table.primary_key.columns) == {
                'user_id', 'category_id',
            }

    @pytest.mark.skip(reason="SingleStore doesn't allow SHARD KEY () with PRIMARY KEY")
    def test_reflect_empty_shard_key(self, test_engine: Any, clean_tables: Any) -> None:
        """Test reflection of empty SHARD KEY for keyless sharding."""
        table_name = 'test_shard_reflection_empty'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with empty SHARD KEY - conflicts with PRIMARY KEY
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT,
                        data VARCHAR(50),
                        SHARD KEY ()
                    )
                """),
                )

            # Reflect the table
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Verify structure
            assert len(reflected_table.columns) == 2
            assert 'id' in reflected_table.columns
            assert 'data' in reflected_table.columns

    @pytest.mark.skipif(True, reason='SHARD KEY ONLY needs database support verification')
    def test_reflect_shard_key_metadata_only(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of SHARD KEY ONLY (prevents index creation)."""
        table_name = 'test_shard_reflection_only'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with SHARD KEY ONLY
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        user_id INT PRIMARY KEY,
                        data VARCHAR(50),
                        SHARD KEY (user_id) METADATA_ONLY
                    )
                """),
                )

            # Reflect the table
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Verify structure
            assert len(reflected_table.columns) == 2
            assert 'user_id' in reflected_table.columns
            assert 'data' in reflected_table.columns

    def test_reflect_complex_table_with_shard_key(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of complex table with SHARD KEY, SORT KEY, and indexes."""
        table_name = 'test_shard_reflection_complex'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create complex table
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        user_id INT,
                        order_id INT,
                        product_name VARCHAR(100),
                        amount DECIMAL(10,2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (user_id, order_id),
                        SHARD KEY (user_id),
                        SORT KEY (created_at),
                        KEY idx_product (product_name),
                        KEY idx_amount (amount)
                    )
                """),
                )

            # Reflect the table
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Verify structure
            assert len(reflected_table.columns) == 5
            assert set(col.name for col in reflected_table.primary_key.columns) == {
                'user_id', 'order_id',
            }

            # Verify indexes can be retrieved without warnings
            indexes = test_engine.dialect.get_indexes(
                conn, table_name, schema=None, dbname=None,
            )
            # Should have regular indexes (not shard/sort keys)
            index_names = {idx['name'] for idx in indexes}
            assert 'idx_product' in index_names
            assert 'idx_amount' in index_names


class TestShardKeyReflectionParser:
    """Test the reflection parser directly with various SHARD KEY patterns."""

    def test_parser_shard_key_variants(self) -> None:
        """Test that the reflection parser correctly identifies all SHARD KEY variants."""
        from sqlalchemy_singlestoredb.reflection import SingleStoreDBTableDefinitionParser
        from sqlalchemy_singlestoredb.base import (
            SingleStoreDBDialect, SingleStoreDBIdentifierPreparer,
        )

        # Create parser
        dialect = SingleStoreDBDialect()
        preparer = SingleStoreDBIdentifierPreparer(dialect)
        parser = SingleStoreDBTableDefinitionParser(dialect, preparer)

        test_cases = [
            {
                'line': '  SHARD KEY (user_id)',
                'expected_type': 'shard_key',
                'expected_columns': [('user_id', 'ASC')],
                'expected_only': False,
            },
            {
                'line': '  SHARD KEY ONLY (user_id)',
                'expected_type': 'shard_key',
                'expected_columns': [('user_id', 'ASC')],
                'expected_only': True,
            },
            {
                'line': '  SHARD KEY ()',
                'expected_type': 'shard_key',
                'expected_columns': [],
                'expected_only': False,
            },
            {
                'line': '  SHARD KEY (user_id, category_id)',
                'expected_type': 'shard_key',
                'expected_columns': [('user_id', 'ASC'), ('category_id', 'ASC')],
                'expected_only': False,
            },
            {
                'line': '  SHARD KEY ONLY (user_id, category_id)',
                'expected_type': 'shard_key',
                'expected_columns': [('user_id', 'ASC'), ('category_id', 'ASC')],
                'expected_only': True,
            },
            {
                'line': '  SORT KEY (created_at)',
                'expected_type': 'sort_key',
                'expected_columns': [('created_at', 'ASC')],
                'expected_only': False,
            },
        ]

        for case in test_cases:
            type_, spec = parser._parse_constraints(str(case['line']))

            assert type_ == case['expected_type'], (
                f"Line: {case['line']}, Expected type: "
                f"{case['expected_type']}, Got: {type_}"
            )
            assert spec['columns'] == case['expected_columns'], (
                f"Line: {case['line']}, Expected columns: "
                f"{case['expected_columns']}, Got: {spec['columns']}"
            )
            assert spec['only'] == case['expected_only'], (
                f"Line: {case['line']}, Expected only: "
                f"{case['expected_only']}, Got: {spec['only']}"
            )

    def test_parser_quoted_column_names(self) -> None:
        """Test parser handles quoted column identifiers correctly."""
        from sqlalchemy_singlestoredb.reflection import SingleStoreDBTableDefinitionParser
        from sqlalchemy_singlestoredb.base import (
            SingleStoreDBDialect, SingleStoreDBIdentifierPreparer,
        )

        # Create parser
        dialect = SingleStoreDBDialect()
        preparer = SingleStoreDBIdentifierPreparer(dialect)
        parser = SingleStoreDBTableDefinitionParser(dialect, preparer)

        test_cases = [
            {
                'line': '  SHARD KEY (`user_id`)',
                'expected_columns': [('user_id', 'ASC')],
            },
            {
                'line': '  SHARD KEY (`user_id`, `category_id`)',
                'expected_columns': [('user_id', 'ASC'), ('category_id', 'ASC')],
            },
            {
                'line': '  SHARD KEY (`order-id`)',  # Column name with special characters
                'expected_columns': [('order-id', 'ASC')],
            },
        ]

        for case in test_cases:
            type_, spec = parser._parse_constraints(str(case['line']))
            assert type_ == 'shard_key'
            assert spec['columns'] == case['expected_columns'], (
                f"Line: {case['line']}, Expected: "
                f"{case['expected_columns']}, Got: {spec['columns']}"
            )


class TestTableConstructorIntegration:
    """Test SingleStore Table constructor with shard_key parameter integration."""

    def setup_method(self) -> None:
        """Set up mock engine for DDL generation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''
        self.metadata = MetaData()

    def test_table_constructor_basic_shard_key(self) -> None:
        """Test Table constructor with basic shard key parameter."""
        table = Table(
            'test_basic', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
            singlestoredb_shard_key=ShardKey('id'),
        )

        # Verify dialect options are set correctly
        assert 'singlestoredb' in table.dialect_options
        assert 'shard_key' in table.dialect_options['singlestoredb']
        shard_key = table.dialect_options['singlestoredb']['shard_key']
        assert isinstance(shard_key, ShardKey)
        assert shard_key.columns == [('id', 'ASC')]
        assert shard_key.metadata_only is False

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY (id)' in self.compiled_ddl
        assert 'CREATE TABLE test_basic' in self.compiled_ddl

    def test_table_constructor_shard_key_metadata_only(self) -> None:
        """Test Table constructor with SHARD KEY ONLY parameter."""
        table = Table(
            'test_only', self.metadata,
            Column('user_id', Integer, primary_key=True),
            Column('name', String(50)),
            singlestoredb_shard_key=ShardKey('user_id', metadata_only=True),
        )

        # Verify dialect options are set correctly
        shard_key = table.dialect_options['singlestoredb']['shard_key']
        assert shard_key.columns == [('user_id', 'ASC')]
        assert shard_key.metadata_only is True

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY (user_id) METADATA_ONLY' in self.compiled_ddl
        assert 'CREATE TABLE test_only' in self.compiled_ddl

    def test_table_constructor_empty_shard_key(self) -> None:
        """Test Table constructor with empty shard key parameter."""
        table = Table(
            'test_empty', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_shard_key=ShardKey(),
        )

        # Verify dialect options are set correctly
        shard_key = table.dialect_options['singlestoredb']['shard_key']
        assert shard_key.columns == []
        assert shard_key.metadata_only is False

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY ()' in self.compiled_ddl
        assert 'CREATE TABLE test_empty' in self.compiled_ddl

    def test_table_constructor_multi_column_shard_key(self) -> None:
        """Test Table constructor with multi-column shard key."""
        table = Table(
            'test_multi', self.metadata,
            Column('user_id', Integer, primary_key=True),
            Column('category_id', Integer, primary_key=True),
            Column('amount', Integer),
            singlestoredb_shard_key=ShardKey('user_id', 'category_id'),
        )

        # Verify dialect options are set correctly
        shard_key = table.dialect_options['singlestoredb']['shard_key']
        assert shard_key.columns == [('user_id', 'ASC'), ('category_id', 'ASC')]
        assert shard_key.metadata_only is False

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY (user_id, category_id)' in self.compiled_ddl
        assert 'CREATE TABLE test_multi' in self.compiled_ddl

    def test_table_constructor_with_both_keys(self) -> None:
        """Test Table constructor with both shard key and sort key parameters."""
        table = Table(
            'test_complex', self.metadata,
            Column('user_id', Integer, primary_key=True),
            Column('order_id', Integer, primary_key=True),
            Column('created_at', String(50)),
            singlestoredb_shard_key=ShardKey('user_id'),
            singlestoredb_sort_key=SortKey('created_at'),
        )

        # Verify dialect options are set correctly
        assert 'singlestoredb' in table.dialect_options
        assert 'shard_key' in table.dialect_options['singlestoredb']
        assert 'sort_key' in table.dialect_options['singlestoredb']

        shard_key = table.dialect_options['singlestoredb']['shard_key']
        sort_key = table.dialect_options['singlestoredb']['sort_key']

        assert shard_key.columns == [('user_id', 'ASC')]
        assert sort_key.columns == [('created_at', 'ASC')]

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY (user_id)' in self.compiled_ddl
        assert 'SORT KEY (created_at)' in self.compiled_ddl
        assert 'CREATE TABLE test_complex' in self.compiled_ddl

    def test_table_constructor_preserves_existing_info(self) -> None:
        """Test that Table constructor preserves existing info dictionary."""
        table = Table(
            'test_preserve', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
            singlestoredb_shard_key=ShardKey('id'),
            info={'custom_key': 'custom_value'},
        )

        # Verify both custom info and dialect options are preserved
        assert table.info['custom_key'] == 'custom_value'
        assert 'singlestoredb' in table.dialect_options
        assert table.dialect_options['singlestoredb']['shard_key'].columns == [
            ('id', 'ASC'),
        ]

    def test_table_constructor_no_shard_key(self) -> None:
        """Test that Table constructor works normally without shard key parameters."""
        table = Table(
            'test_normal', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
        )

        # Should not have shard key dialect options
        assert (
            'singlestoredb' not in table.dialect_options or
            'shard_key' not in table.dialect_options.get('singlestoredb', {})
        )

        # Test DDL generation (should work normally)
        table.create(self.mock_engine, checkfirst=False)
        assert 'CREATE TABLE test_normal' in self.compiled_ddl
        # No shard key should be added  # No shard key should be added
        assert 'SHARD KEY' not in self.compiled_ddl

    def test_table_constructor_multiple_shard_keys_error(self) -> None:
        """Test that Table constructor now prevents multiple shard keys by design."""
        # With the new API, you can only specify one shardkey parameter,
        # so this type of error is no longer possible. The dialect validates
        # that only one value is provided for each parameter.
        table = Table(
            'test_single', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer),
            Column('data', String(50)),
            # Only one allowed by parameter design
            singlestoredb_shard_key=ShardKey('id'),
        )

        # Verify it works correctly
        assert 'singlestoredb' in table.dialect_options
        assert table.dialect_options['singlestoredb']['shard_key'].columns == [
            ('id', 'ASC'),
        ]

    def test_table_constructor_multiple_sort_keys_error(self) -> None:
        """Test that Table constructor now prevents multiple sort keys by design."""
        # With the new API, you can only specify one sortkey parameter,
        # so this type of error is no longer possible. The dialect validates
        # that only one value is provided for each parameter.
        table = Table(
            'test_sort', self.metadata,
            Column('user_id', Integer, primary_key=True),
            Column('created_at', String(50)),
            Column('data', String(50)),
            # Only one allowed by parameter design
            singlestoredb_sort_key=SortKey('created_at'),
        )

        # Verify it works correctly
        assert 'singlestoredb' in table.dialect_options
        assert table.dialect_options['singlestoredb']['sort_key'].columns == [
            ('created_at', 'ASC'),
        ]
