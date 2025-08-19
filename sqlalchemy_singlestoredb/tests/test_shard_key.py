"""Tests for SingleStore SHARD KEY functionality."""
from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import Column
from sqlalchemy import create_mock_engine
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import text
from sqlalchemy.orm import declarative_base

from sqlalchemy_singlestoredb import ShardKey
from sqlalchemy_singlestoredb import SortKey
from sqlalchemy_singlestoredb import Table


class TestShardKeyConstruction:
    """Test ShardKey DDL element construction and representation."""

    def test_basic_shard_key(self) -> None:
        """Test basic shard key with single column."""
        shard_key = ShardKey('user_id')
        assert shard_key.columns == ('user_id',)
        assert shard_key.metadata_only is False
        assert repr(shard_key) == "ShardKey('user_id')"

    def test_multi_column_shard_key(self) -> None:
        """Test shard key with multiple columns."""
        shard_key = ShardKey(['user_id', 'category_id'])
        assert shard_key.columns == ('user_id', 'category_id')
        assert shard_key.metadata_only is False
        assert repr(shard_key) == "ShardKey(['user_id', 'category_id'])"

    def test_empty_shard_key(self) -> None:
        """Test empty shard key for keyless sharding."""
        shard_key = ShardKey()
        assert shard_key.columns == ()
        assert shard_key.metadata_only is False
        assert repr(shard_key) == 'ShardKey()'

    def test_shard_key_metadata_only_single_column(self) -> None:
        """Test SHARD KEY ONLY with single column."""
        shard_key = ShardKey('user_id', metadata_only=True)
        assert shard_key.columns == ('user_id',)
        assert shard_key.metadata_only is True
        assert repr(shard_key) == "ShardKey('user_id', metadata_only=True)"

    def test_shard_key_metadata_only_multi_column(self) -> None:
        """Test SHARD KEY ONLY with multiple columns."""
        shard_key = ShardKey(['user_id', 'category_id'], metadata_only=True)
        assert shard_key.columns == ('user_id', 'category_id')
        assert shard_key.metadata_only is True
        expected_repr = "ShardKey(['user_id', 'category_id'], metadata_only=True)"
        assert repr(shard_key) == expected_repr

    def test_shard_key_metadata_only_empty(self) -> None:
        """Test SHARD KEY ONLY with no columns (should fallback to empty)."""
        shard_key = ShardKey(metadata_only=True)
        assert shard_key.columns == ()
        assert shard_key.metadata_only is True
        assert repr(shard_key) == 'ShardKey(None, metadata_only=True)'


class TestShardKeyCompiler:
    """Test ShardKey DDL compilation."""

    def setup_method(self) -> None:
        """Set up mock engine for DDL compilation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_sql = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_sql = ''

    def test_compile_basic_shard_key(self) -> None:
        """Test compilation of basic shard key."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey('user_id')
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (user_id)'

    def test_compile_multi_column_shard_key(self) -> None:
        """Test compilation of multi-column shard key."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey(['user_id', 'category_id'])
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

        shard_key = ShardKey(['user_id', 'category_id'], metadata_only=True)
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY (user_id, category_id) METADATA_ONLY'

    def test_compile_shard_key_metadata_only_empty(self) -> None:
        """Test compilation of SHARD KEY ONLY with no columns (fallback)."""
        from sqlalchemy_singlestoredb.ddlelement import compile_shard_key

        shard_key = ShardKey(metadata_only=True)
        result = compile_shard_key(shard_key, None)
        assert result == 'SHARD KEY ()'


class TestShardKeyTableIntegration:
    """Test ShardKey integration with table creation."""

    def setup_method(self) -> None:
        """Set up mock engine for table DDL generation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''
        self.metadata = MetaData()

    def test_table_with_basic_shard_key(self) -> None:
        """Test table creation with basic shard key."""
        Base = declarative_base()

        class MyTable(Base):  # type: ignore
            __tablename__ = 'test_table'

            id = Column(Integer, primary_key=True)
            user_id = Column(Integer)
            data = Column(String(50))

            __table_args__ = {
                'info': {'singlestoredb_shard_key': ShardKey('user_id')},
            }

        Base.metadata.create_all(self.mock_engine, checkfirst=False)

        # Check that SHARD KEY appears in the DDL
        assert 'SHARD KEY (user_id)' in self.compiled_ddl
        assert 'CREATE TABLE test_table' in self.compiled_ddl

    def test_table_with_empty_shard_key(self) -> None:
        """Test table creation with empty shard key."""
        Base = declarative_base()

        class MyTable(Base):  # type: ignore
            __tablename__ = 'test_table_empty'

            id = Column(Integer, primary_key=True)
            data = Column(String(50))

            __table_args__ = {
                'info': {'singlestoredb_shard_key': ShardKey()},
            }

        Base.metadata.create_all(self.mock_engine, checkfirst=False)

        # Check that empty SHARD KEY appears in the DDL
        assert 'SHARD KEY ()' in self.compiled_ddl
        assert 'CREATE TABLE test_table_empty' in self.compiled_ddl

    def test_table_with_shard_key_metadata_only(self) -> None:
        """Test table creation with SHARD KEY ONLY."""
        Base = declarative_base()

        class MyTable(Base):  # type: ignore
            __tablename__ = 'test_table_only'

            id = Column(Integer, primary_key=True)
            user_id = Column(Integer)
            data = Column(String(50))

            __table_args__ = {
                'info': {
                    'singlestoredb_shard_key': ShardKey('user_id', metadata_only=True),
                },
            }

        Base.metadata.create_all(self.mock_engine, checkfirst=False)

        # Check that SHARD KEY METADATA_ONLY appears in the DDL
        assert 'SHARD KEY (user_id) METADATA_ONLY' in self.compiled_ddl
        assert 'CREATE TABLE test_table_only' in self.compiled_ddl

    def test_table_with_multi_column_shard_key(self) -> None:
        """Test table creation with multi-column shard key."""
        Base = declarative_base()

        class MyTable(Base):  # type: ignore
            __tablename__ = 'test_table_multi'

            id = Column(Integer, primary_key=True)
            user_id = Column(Integer)
            category_id = Column(Integer)
            data = Column(String(50))

            __table_args__ = {
                'info': {'singlestoredb_shard_key': ShardKey(['user_id', 'category_id'])},
            }

        Base.metadata.create_all(self.mock_engine, checkfirst=False)

        # Check that multi-column SHARD KEY appears in the DDL
        assert 'SHARD KEY (user_id, category_id)' in self.compiled_ddl
        assert 'CREATE TABLE test_table_multi' in self.compiled_ddl


class TestShardKeyBackwardCompatibility:
    """Test backward compatibility with existing ShardKey usage."""

    def setup_method(self) -> None:
        """Set up mock engine for testing."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''

    def test_existing_usage_still_works(self) -> None:
        """Test that existing ShardKey('column') usage still works."""
        # This mimics the existing usage pattern from the examples
        Base = declarative_base()

        class MyTable(Base):  # type: ignore
            __tablename__ = 'my_new_table'

            id = Column(Integer, primary_key=True)
            data = Column(String(50))

            __table_args__ = {
                'info': {'singlestoredb_shard_key': ShardKey('id')},
            }

        Base.metadata.create_all(self.mock_engine, checkfirst=False)

        # Verify the old usage pattern still generates correct DDL
        assert 'SHARD KEY (id)' in self.compiled_ddl
        assert 'CREATE TABLE my_new_table' in self.compiled_ddl

        # Verify the ShardKey object has expected properties
        shard_key = ShardKey('id')
        assert shard_key.metadata_only is False  # New property defaults to False
        assert shard_key.columns == ('id',)  # Existing behavior preserved


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
                'expected_columns': ['user_id'],
                'expected_only': False,
            },
            {
                'line': '  SHARD KEY ONLY (user_id)',
                'expected_type': 'shard_key',
                'expected_columns': ['user_id'],
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
                'expected_columns': ['user_id', 'category_id'],
                'expected_only': False,
            },
            {
                'line': '  SHARD KEY ONLY (user_id, category_id)',
                'expected_type': 'shard_key',
                'expected_columns': ['user_id', 'category_id'],
                'expected_only': True,
            },
            {
                'line': '  SORT KEY (created_at)',
                'expected_type': 'sort_key',
                'expected_columns': ['created_at'],
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
                'expected_columns': ['user_id'],
            },
            {
                'line': '  SHARD KEY (`user_id`, `category_id`)',
                'expected_columns': ['user_id', 'category_id'],
            },
            {
                'line': '  SHARD KEY (`order-id`)',  # Column name with special characters
                'expected_columns': ['order-id'],
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
            ShardKey('id'),
        )

        # Verify info is set correctly
        assert 'singlestoredb_shard_key' in table.info
        assert isinstance(table.info['singlestoredb_shard_key'], ShardKey)
        assert table.info['singlestoredb_shard_key'].columns == ('id',)
        assert table.info['singlestoredb_shard_key'].metadata_only is False

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
            ShardKey('user_id', metadata_only=True),
        )

        # Verify info is set correctly
        shard_key = table.info['singlestoredb_shard_key']
        assert shard_key.columns == ('user_id',)
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
            ShardKey(),
        )

        # Verify info is set correctly
        shard_key = table.info['singlestoredb_shard_key']
        assert shard_key.columns == ()
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
            ShardKey(['user_id', 'category_id']),
        )

        # Verify info is set correctly
        shard_key = table.info['singlestoredb_shard_key']
        assert shard_key.columns == ('user_id', 'category_id')
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
            ShardKey('user_id'),
            SortKey(['created_at']),
        )

        # Verify info is set correctly
        assert 'singlestoredb_shard_key' in table.info
        assert 'singlestoredb_sort_key' in table.info

        shard_key = table.info['singlestoredb_shard_key']
        sort_key = table.info['singlestoredb_sort_key']

        assert shard_key.columns == ('user_id',)
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
            ShardKey('id'),
            info={'custom_key': 'custom_value'},
        )

        # Verify both custom info and shard key are preserved
        assert table.info['custom_key'] == 'custom_value'
        assert 'singlestoredb_shard_key' in table.info
        assert table.info['singlestoredb_shard_key'].columns == ('id',)

    def test_table_constructor_no_shard_key(self) -> None:
        """Test that Table constructor works normally without shard key parameters."""
        table = Table(
            'test_normal', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
        )

        # Should not have shard key info
        assert 'singlestoredb_shard_key' not in table.info

        # Test DDL generation (should work normally)
        table.create(self.mock_engine, checkfirst=False)
        assert 'CREATE TABLE test_normal' in self.compiled_ddl
        assert 'SHARD KEY' not in self.compiled_ddl  # No shard key should be added

    def test_table_constructor_multiple_shard_keys_error(self) -> None:
        """Test that Table constructor raises error when multiple ShardKeys provided."""
        with pytest.raises(
            ValueError,
            match='Only one ShardKey can be specified per table',
        ):
            Table(
                'test_error', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('user_id', Integer),
                Column('data', String(50)),
                ShardKey('id'),
                ShardKey('user_id'),  # This should cause an error
            )

    def test_table_constructor_multiple_sort_keys_error(self) -> None:
        """Test that Table constructor raises error when multiple SortKeys provided."""
        with pytest.raises(
            ValueError,
            match='Only one SortKey can be specified per table',
        ):
            Table(
                'test_error', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('created_at', String(50)),
                Column('updated_at', String(50)),
                SortKey(['created_at']),
                SortKey(['updated_at']),  # This should cause an error
            )
