#!/usr/bin/env python3
"""
Comprehensive tests for SortKey DDL element and integration.

These tests mirror the comprehensive shard key tests to ensure
equivalent functionality and coverage for sort keys.
"""
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
from sqlalchemy.orm import declarative_base

from sqlalchemy_singlestoredb import SortKey


class TestSortKeyConstruction:
    """Test SortKey DDL element construction and representation."""

    def test_basic_sort_key(self) -> None:
        """Test basic sort key with single column."""
        sort_key = SortKey('created_at')
        assert sort_key.columns == [('created_at', 'ASC')]
        assert repr(sort_key) == "SortKey('created_at')"

    def test_multi_column_sort_key(self) -> None:
        """Test sort key with multiple columns."""
        sort_key = SortKey('user_id', 'created_at')
        assert sort_key.columns == [('user_id', 'ASC'), ('created_at', 'ASC')]
        assert repr(sort_key) == "SortKey('user_id', 'created_at')"

    def test_empty_sort_key(self) -> None:
        """Test empty sort key (should be allowed)."""
        sort_key = SortKey()
        assert sort_key.columns == []
        assert repr(sort_key) == 'SortKey()'

    def test_sort_key_with_mixed_column_types(self) -> None:
        """Test sort key with different column reference types."""
        sort_key = SortKey('timestamp', 'priority', 'status')
        expected = [('timestamp', 'ASC'), ('priority', 'ASC'), ('status', 'ASC')]
        assert sort_key.columns == expected
        assert repr(sort_key) == "SortKey('timestamp', 'priority', 'status')"

    def test_single_string_sort_key(self) -> None:
        """Test sort key with single string input (convenience syntax)."""
        sort_key = SortKey('created_at')
        assert sort_key.columns == [('created_at', 'ASC')]
        assert repr(sort_key) == "SortKey('created_at')"


class TestSortKeyStaticMethods:
    """Test SortKey static helper methods."""

    def test_asc_static_method(self) -> None:
        """Test SortKey.asc() static method."""
        result = SortKey.asc('created_at')
        assert result == ('created_at', 'ASC')
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] == 'created_at'
        assert result[1] == 'ASC'

    def test_desc_static_method(self) -> None:
        """Test SortKey.desc() static method."""
        result = SortKey.desc('created_at')
        assert result == ('created_at', 'DESC')
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] == 'created_at'
        assert result[1] == 'DESC'

    def test_static_methods_in_constructor(self) -> None:
        """Test using static methods in SortKey constructor."""
        sort_key = SortKey(
            SortKey.asc('user_id'),
            SortKey.desc('created_at'),
            SortKey.asc('priority'),
        )
        expected = [
            ('user_id', 'ASC'),
            ('created_at', 'DESC'),
            ('priority', 'ASC'),
        ]
        assert sort_key.columns == expected


class TestSortKeyListAPI:
    """Test SortKey list-based API functionality."""

    def test_list_api_single_column(self) -> None:
        """Test list API with single column."""
        sort_key = SortKey('created_at')
        assert sort_key.columns == [('created_at', 'ASC')]

    def test_list_api_multiple_columns(self) -> None:
        """Test list API with multiple columns."""
        sort_key = SortKey('user_id', 'created_at', 'priority')
        expected = [
            ('user_id', 'ASC'),
            ('created_at', 'ASC'),
            ('priority', 'ASC'),
        ]
        assert sort_key.columns == expected

    def test_list_api_with_tuples(self) -> None:
        """Test list API with tuple specifications."""
        sort_key = SortKey(
            ('user_id', 'ASC'),
            ('created_at', 'DESC'),
            ('priority', 'ASC'),
        )
        expected = [
            ('user_id', 'ASC'),
            ('created_at', 'DESC'),
            ('priority', 'ASC'),
        ]
        assert sort_key.columns == expected

    def test_list_api_mixed_formats(self) -> None:
        """Test list API with mixed string and tuple formats."""
        sort_key = SortKey(
            'user_id',
            ('created_at', 'DESC'),
            'priority',
            ('status', 'ASC'),
        )
        expected = [
            ('user_id', 'ASC'),
            ('created_at', 'DESC'),
            ('priority', 'ASC'),
            ('status', 'ASC'),
        ]
        assert sort_key.columns == expected

    def test_empty_list(self) -> None:
        """Test empty list in constructor."""
        sort_key = SortKey()
        assert sort_key.columns == []

    def test_case_insensitive_directions(self) -> None:
        """Test that directions are case-insensitive."""
        sort_key = SortKey(
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
        assert sort_key.columns == expected


class TestSortKeyErrorHandling:
    """Test SortKey error handling and validation."""

    def test_invalid_direction_raises_error(self) -> None:
        """Test that invalid direction raises ValueError."""
        expected_msg = "Direction must be 'ASC' or 'DESC', got 'INVALID'"
        with pytest.raises(ValueError, match=expected_msg):
            SortKey(('created_at', 'INVALID'))

    def test_invalid_direction_case_raises_error(self) -> None:
        """Test that invalid direction with different case raises ValueError."""
        expected_msg = "Direction must be 'ASC' or 'DESC', got 'ASCENDING'"
        with pytest.raises(ValueError, match=expected_msg):
            SortKey(('created_at', 'ASCENDING'))

    def test_empty_direction_raises_error(self) -> None:
        """Test that empty direction raises ValueError."""
        with pytest.raises(ValueError, match="Direction must be 'ASC' or 'DESC', got ''"):
            SortKey(('created_at', ''))

    def test_none_direction_raises_error(self) -> None:
        """Test that None direction raises appropriate error."""
        with pytest.raises((ValueError, TypeError)):
            SortKey(('created_at', None))  # type: ignore


class TestSortKeyRepr:
    """Test SortKey string representation."""

    def test_repr_single_asc_column(self) -> None:
        """Test __repr__ for single ascending column."""
        sort_key = SortKey('created_at')
        assert repr(sort_key) == "SortKey('created_at')"

    def test_repr_single_desc_column(self) -> None:
        """Test __repr__ for single descending column."""
        sort_key = SortKey(('created_at', 'DESC'))
        assert repr(sort_key) == "SortKey(('created_at', 'DESC'))"

    def test_repr_mixed_columns(self) -> None:
        """Test __repr__ for mixed ascending and descending columns."""
        sort_key = SortKey('user_id', ('created_at', 'DESC'))
        assert repr(sort_key) == "SortKey('user_id', ('created_at', 'DESC'))"

    def test_repr_empty_sort_key(self) -> None:
        """Test __repr__ for empty sort key."""
        sort_key = SortKey()
        assert repr(sort_key) == 'SortKey()'

    def test_repr_all_explicit(self) -> None:
        """Test __repr__ with all explicit directions."""
        sort_key = SortKey(('user_id', 'ASC'), ('created_at', 'DESC'))
        assert repr(sort_key) == "SortKey('user_id', ('created_at', 'DESC'))"


class TestSortKeyCompiler:
    """Test SortKey DDL compilation."""

    def setup_method(self) -> None:
        """Set up mock engine for DDL compilation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_sql = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_sql = ''

    def test_compile_basic_sort_key(self) -> None:
        """Test compilation of basic sort key."""
        from sqlalchemy_singlestoredb.ddlelement import compile_sort_key

        sort_key = SortKey('created_at')
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (created_at)'

    def test_compile_multi_column_sort_key(self) -> None:
        """Test compilation of multi-column sort key."""
        from sqlalchemy_singlestoredb.ddlelement import compile_sort_key

        sort_key = SortKey('user_id', 'created_at')
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (user_id, created_at)'

    def test_compile_empty_sort_key(self) -> None:
        """Test compilation of empty sort key."""
        from sqlalchemy_singlestoredb.ddlelement import compile_sort_key

        sort_key = SortKey()
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY ()'

    def test_compile_three_column_sort_key(self) -> None:
        """Test compilation of sort key with three columns."""
        from sqlalchemy_singlestoredb.ddlelement import compile_sort_key

        sort_key = SortKey('timestamp', 'priority', 'status')
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (timestamp, priority, status)'

    def test_compile_sort_key_with_list_api(self) -> None:
        """Test compilation with new list-based API."""
        from sqlalchemy_singlestoredb.ddlelement import compile_sort_key

        sort_key = SortKey('created_at')
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (created_at)'

    def test_compile_sort_key_with_descending(self) -> None:
        """Test compilation with descending sort key."""
        from sqlalchemy_singlestoredb.ddlelement import compile_sort_key

        sort_key = SortKey(('created_at', 'DESC'))
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (created_at DESC)'

    def test_compile_sort_key_with_mixed_directions(self) -> None:
        """Test compilation with mixed ascending and descending columns."""
        from sqlalchemy_singlestoredb.ddlelement import compile_sort_key

        sort_key = SortKey('user_id', ('created_at', 'DESC'))
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (user_id, created_at DESC)'

    def test_compile_sort_key_with_explicit_ascending(self) -> None:
        """Test compilation with explicit ascending direction."""
        from sqlalchemy_singlestoredb.ddlelement import compile_sort_key

        sort_key = SortKey(('user_id', 'ASC'), ('created_at', 'DESC'))
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (user_id, created_at DESC)'

    def test_compile_sort_key_with_static_methods(self) -> None:
        """Test compilation using static helper methods."""
        from sqlalchemy_singlestoredb.ddlelement import compile_sort_key

        sort_key = SortKey(SortKey.asc('user_id'), SortKey.desc('created_at'))
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (user_id, created_at DESC)'

    def test_compile_empty_sort_key_with_list(self) -> None:
        """Test compilation of empty sort key with list API."""
        from sqlalchemy_singlestoredb.ddlelement import compile_sort_key

        sort_key = SortKey()
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY ()'

    def test_compile_sort_key_with_special_column_names(self) -> None:
        """Test compilation of sort key with special column names."""
        from sqlalchemy_singlestoredb.ddlelement import compile_sort_key

        # Test single column with hyphen
        sort_key = SortKey('user-id')
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (`user-id`)'

        # Test single column with space
        sort_key = SortKey('created at')
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (`created at`)'

        # Test multiple columns with special characters
        sort_key = SortKey('user-id', 'created at', 'normal_column')
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (`user-id`, `created at`, normal_column)'

        # Test with descending and special characters
        sort_key = SortKey(('user-id', 'ASC'), ('created at', 'DESC'))
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (`user-id`, `created at` DESC)'

        # Test column with backticks
        sort_key = SortKey('column`with`backticks')
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (`column``with``backticks`)'

        # Test using static methods with special characters
        sort_key = SortKey(SortKey.asc('user-id'), SortKey.desc('created at'))
        result = compile_sort_key(sort_key, None)
        assert result == 'SORT KEY (`user-id`, `created at` DESC)'


class TestSortKeyTableIntegration:
    """Test SortKey integration with table creation."""

    def setup_method(self) -> None:
        """Set up mock engine for table DDL generation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''
        # Create metadata without binding (SQLAlchemy 2.0 compatibility)
        self.metadata = MetaData()

    def test_table_with_basic_sort_key(self) -> None:
        """Test table creation with basic sort key."""
        Base = declarative_base()

        class MyTable(Base):  # type: ignore
            __tablename__ = 'test_table'

            id = Column(Integer, primary_key=True)
            created_at = Column(String(50))
            data = Column(String(50))

            __table_args__ = {
                'singlestoredb_sort_key': SortKey('created_at'),
            }

        Base.metadata.create_all(self.mock_engine, checkfirst=False)

        # Check that SORT KEY appears in the DDL
        assert 'SORT KEY (created_at)' in self.compiled_ddl
        assert 'CREATE TABLE test_table' in self.compiled_ddl

    def test_table_with_empty_sort_key(self) -> None:
        """Test table creation with empty sort key."""
        Base = declarative_base()

        class MyTable(Base):  # type: ignore
            __tablename__ = 'test_table_empty'

            id = Column(Integer, primary_key=True)
            data = Column(String(50))

            __table_args__ = {
                'singlestoredb_sort_key': SortKey(),
            }

        Base.metadata.create_all(self.mock_engine, checkfirst=False)

        # Check that empty SORT KEY appears in the DDL
        assert 'SORT KEY ()' in self.compiled_ddl
        assert 'CREATE TABLE test_table_empty' in self.compiled_ddl

    def test_table_with_multi_column_sort_key(self) -> None:
        """Test table creation with multi-column sort key."""
        Base = declarative_base()

        class MyTable(Base):  # type: ignore
            __tablename__ = 'test_table_multi'

            id = Column(Integer, primary_key=True)
            user_id = Column(Integer)
            created_at = Column(String(50))
            data = Column(String(50))

            __table_args__ = {
                'singlestoredb_sort_key': SortKey('user_id', 'created_at'),
            }

        Base.metadata.create_all(self.mock_engine, checkfirst=False)

        # Check that multi-column SORT KEY appears in the DDL
        assert 'SORT KEY (user_id, created_at)' in self.compiled_ddl
        assert 'CREATE TABLE test_table_multi' in self.compiled_ddl

    def test_table_with_three_column_sort_key(self) -> None:
        """Test table creation with three-column sort key."""
        Base = declarative_base()

        class MyTable(Base):  # type: ignore
            __tablename__ = 'test_table_three'

            id = Column(Integer, primary_key=True)
            timestamp = Column(String(50))
            priority = Column(Integer)
            status = Column(String(20))
            data = Column(String(50))

            __table_args__ = {
                'singlestoredb_sort_key': SortKey(
                    'timestamp', 'priority', 'status',
                ),
            }

        Base.metadata.create_all(self.mock_engine, checkfirst=False)

        # Check that three-column SORT KEY appears in the DDL
        assert 'SORT KEY (timestamp, priority, status)' in self.compiled_ddl
        assert 'CREATE TABLE test_table_three' in self.compiled_ddl


class TestSortKeyReflection:
    """Test SORT KEY reflection from actual database tables."""

    def test_reflect_basic_sort_key(self, test_engine: Any, clean_tables: Any) -> None:
        """Test reflection of basic SORT KEY with columns."""
        table_name = 'test_sort_reflection_basic'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with SORT KEY
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        user_id INT PRIMARY KEY,
                        created_at DATETIME,
                        data VARCHAR(50),
                        SORT KEY (created_at)
                    )
                """),
                )

            # Reflect the table
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Verify basic structure
            assert len(reflected_table.columns) == 3
            assert 'user_id' in reflected_table.columns
            assert 'created_at' in reflected_table.columns
            assert 'data' in reflected_table.columns

    def test_reflect_multi_column_sort_key(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of multi-column SORT KEY."""
        table_name = 'test_sort_reflection_multi'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with multi-column SORT KEY
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        user_id INT,
                        order_id INT,
                        created_at DATETIME,
                        amount DECIMAL(10,2),
                        PRIMARY KEY (user_id, order_id),
                        SHARD KEY (user_id),
                        SORT KEY (created_at, amount)
                    )
                """),
                )

            # Reflect the table
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Verify structure
            assert len(reflected_table.columns) == 4
            assert set(col.name for col in reflected_table.primary_key.columns) == {
                'user_id', 'order_id',
            }

    def test_reflect_empty_sort_key(self, test_engine: Any, clean_tables: Any) -> None:
        """Test reflection of empty SORT KEY."""
        table_name = 'test_sort_reflection_empty'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with empty SORT KEY
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        data VARCHAR(50),
                        SORT KEY ()
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

    def test_reflect_complex_table_with_sort_key(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of complex table with SHARD KEY, SORT KEY, and indexes."""
        table_name = 'test_sort_reflection_complex'

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
                        priority INT,
                        PRIMARY KEY (user_id, order_id),
                        SHARD KEY (user_id),
                        SORT KEY (created_at, priority),
                        KEY idx_product (product_name),
                        KEY idx_amount (amount)
                    )
                """),
                )

            # Reflect the table
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Verify structure
            assert len(reflected_table.columns) == 6
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


class TestSortKeyReflectionParser:
    """Test the reflection parser directly with various SORT KEY patterns."""

    def test_parser_sort_key_variants(self) -> None:
        """Test that the reflection parser correctly identifies all SORT KEY variants."""
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
                'line': '  SORT KEY (created_at)',
                'expected_type': 'sort_key',
                'expected_columns': ['created_at'],
            },
            {
                'line': '  SORT KEY (user_id, created_at)',
                'expected_type': 'sort_key',
                'expected_columns': ['user_id', 'created_at'],
            },
            {
                'line': '  SORT KEY ()',
                'expected_type': 'sort_key',
                'expected_columns': [],
            },
            {
                'line': '  SORT KEY (timestamp, priority, status)',
                'expected_type': 'sort_key',
                'expected_columns': ['timestamp', 'priority', 'status'],
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
                'line': '  SORT KEY (`created_at`)',
                'expected_columns': ['created_at'],
            },
            {
                'line': '  SORT KEY (`user_id`, `created_at`)',
                'expected_columns': ['user_id', 'created_at'],
            },
            {
                # Column name with special characters
                'line': '  SORT KEY (`order-timestamp`)',
                'expected_columns': ['order-timestamp'],
            },
        ]

        for case in test_cases:
            type_, spec = parser._parse_constraints(str(case['line']))
            assert type_ == 'sort_key'
            assert spec['columns'] == case['expected_columns'], (
                f"Line: {case['line']}, Expected: "
                f"{case['expected_columns']}, Got: {spec['columns']}"
            )


class TestSortKeyTableConstructorIntegration:
    """Test SingleStore Table constructor with sort_key parameter integration."""

    def setup_method(self) -> None:
        """Set up mock engine for DDL generation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''
        # Create metadata without binding (SQLAlchemy 2.0 compatibility)
        self.metadata = MetaData()

    def test_table_constructor_basic_sort_key(self) -> None:
        """Test Table constructor with basic sort key parameter."""
        table = Table(
            'test_basic', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('created_at', String(50)),
            Column('data', String(50)),
            singlestoredb_sort_key=SortKey('created_at'),
        )

        # Verify dialect options are set correctly
        assert 'singlestoredb' in table.dialect_options
        assert 'sort_key' in table.dialect_options['singlestoredb']
        sort_key = table.dialect_options['singlestoredb']['sort_key']
        assert isinstance(sort_key, SortKey)
        assert sort_key.columns == [('created_at', 'ASC')]

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SORT KEY (created_at)' in self.compiled_ddl
        assert 'CREATE TABLE test_basic' in self.compiled_ddl

    def test_table_constructor_empty_sort_key(self) -> None:
        """Test Table constructor with empty sort key parameter."""
        table = Table(
            'test_empty', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_sort_key=SortKey(),
        )

        # Verify dialect options are set correctly
        sort_key = table.dialect_options['singlestoredb']['sort_key']
        assert sort_key.columns == []

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SORT KEY ()' in self.compiled_ddl
        assert 'CREATE TABLE test_empty' in self.compiled_ddl

    def test_table_constructor_multi_column_sort_key(self) -> None:
        """Test Table constructor with multi-column sort key."""
        table = Table(
            'test_multi', self.metadata,
            Column('user_id', Integer, primary_key=True),
            Column('created_at', String(50), primary_key=True),
            Column('priority', Integer),
            Column('amount', Integer),
            singlestoredb_sort_key=SortKey('created_at', 'priority'),
        )

        # Verify dialect options are set correctly
        sort_key = table.dialect_options['singlestoredb']['sort_key']
        assert sort_key.columns == [('created_at', 'ASC'), ('priority', 'ASC')]

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SORT KEY (created_at, priority)' in self.compiled_ddl
        assert 'CREATE TABLE test_multi' in self.compiled_ddl

    def test_table_constructor_preserves_existing_info(self) -> None:
        """Test that Table constructor preserves existing info dictionary."""
        table = Table(
            'test_preserve', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('created_at', String(50)),
            Column('data', String(50)),
            singlestoredb_sort_key=SortKey('created_at'),
            info={'custom_key': 'custom_value'},
        )

        # Verify both custom info and dialect options are preserved
        assert table.info['custom_key'] == 'custom_value'
        assert 'singlestoredb' in table.dialect_options
        assert table.dialect_options['singlestoredb']['sort_key'].columns == [
            ('created_at', 'ASC'),
        ]

    def test_table_constructor_no_sort_key(self) -> None:
        """Test that Table constructor works normally without sort key parameters."""
        table = Table(
            'test_normal', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
        )

        # Should not have sort key dialect options
        assert (
            'singlestoredb' not in table.dialect_options or
            'sort_key' not in table.dialect_options.get('singlestoredb', {})
        )

        # Test DDL generation (should work normally)
        table.create(self.mock_engine, checkfirst=False)
        assert 'CREATE TABLE test_normal' in self.compiled_ddl
        assert 'SORT KEY' not in self.compiled_ddl

    def test_table_constructor_single_string_sort_key(self) -> None:
        """Test Table constructor with single string sort key (convenience syntax)."""
        table = Table(
            'test_string', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('created_at', String(50)),
            Column('data', String(50)),
            singlestoredb_sort_key=SortKey('created_at'),  # Single string syntax
        )

        # Verify dialect options are set correctly
        assert 'singlestoredb' in table.dialect_options
        assert 'sort_key' in table.dialect_options['singlestoredb']
        sort_key = table.dialect_options['singlestoredb']['sort_key']
        assert isinstance(sort_key, SortKey)
        assert sort_key.columns == [('created_at', 'ASC')]

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SORT KEY (created_at)' in self.compiled_ddl
        assert 'CREATE TABLE test_string' in self.compiled_ddl
