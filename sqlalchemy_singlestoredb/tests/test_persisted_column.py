"""Tests for SingleStore PersistedColumn functionality."""
from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import Column
from sqlalchemy import create_mock_engine
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Numeric
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.sql.expression import TextClause

from sqlalchemy_singlestoredb import PersistedColumn


class TestPersistedColumnConstruction:
    """Test PersistedColumn construction and validation."""

    def test_basic_construction_with_string_expression(self) -> None:
        """Test basic PersistedColumn with string expression."""
        col = PersistedColumn(
            'total', Integer,
            persisted_expression='price * quantity',
        )
        assert col.name == 'total'
        assert col.is_persisted is True
        assert col.persisted_expression == 'price * quantity'

    def test_construction_with_text_clause(self) -> None:
        """Test PersistedColumn with TextClause expression."""
        expr = text('CONCAT(first_name, last_name)')
        col = PersistedColumn(
            'full_name', String(100),
            persisted_expression=expr,
        )
        assert col.name == 'full_name'
        assert col.is_persisted is True
        assert isinstance(col.persisted_expression, TextClause)

    def test_invalid_expression_type_raises_error(self) -> None:
        """Test that invalid expression type raises ValueError."""
        with pytest.raises(ValueError, match='must be a SQL expression'):
            PersistedColumn(
                'invalid', Integer,
                persisted_expression=123,
            )

    def test_info_contains_persisted_expression(self) -> None:
        """Test that info dict contains persisted_expression."""
        col = PersistedColumn(
            'total', Integer,
            persisted_expression='price * qty',
        )
        assert 'persisted_expression' in col.info
        assert col.info['persisted_expression'] == 'price * qty'

    def test_column_without_persisted_expression(self) -> None:
        """Test that regular column construction still works."""
        # When no persisted_expression is provided, it should behave
        # like a regular column (though is_persisted won't be set)
        col = PersistedColumn('normal', Integer)
        assert col.name == 'normal'
        assert not hasattr(col, 'is_persisted') or not col.is_persisted


class TestPersistedColumnCompiler:
    """Test PersistedColumn DDL compilation."""

    def setup_method(self) -> None:
        """Set up mock engine for DDL compilation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''
        self.metadata = MetaData()

    def test_compile_persisted_column_integer(self) -> None:
        """Test DDL compilation for PersistedColumn with Integer type."""
        table = Table(
            'test_persisted', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('price', Integer),
            Column('quantity', Integer),
            PersistedColumn(
                'total', Integer,
                persisted_expression='price * quantity',
            ),
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AS price * quantity PERSISTED' in self.compiled_ddl
        assert 'INTEGER' in self.compiled_ddl

    def test_compile_persisted_column_string(self) -> None:
        """Test DDL compilation for PersistedColumn with String type."""
        table = Table(
            'test_persisted_str', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('first_name', String(50)),
            Column('last_name', String(50)),
            PersistedColumn(
                'full_name', String(100),
                persisted_expression="CONCAT(first_name, ' ', last_name)",
            ),
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AS CONCAT(first_name' in self.compiled_ddl
        assert 'PERSISTED' in self.compiled_ddl

    def test_compile_persisted_column_numeric(self) -> None:
        """Test DDL compilation for PersistedColumn with Numeric type."""
        table = Table(
            'test_persisted_num', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('price', Numeric(10, 2)),
            Column('tax_rate', Numeric(5, 4)),
            PersistedColumn(
                'price_with_tax', Numeric(12, 2),
                persisted_expression='price * (1 + tax_rate)',
            ),
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'AS price * (1 + tax_rate) PERSISTED' in self.compiled_ddl


class TestPersistedColumnTableIntegration:
    """Test PersistedColumn integration with table creation."""

    def setup_method(self) -> None:
        """Set up mock engine for table DDL generation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''
        self.metadata = MetaData()

    def test_table_with_single_persisted_column(self) -> None:
        """Test table creation with single persisted column."""
        table = Table(
            'orders', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('price', Integer),
            Column('qty', Integer),
            PersistedColumn(
                'total', Integer,
                persisted_expression='price * qty',
            ),
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'CREATE TABLE orders' in self.compiled_ddl
        assert 'total AS price * qty PERSISTED INTEGER' in self.compiled_ddl

    def test_table_with_multiple_persisted_columns(self) -> None:
        """Test table creation with multiple persisted columns."""
        table = Table(
            'products', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('base_price', Integer),
            Column('discount_pct', Integer),
            PersistedColumn(
                'discount_amount', Integer,
                persisted_expression='base_price * discount_pct / 100',
            ),
            PersistedColumn(
                'final_price', Integer,
                persisted_expression='base_price - (base_price * discount_pct / 100)',
            ),
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'CREATE TABLE products' in self.compiled_ddl
        assert 'discount_amount AS base_price * discount_pct / 100' in self.compiled_ddl
        assert 'final_price AS base_price - (base_price * discount_pct / 100)' in self.compiled_ddl  # noqa: E501


class TestPersistedColumnReflection:
    """Test PersistedColumn reflection from actual database tables."""

    def test_reflect_table_with_computed_column(
        self, test_engine: Any, table_name_prefix: str, clean_tables: None,
    ) -> None:
        """Test reflection of table with computed column."""
        table_name = f'{table_name_prefix}test_computed'

        with test_engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        price INT,
                        qty INT,
                        total AS price * qty PERSISTED INT
                    )
                """),
                )

        metadata = MetaData()
        reflected = Table(table_name, metadata, autoload_with=test_engine)

        assert 'id' in reflected.columns
        assert 'price' in reflected.columns
        assert 'qty' in reflected.columns
        assert 'total' in reflected.columns

    def test_computed_column_value_after_insert(
        self, test_engine: Any, table_name_prefix: str, clean_tables: None,
    ) -> None:
        """Test that computed column values are calculated correctly."""
        table_name = f'{table_name_prefix}test_comp_val'

        with test_engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        price INT,
                        qty INT,
                        total AS price * qty PERSISTED INT
                    )
                """),
                )

            with conn.begin():
                conn.execute(
                    text(f"""
                    INSERT INTO {table_name} (id, price, qty)
                    VALUES (1, 10, 5)
                """),
                )

            result = conn.execute(
                text(f'SELECT total FROM {table_name} WHERE id = 1'),
            ).fetchone()

            assert result is not None
            assert result[0] == 50  # 10 * 5

    def test_reflect_string_computed_column(
        self, test_engine: Any, table_name_prefix: str, clean_tables: None,
    ) -> None:
        """Test reflection of string computed column."""
        table_name = f'{table_name_prefix}test_comp_str'

        with test_engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        first_name VARCHAR(50),
                        last_name VARCHAR(50),
                        full_name AS CONCAT(first_name, ' ', last_name)
                            PERSISTED VARCHAR(100)
                    )
                """),
                )

            with conn.begin():
                conn.execute(
                    text(f"""
                    INSERT INTO {table_name} (id, first_name, last_name)
                    VALUES (1, 'John', 'Doe')
                """),
                )

            result = conn.execute(
                text(f'SELECT full_name FROM {table_name} WHERE id = 1'),
            ).fetchone()

            assert result is not None
            assert result[0] == 'John Doe'
