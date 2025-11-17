#!/usr/bin/env python
"""Tests for SingleStore table type specifications (RowStore and ColumnStore)."""
from __future__ import annotations

import unittest
from typing import Any

import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.schema import CreateTable

from sqlalchemy_singlestoredb import ColumnStore
from sqlalchemy_singlestoredb import RowStore
from sqlalchemy_singlestoredb import ShardKey
from sqlalchemy_singlestoredb import SortKey
from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
from sqlalchemy_singlestoredb.ddlelement import TableType


class TestTableTypeValidation(unittest.TestCase):
    """Test table type class validation and construction."""

    def test_rowstore_basic_construction(self) -> None:
        """Test basic RowStore construction."""
        rs = RowStore()
        self.assertFalse(rs.reference)
        self.assertFalse(rs.temporary)
        self.assertFalse(rs.global_temporary)
        self.assertEqual(repr(rs), 'RowStore()')

    def test_rowstore_with_modifiers(self) -> None:
        """Test RowStore construction with modifiers."""
        # Test each modifier individually
        rs_ref = RowStore(reference=True)
        self.assertTrue(rs_ref.reference)
        self.assertFalse(rs_ref.temporary)
        self.assertFalse(rs_ref.global_temporary)
        self.assertEqual(repr(rs_ref), 'RowStore(reference=True)')

        rs_temp = RowStore(temporary=True)
        self.assertFalse(rs_temp.reference)
        self.assertTrue(rs_temp.temporary)
        self.assertFalse(rs_temp.global_temporary)
        self.assertEqual(repr(rs_temp), 'RowStore(temporary=True)')

        rs_global = RowStore(global_temporary=True)
        self.assertFalse(rs_global.reference)
        self.assertFalse(rs_global.temporary)
        self.assertTrue(rs_global.global_temporary)
        self.assertEqual(repr(rs_global), 'RowStore(global_temporary=True)')

    def test_columnstore_basic_construction(self) -> None:
        """Test basic ColumnStore construction."""
        cs = ColumnStore()
        self.assertFalse(cs.reference)
        self.assertFalse(cs.temporary)
        self.assertFalse(cs.global_temporary)
        self.assertEqual(repr(cs), 'ColumnStore()')

    def test_columnstore_with_modifiers(self) -> None:
        """Test ColumnStore construction with modifiers."""
        # Test each supported modifier individually
        cs_ref = ColumnStore(reference=True)
        self.assertTrue(cs_ref.reference)
        self.assertFalse(cs_ref.temporary)
        self.assertFalse(cs_ref.global_temporary)
        self.assertEqual(repr(cs_ref), 'ColumnStore(reference=True)')

        cs_temp = ColumnStore(temporary=True)
        self.assertFalse(cs_temp.reference)
        self.assertTrue(cs_temp.temporary)
        self.assertFalse(cs_temp.global_temporary)
        self.assertEqual(repr(cs_temp), 'ColumnStore(temporary=True)')

    def test_mutually_exclusive_modifiers_error(self) -> None:
        """Test that multiple modifiers raise ValueError."""
        # Test RowStore with multiple modifiers
        with self.assertRaises(ValueError) as cm:
            RowStore(reference=True, temporary=True)
        self.assertIn(
            'Only one of reference, temporary, or global_temporary can be True',
            str(cm.exception),
        )

        with self.assertRaises(ValueError) as cm:
            RowStore(reference=True, global_temporary=True)
        self.assertIn(
            'Only one of reference, temporary, or global_temporary can be True',
            str(cm.exception),
        )

        with self.assertRaises(ValueError) as cm:
            RowStore(temporary=True, global_temporary=True)
        self.assertIn(
            'Only one of reference, temporary, or global_temporary can be True',
            str(cm.exception),
        )

        with self.assertRaises(ValueError) as cm:
            RowStore(reference=True, temporary=True, global_temporary=True)
        self.assertIn(
            'Only one of reference, temporary, or global_temporary can be True',
            str(cm.exception),
        )

        # Test ColumnStore with multiple modifiers
        with self.assertRaises(ValueError) as cm:
            ColumnStore(reference=True, temporary=True)
        self.assertIn(
            'Only one of reference, temporary, or global_temporary can be True',
            str(cm.exception),
        )

    def test_columnstore_no_global_temporary(self) -> None:
        """Test that ColumnStore doesn't support global_temporary parameter."""
        # ColumnStore constructor should not have global_temporary parameter
        import inspect
        sig = inspect.signature(ColumnStore.__init__)
        self.assertNotIn('global_temporary', sig.parameters)

    def test_isinstance_tabletype(self) -> None:
        """Test that both classes are instances of TableType."""
        rs = RowStore()
        cs = ColumnStore()
        self.assertIsInstance(rs, TableType)
        self.assertIsInstance(cs, TableType)


class TestTableTypeCompilation(unittest.TestCase):
    """Test table type SQL compilation."""

    def setUp(self) -> None:
        self.dialect = SingleStoreDBDialect()
        self.metadata = MetaData()

    def _compile_table(self, table: Table) -> str:
        """Compile a table to SQL."""
        create_ddl = CreateTable(table)
        compiled = create_ddl.compile(dialect=self.dialect)
        return str(compiled)

    def test_default_table_no_type(self) -> None:
        """Test default table without any type specification."""
        table = Table(
            'default_table',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
        )

        sql = self._compile_table(table)
        self.assertIn('CREATE TABLE default_table', sql)
        # Should not contain any table type prefixes
        self.assertNotIn('ROWSTORE', sql)
        self.assertNotIn('COLUMNSTORE', sql)
        self.assertNotIn('REFERENCE', sql)
        self.assertNotIn('TEMPORARY', sql)

    def test_rowstore_basic(self) -> None:
        """Test basic RowStore table compilation."""
        table = Table(
            'rowstore_table',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            singlestoredb_table_type=RowStore(),
        )

        sql = self._compile_table(table)
        self.assertIn('CREATE ROWSTORE TABLE rowstore_table', sql)

    def test_rowstore_temporary(self) -> None:
        """Test RowStore temporary table compilation."""
        table = Table(
            'temp_rowstore',
            self.metadata,
            Column('id', Integer, primary_key=True),
            singlestoredb_table_type=RowStore(temporary=True),
        )

        sql = self._compile_table(table)
        self.assertIn('CREATE ROWSTORE TEMPORARY TABLE temp_rowstore', sql)

    def test_rowstore_global_temporary(self) -> None:
        """Test RowStore global temporary table compilation."""
        table = Table(
            'global_temp_rowstore',
            self.metadata,
            Column('id', Integer, primary_key=True),
            singlestoredb_table_type=RowStore(global_temporary=True),
        )

        sql = self._compile_table(table)
        self.assertIn(
            'CREATE ROWSTORE GLOBAL TEMPORARY TABLE global_temp_rowstore',
            sql,
        )

    def test_rowstore_reference(self) -> None:
        """Test RowStore reference table compilation."""
        table = Table(
            'ref_rowstore',
            self.metadata,
            Column('id', Integer, primary_key=True),
            singlestoredb_table_type=RowStore(reference=True),
        )

        sql = self._compile_table(table)
        self.assertIn('CREATE ROWSTORE REFERENCE TABLE ref_rowstore', sql)

    def test_columnstore_basic(self) -> None:
        """Test basic ColumnStore table compilation."""
        table = Table(
            'columnstore_table',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            singlestoredb_table_type=ColumnStore(),
        )

        sql = self._compile_table(table)
        # ColumnStore should not add any prefix (it's the default)
        self.assertIn('CREATE TABLE columnstore_table', sql)
        self.assertNotIn('COLUMNSTORE', sql)

    def test_columnstore_temporary(self) -> None:
        """Test ColumnStore temporary table compilation."""
        table = Table(
            'temp_columnstore',
            self.metadata,
            Column('id', Integer, primary_key=True),
            singlestoredb_table_type=ColumnStore(temporary=True),
        )

        sql = self._compile_table(table)
        self.assertIn('CREATE TEMPORARY TABLE temp_columnstore', sql)
        # Should not contain ROWSTORE or COLUMNSTORE
        self.assertNotIn('ROWSTORE', sql)
        self.assertNotIn('COLUMNSTORE', sql)

    def test_columnstore_reference(self) -> None:
        """Test ColumnStore reference table compilation."""
        table = Table(
            'ref_columnstore',
            self.metadata,
            Column('id', Integer, primary_key=True),
            singlestoredb_table_type=ColumnStore(reference=True),
        )

        sql = self._compile_table(table)
        self.assertIn('CREATE REFERENCE TABLE ref_columnstore', sql)
        # Should not contain ROWSTORE or COLUMNSTORE
        self.assertNotIn('ROWSTORE', sql)
        self.assertNotIn('COLUMNSTORE', sql)

    def test_invalid_table_type_error(self) -> None:
        """Test that invalid table type raises TypeError."""
        table = Table(
            'invalid_table',
            self.metadata,
            Column('id', Integer, primary_key=True),
            singlestoredb_table_type='invalid',
        )

        with self.assertRaises(TypeError) as cm:
            self._compile_table(table)
        self.assertIn(
            'singlestoredb_table_type must be a RowStore or ColumnStore instance',
            str(cm.exception),
        )

    def test_table_type_with_existing_prefixes(self) -> None:
        """Test that table type prefixes are added to existing prefixes."""
        # Create table with existing prefixes
        table = Table(
            'test_table',
            self.metadata,
            Column('id', Integer, primary_key=True),
            prefixes=['IF', 'NOT', 'EXISTS'],
            singlestoredb_table_type=RowStore(temporary=True),
        )

        sql = self._compile_table(table)
        # Should contain both our prefixes and existing ones
        self.assertIn(
            'CREATE ROWSTORE TEMPORARY IF NOT EXISTS TABLE test_table',
            sql,
        )

    def test_prefix_restoration(self) -> None:
        """Test that original prefixes are restored after compilation."""
        original_prefixes = ['IF', 'NOT', 'EXISTS']
        table = Table(
            'test_table',
            self.metadata,
            Column('id', Integer, primary_key=True),
            prefixes=original_prefixes,
            singlestoredb_table_type=RowStore(),
        )

        # Compile the table
        self._compile_table(table)

        # Check that original prefixes are restored
        self.assertEqual(list(table._prefixes), original_prefixes)


class TestTableTypeIntegration(unittest.TestCase):
    """Test table type integration with other SingleStore features."""

    def setUp(self) -> None:
        self.dialect = SingleStoreDBDialect()
        self.metadata = MetaData()

    def _compile_table(self, table: Table) -> str:
        """Compile a table to SQL."""
        create_ddl = CreateTable(table)
        compiled = create_ddl.compile(dialect=self.dialect)
        return str(compiled)

    def test_table_type_with_shard_key(self) -> None:
        """Test table type with shard key."""
        table = Table(
            'test_table',
            self.metadata,
            Column('user_id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_table_type=RowStore(),
            singlestoredb_shard_key=ShardKey('user_id'),
        )

        sql = self._compile_table(table)
        self.assertIn('CREATE ROWSTORE TABLE test_table', sql)
        self.assertIn('SHARD KEY (user_id)', sql)

    def test_table_type_with_sort_key(self) -> None:
        """Test table type with sort key."""
        table = Table(
            'test_table',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('created_at', String(50)),
            singlestoredb_table_type=ColumnStore(temporary=True),
            singlestoredb_sort_key=SortKey('created_at'),
        )

        sql = self._compile_table(table)
        self.assertIn('CREATE TEMPORARY TABLE test_table', sql)
        self.assertIn('SORT KEY (created_at)', sql)


class TestTableTypeReflection:
    """Test TableType reflection from actual database tables."""

    def test_reflect_rowstore_table(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of ROWSTORE table."""
        table_name = 'test_rowstore_reflection'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create ROWSTORE table
                conn.execute(
                    text(f"""
                    CREATE ROWSTORE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        name VARCHAR(100),
                        value DECIMAL(10,2),
                        created_at TIMESTAMP
                    )
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
            assert 'id' in reflected_table.columns
            assert 'name' in reflected_table.columns
            assert 'value' in reflected_table.columns
            assert 'created_at' in reflected_table.columns

    def test_reflect_columnstore_table(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of COLUMNSTORE table."""
        table_name = 'test_columnstore_reflection'

        with test_engine.connect() as conn:
            try:
                with conn.begin():
                    # Create COLUMNSTORE table (explicit syntax)
                    conn.execute(
                        text(f"""
                        CREATE COLUMNSTORE TABLE {table_name} (
                            user_id INT,
                            product_id INT,
                            quantity INT,
                            price DECIMAL(10,2),
                            order_date DATE,
                            PRIMARY KEY (user_id, product_id)
                        )
                    """),
                    )
            except Exception as e:
                if 'syntax' in str(e).lower() or 'columnstore' in str(e).lower():
                    # Try default table (which is columnstore by default)
                    with conn.begin():
                        conn.execute(
                            text(f"""
                            CREATE TABLE {table_name} (
                                user_id INT,
                                product_id INT,
                                quantity INT,
                                price DECIMAL(10,2),
                                order_date DATE,
                                PRIMARY KEY (user_id, product_id)
                            )
                        """),
                        )
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
                'user_id', 'product_id', 'quantity', 'price', 'order_date',
            }

            # Should have composite primary key
            pk_columns = {col.name for col in reflected_table.primary_key.columns}
            assert pk_columns == {'user_id', 'product_id'}

    def test_reflect_temporary_rowstore_table(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of temporary ROWSTORE table."""
        table_name = 'test_temp_rowstore_reflection'

        with test_engine.connect() as conn:
            with conn.begin():
                try:
                    # Create temporary ROWSTORE table
                    conn.execute(
                        text(f"""
                        CREATE TEMPORARY ROWSTORE TABLE {table_name} (
                            session_id VARCHAR(64) PRIMARY KEY,
                            data JSON,
                            expires_at TIMESTAMP
                        )
                    """),
                    )

                except Exception as e:
                    if (
                        (
                            'temporary' in str(e).lower() and
                            'not supported' in str(e).lower()
                        ) or
                        ('syntax' in str(e).lower() and 'rowstore' in str(e).lower())
                    ):
                        # TEMPORARY ROWSTORE syntax not supported - try regular TEMPORARY
                        try:
                            conn.execute(
                                text(f"""
                                CREATE TEMPORARY TABLE {table_name} (
                                    session_id VARCHAR(64) PRIMARY KEY,
                                    data JSON,
                                    expires_at TIMESTAMP
                                )
                            """),
                            )
                        except Exception as e2:
                            if (
                                'temporary' in str(e2).lower() and
                                'not supported' in str(e2).lower()
                            ):
                                pytest.skip(f'Temporary tables not supported: {e2}')
                            else:
                                raise
                    else:
                        raise

                # Show the generated CREATE TABLE
                result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
                create_sql = result.fetchone()[1]
                print(f'\nGenerated CREATE TABLE for {table_name}:')
                print(create_sql)

                # Verify reflection works (must be in same transaction for temp tables)
                metadata = MetaData()
                reflected_table = Table(table_name, metadata, autoload_with=conn)

                # Should have expected columns
                assert len(reflected_table.columns) == 3
                assert 'session_id' in reflected_table.columns
                assert 'data' in reflected_table.columns
                assert 'expires_at' in reflected_table.columns

    def test_reflect_reference_table(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of REFERENCE table."""
        table_name = 'test_reference_reflection'

        with test_engine.connect() as conn:
            try:
                with conn.begin():
                    # Create REFERENCE table
                    conn.execute(
                        text(f"""
                        CREATE REFERENCE TABLE {table_name} (
                            code VARCHAR(10) PRIMARY KEY,
                            description VARCHAR(100),
                            category VARCHAR(50)
                        )
                    """),
                    )

            except Exception as e:
                if 'reference' in str(e).lower() and 'not supported' in str(e).lower():
                    pytest.skip(f'Reference tables not supported: {e}')
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
            assert len(reflected_table.columns) == 3
            assert 'code' in reflected_table.columns
            assert 'description' in reflected_table.columns
            assert 'category' in reflected_table.columns

    def test_reflect_table_with_mixed_features(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of table with table type and other features."""
        table_name = 'test_mixed_features_reflection'

        with test_engine.connect() as conn:
            try:
                with conn.begin():
                    # Create ROWSTORE table with keys and indexes
                    conn.execute(
                        text(f"""
                        CREATE ROWSTORE TABLE {table_name} (
                            user_id INT,
                            doc_id INT,
                            title VARCHAR(200),
                            content TEXT,
                            created_at TIMESTAMP,
                            PRIMARY KEY (user_id, doc_id),
                            SHARD KEY (user_id),
                            SORT KEY (created_at),
                            KEY idx_title (title)
                        )
                    """),
                    )
            except Exception as e:
                if 'rowstore' in str(e).lower() or 'clustered columnar' in str(e).lower():
                    # Fallback to regular table
                    with conn.begin():
                        conn.execute(
                            text(f"""
                            CREATE TABLE {table_name} (
                                user_id INT,
                                doc_id INT,
                                title VARCHAR(200),
                                content TEXT,
                                created_at TIMESTAMP,
                                PRIMARY KEY (user_id, doc_id),
                                SHARD KEY (user_id),
                                SORT KEY (created_at),
                                KEY idx_title (title)
                            )
                        """),
                        )
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
            assert set(col.name for col in reflected_table.primary_key.columns) == {
                'user_id', 'doc_id',
            }

            # Should have regular indexes
            indexes = reflected_table.indexes
            index_names = {idx.name for idx in indexes}
            print(f'\nReflected indexes: {index_names}')
            assert 'idx_title' in index_names


if __name__ == '__main__':
    unittest.main()
