#!/usr/bin/env python
"""Tests for SingleStore table type specifications (RowStore and ColumnStore)."""
from __future__ import annotations

import unittest

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
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


if __name__ == '__main__':
    unittest.main()
