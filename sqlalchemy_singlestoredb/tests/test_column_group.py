"""Test ColumnGroup DDL element functionality."""
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

from sqlalchemy_singlestoredb import ColumnGroup
from sqlalchemy_singlestoredb.ddlelement import compile_column_group


class TestColumnGroupConstruction:
    """Test ColumnGroup DDL element construction and validation."""

    def test_column_group_basic_construction(self) -> None:
        """Test basic ColumnGroup construction with name parameter."""
        column_group = ColumnGroup(name='cg_all_columns')
        assert column_group.name == 'cg_all_columns'

    def test_column_group_optional_name(self) -> None:
        """Test ColumnGroup construction without name parameter (optional)."""
        column_group = ColumnGroup()
        assert column_group.name is None

    def test_column_group_repr(self) -> None:
        """Test ColumnGroup string representation."""
        column_group = ColumnGroup(name='cg_test')
        expected = "ColumnGroup(name='cg_test')"
        assert repr(column_group) == expected

    def test_column_group_repr_without_name(self) -> None:
        """Test ColumnGroup string representation without name."""
        column_group = ColumnGroup()
        expected = 'ColumnGroup()'
        assert repr(column_group) == expected

    def test_column_group_empty_name_error(self) -> None:
        """Test ColumnGroup construction with empty name raises ValueError."""
        with pytest.raises(ValueError, match='Column group name cannot be empty string'):
            ColumnGroup(name='')

    def test_column_group_keyword_only_name(self) -> None:
        """Test that name parameter is keyword-only."""
        # This should work (keyword argument)
        column_group = ColumnGroup(name='valid_name')
        assert column_group.name == 'valid_name'

        # This should raise TypeError (positional argument)
        with pytest.raises(TypeError):
            ColumnGroup('invalid_positional_name')  # type: ignore


class TestColumnGroupCompiler:
    """Test ColumnGroup DDL compilation."""

    def test_compile_basic_column_group(self) -> None:
        """Test compiling basic column group to SQL."""
        column_group = ColumnGroup(name='cg_all_columns')

        # Create a mock compiler
        class MockCompiler:
            pass

        compiler = MockCompiler()
        result = compile_column_group(column_group, compiler)
        assert result == 'COLUMN GROUP cg_all_columns (*)'

    def test_compile_column_group_with_special_chars(self) -> None:
        """Test compiling column group with special characters in name."""
        column_group = ColumnGroup(name='cg-test_name.123')

        class MockCompiler:
            pass

        compiler = MockCompiler()
        result = compile_column_group(column_group, compiler)
        # The _escape_column_name function should handle escaping if needed
        assert 'cg-test_name.123' in result
        assert result.startswith('COLUMN GROUP')
        assert result.endswith('(*)')

    def test_compile_column_group_without_name(self) -> None:
        """Test compiling column group without name (auto-generated)."""
        column_group = ColumnGroup()

        class MockCompiler:
            pass

        compiler = MockCompiler()
        result = compile_column_group(column_group, compiler)
        assert result == 'COLUMN GROUP (*)'


class TestColumnGroupTableIntegration:
    """Test ColumnGroup integration with SQLAlchemy Table."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        def capture_sql(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', capture_sql)
        self.compiled_ddl = ''
        self.metadata = MetaData()

    def test_table_with_basic_column_group(self) -> None:
        """Test Table with basic column group."""
        self.setUp()

        table = Table(
            'test_table', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            Column('metadata_field', String(50)),
            singlestoredb_column_group=ColumnGroup(name='cg_all_columns'),
        )

        # Verify dialect options are set correctly
        assert 'singlestoredb' in table.dialect_options
        assert 'column_group' in table.dialect_options['singlestoredb']
        column_group = table.dialect_options['singlestoredb']['column_group']
        assert isinstance(column_group, ColumnGroup)
        assert column_group.name == 'cg_all_columns'

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'COLUMN GROUP cg_all_columns (*)' in self.compiled_ddl
        assert 'CREATE TABLE test_table' in self.compiled_ddl

    def test_table_with_column_group_without_name(self) -> None:
        """Test Table with column group without explicit name (auto-generated)."""
        self.setUp()

        table = Table(
            'auto_named_table', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_column_group=ColumnGroup(),
        )

        # Verify dialect options are set correctly
        assert 'singlestoredb' in table.dialect_options
        assert 'column_group' in table.dialect_options['singlestoredb']
        column_group = table.dialect_options['singlestoredb']['column_group']
        assert isinstance(column_group, ColumnGroup)
        assert column_group.name is None

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'COLUMN GROUP (*)' in self.compiled_ddl
        assert 'CREATE TABLE auto_named_table' in self.compiled_ddl

    def test_table_column_group_preserves_info(self) -> None:
        """Test that Table constructor preserves existing info dictionary."""
        self.setUp()

        table = Table(
            'test_info_table', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
            singlestoredb_column_group=ColumnGroup(name='cg_info_test'),
            info={'custom_metadata': 'test_value'},
        )

        # Test dialect options
        assert 'singlestoredb' in table.dialect_options
        column_group = table.dialect_options['singlestoredb']['column_group']
        assert column_group.name == 'cg_info_test'

        # Test info preservation
        assert table.info.get('custom_metadata') == 'test_value'

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'COLUMN GROUP cg_info_test (*)' in self.compiled_ddl

    def test_table_without_column_group(self) -> None:
        """Test that Table constructor works normally without column group parameters."""
        self.setUp()

        table = Table(
            'normal_table', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),
        )

        # No SingleStore dialect options should be set
        singlestore_opts = table.dialect_options.get('singlestoredb', {})
        assert 'column_group' not in singlestore_opts

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'COLUMN GROUP' not in self.compiled_ddl
        assert 'CREATE TABLE normal_table' in self.compiled_ddl

    def test_table_column_group_with_other_ddl_elements(self) -> None:
        """Test Table with column group combined with other DDL elements."""
        self.setUp()

        from sqlalchemy_singlestoredb import ShardKey, SortKey

        table = Table(
            'complex_table', self.metadata,
            Column('user_id', Integer, primary_key=True),
            Column('timestamp', String(50)),
            Column('data', String(200)),
            singlestoredb_shard_key=ShardKey('user_id'),
            singlestoredb_sort_key=SortKey('timestamp'),
            singlestoredb_column_group=ColumnGroup(name='cg_complex'),
        )

        # Verify all dialect options are set
        singlestore_opts = table.dialect_options['singlestoredb']
        assert 'shard_key' in singlestore_opts
        assert 'sort_key' in singlestore_opts
        assert 'column_group' in singlestore_opts

        column_group = singlestore_opts['column_group']
        assert column_group.name == 'cg_complex'

        # Test DDL generation includes all elements
        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY (user_id)' in self.compiled_ddl
        assert 'SORT KEY (timestamp)' in self.compiled_ddl
        assert 'COLUMN GROUP cg_complex (*)' in self.compiled_ddl


class TestColumnGroupReflection:
    """Test ColumnGroup reflection from actual database tables."""

    def test_reflect_basic_column_group(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of basic ColumnGroup."""
        table_name = 'test_column_group_reflection'

        with test_engine.connect() as conn:
            try:
                with conn.begin():
                    # Create table with ColumnGroup (try different syntax)
                    conn.execute(
                        text(f"""
                        CREATE TABLE {table_name} (
                            id INT PRIMARY KEY,
                            data1 VARCHAR(100),
                            data2 VARCHAR(100),
                            data3 INT,
                            COLUMN GROUP cg_all_data (*)
                        )
                    """),
                    )
            except Exception as e:
                if 'syntax' in str(e).lower() or 'not supported' in str(e).lower():
                    pytest.skip(f'ColumnGroup syntax not supported: {e}')
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
            assert len(reflected_table.columns) == 4
            assert 'id' in reflected_table.columns
            assert 'data1' in reflected_table.columns
            assert 'data2' in reflected_table.columns
            assert 'data3' in reflected_table.columns

    def test_reflect_named_column_group(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of named ColumnGroup."""
        table_name = 'test_named_column_group_reflection'

        with test_engine.connect() as conn:
            try:
                with conn.begin():
                    # Create table with named ColumnGroup
                    conn.execute(
                        text(f"""
                        CREATE TABLE {table_name} (
                            user_id INT,
                            name VARCHAR(100),
                            email VARCHAR(200),
                            created_at TIMESTAMP,
                            PRIMARY KEY (user_id),
                            COLUMN GROUP cg_user_info (*)
                        )
                    """),
                    )
            except Exception as e:
                if 'syntax' in str(e).lower() or 'not supported' in str(e).lower():
                    pytest.skip(f'ColumnGroup syntax not supported: {e}')
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
            assert len(reflected_table.columns) == 4
            assert set(col.name for col in reflected_table.columns) == {
                'user_id', 'name', 'email', 'created_at',
            }

    def test_reflect_column_group_with_other_keys(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of ColumnGroup combined with other keys."""
        table_name = 'test_column_group_complex_reflection'

        with test_engine.connect() as conn:
            try:
                with conn.begin():
                    # Create complex table with ColumnGroup and other keys
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
                            KEY idx_title (title),
                            COLUMN GROUP cg_all_data (*)
                        )
                    """),
                    )
            except Exception as e:
                if 'syntax' in str(e).lower() or 'not supported' in str(e).lower():
                    pytest.skip(f'ColumnGroup syntax not supported: {e}')
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
    pytest.main([__file__])
