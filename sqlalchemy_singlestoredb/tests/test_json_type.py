"""Tests for SingleStore JSON data type functionality."""
from __future__ import annotations

from typing import Any

from sqlalchemy import Column
from sqlalchemy import create_mock_engine
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text

from sqlalchemy_singlestoredb import JSON


class TestJSONConstruction:
    """Test JSON data type construction."""

    def test_basic_construction(self) -> None:
        """Test basic JSON construction."""
        json_type = JSON()
        assert json_type is not None

    def test_collate_parameter_silently_ignored(self) -> None:
        """Test that collate parameter is silently ignored."""
        # SingleStoreDB JSON doesn't support collate, so it should be ignored
        json_type = JSON(collate='utf8mb4_general_ci')
        assert json_type is not None


class TestJSONResultProcessor:
    """Test JSON result processor functionality."""

    def test_process_string_to_dict(self) -> None:
        """Test processing JSON string to Python dict."""
        json_type = JSON()
        from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
        dialect = SingleStoreDBDialect()
        processor = json_type.result_processor(dialect, None)

        result = processor('{"key": "value"}')
        assert result == {'key': 'value'}

    def test_process_bytes_to_dict(self) -> None:
        """Test processing JSON bytes to Python dict."""
        json_type = JSON()
        from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
        dialect = SingleStoreDBDialect()
        processor = json_type.result_processor(dialect, None)

        result = processor(b'{"key": "value"}')
        assert result == {'key': 'value'}

    def test_process_dict_passthrough(self) -> None:
        """Test that dict values pass through unchanged."""
        json_type = JSON()
        from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
        dialect = SingleStoreDBDialect()
        processor = json_type.result_processor(dialect, None)

        input_dict = {'key': 'value'}
        result = processor(input_dict)
        assert result == input_dict

    def test_process_list_passthrough(self) -> None:
        """Test that list values pass through unchanged."""
        json_type = JSON()
        from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
        dialect = SingleStoreDBDialect()
        processor = json_type.result_processor(dialect, None)

        input_list = [1, 2, 3]
        result = processor(input_list)
        assert result == input_list

    def test_process_none_value(self) -> None:
        """Test processing None value."""
        json_type = JSON()
        from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
        dialect = SingleStoreDBDialect()
        processor = json_type.result_processor(dialect, None)

        result = processor(None)
        assert result is None

    def test_process_nested_structure(self) -> None:
        """Test processing nested JSON structure."""
        json_type = JSON()
        from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
        dialect = SingleStoreDBDialect()
        processor = json_type.result_processor(dialect, None)

        nested_json = '{"user": {"name": "John", "tags": ["admin", "user"]}}'
        result = processor(nested_json)
        assert result == {'user': {'name': 'John', 'tags': ['admin', 'user']}}


class TestJSONTableIntegration:
    """Test JSON integration with table creation."""

    def setup_method(self) -> None:
        """Set up mock engine for table DDL generation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''
        self.metadata = MetaData()

    def test_table_with_json_column(self) -> None:
        """Test table creation with JSON column."""
        table = Table(
            'test_json_table', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', JSON),
            Column('name', String(50)),
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'CREATE TABLE test_json_table' in self.compiled_ddl
        assert 'JSON' in self.compiled_ddl

    def test_table_with_multiple_json_columns(self) -> None:
        """Test table creation with multiple JSON columns."""
        table = Table(
            'test_multi_json', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('metadata_col', JSON),
            Column('config', JSON),
            Column('tags', JSON),
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'CREATE TABLE test_multi_json' in self.compiled_ddl


class TestJSONReflection:
    """Test JSON column reflection from actual database tables."""

    def test_reflect_basic_json_column(
        self, test_engine: Any, table_name_prefix: str, clean_tables: None,
    ) -> None:
        """Test reflection of basic JSON column."""
        table_name = f'{table_name_prefix}test_json_reflect'

        with test_engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        data JSON
                    )
                """),
                )

        metadata = MetaData()
        reflected = Table(table_name, metadata, autoload_with=test_engine)

        assert 'id' in reflected.columns
        assert 'data' in reflected.columns

    def test_json_insert_select_roundtrip(
        self, test_engine: Any, table_name_prefix: str, clean_tables: None,
    ) -> None:
        """Test inserting and selecting JSON data."""
        table_name = f'{table_name_prefix}test_json_roundtrip'

        with test_engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        data JSON
                    )
                """),
                )

            with conn.begin():
                conn.execute(
                    text(f"""
                    INSERT INTO {table_name} (id, data)
                    VALUES (1, '{{"key": "value", "number": 42}}')
                """),
                )

            result = conn.execute(
                text(f'SELECT data FROM {table_name} WHERE id = 1'),
            ).fetchone()

            assert result is not None
            data = result[0]
            assert data is not None
            # Result should be a dict
            if isinstance(data, str):
                import json
                data = json.loads(data)
            assert data['key'] == 'value'
            assert data['number'] == 42

    def test_json_with_nested_structure(
        self, test_engine: Any, table_name_prefix: str, clean_tables: None,
    ) -> None:
        """Test JSON with nested structure."""
        table_name = f'{table_name_prefix}test_json_nested'

        with test_engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        profile JSON
                    )
                """),
                )

            with conn.begin():
                conn.execute(
                    text(f"""
                    INSERT INTO {table_name} (id, profile)
                    VALUES (1, '{{"user": {{"name": "John", "age": 30}}}}')
                """),
                )

            result = conn.execute(
                text(f'SELECT profile FROM {table_name} WHERE id = 1'),
            ).fetchone()

            assert result is not None
            profile = result[0]
            if isinstance(profile, str):
                import json
                profile = json.loads(profile)
            assert profile['user']['name'] == 'John'
            assert profile['user']['age'] == 30
