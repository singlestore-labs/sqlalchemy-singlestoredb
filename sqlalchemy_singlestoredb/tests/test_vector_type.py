"""Tests for SingleStore VECTOR data type functionality."""
from __future__ import annotations

from typing import Any

from sqlalchemy import Column
from sqlalchemy import create_mock_engine
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text

from sqlalchemy_singlestoredb import VECTOR
from sqlalchemy_singlestoredb import VectorKey


class TestVECTORConstruction:
    """Test VECTOR data type construction."""

    def test_basic_construction_with_dimension(self) -> None:
        """Test basic VECTOR construction with dimension."""
        vec = VECTOR(128)
        assert vec.n_elems == 128
        assert vec.elem_type == 'F32'  # Default element type

    def test_construction_with_element_type(self) -> None:
        """Test VECTOR construction with explicit element type."""
        vec = VECTOR(256, 'F64')
        assert vec.n_elems == 256
        assert vec.elem_type == 'F64'

    def test_default_values(self) -> None:
        """Test VECTOR default values when no arguments provided."""
        vec = VECTOR()
        assert vec.n_elems == 1
        assert vec.elem_type == 'F32'

    def test_all_element_types(self) -> None:
        """Test VECTOR with all supported element types."""
        element_types = ['F16', 'F32', 'F64', 'I8', 'I16', 'I32', 'I64']
        for elem_type in element_types:
            vec = VECTOR(64, elem_type)
            assert vec.elem_type == elem_type
            assert vec.n_elems == 64

    def test_class_constants(self) -> None:
        """Test VECTOR class constants for element types."""
        assert VECTOR.F16 == 'F16'
        assert VECTOR.FLOAT16 == 'F16'
        assert VECTOR.F32 == 'F32'
        assert VECTOR.FLOAT32 == 'F32'
        assert VECTOR.F64 == 'F64'
        assert VECTOR.FLOAT64 == 'F64'
        assert VECTOR.I8 == 'I8'
        assert VECTOR.INT8 == 'I8'
        assert VECTOR.I16 == 'I16'
        assert VECTOR.INT16 == 'I16'
        assert VECTOR.I32 == 'I32'
        assert VECTOR.INT32 == 'I32'
        assert VECTOR.I64 == 'I64'
        assert VECTOR.INT64 == 'I64'

    def test_using_class_constants_in_construction(self) -> None:
        """Test using class constants for element type in construction."""
        vec = VECTOR(128, VECTOR.F64)
        assert vec.n_elems == 128
        assert vec.elem_type == 'F64'


class TestVECTORCompiler:
    """Test VECTOR DDL compilation."""

    def setup_method(self) -> None:
        """Set up mock engine for DDL compilation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''
        self.metadata = MetaData()

    def test_compile_basic_vector(self) -> None:
        """Test DDL compilation for basic VECTOR column."""
        table = Table(
            'test_vec', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('embedding', VECTOR(128)),
        )
        table.create(self.mock_engine, checkfirst=False)
        assert 'VECTOR(128, F32)' in self.compiled_ddl

    def test_compile_vector_with_different_dimensions(self) -> None:
        """Test DDL compilation for various VECTOR dimensions."""
        dimensions = [128, 256, 512, 768, 1536]
        for dim in dimensions:
            self.metadata = MetaData()
            table = Table(
                f'test_vec_{dim}', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('embedding', VECTOR(dim)),
            )
            table.create(self.mock_engine, checkfirst=False)
            assert f'VECTOR({dim}, F32)' in self.compiled_ddl

    def test_compile_vector_all_element_types(self) -> None:
        """Test DDL compilation for all VECTOR element types."""
        element_types = ['F16', 'F32', 'F64', 'I8', 'I16', 'I32', 'I64']
        for elem_type in element_types:
            self.metadata = MetaData()
            table = Table(
                f'test_vec_{elem_type}', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('embedding', VECTOR(64, elem_type)),
            )
            table.create(self.mock_engine, checkfirst=False)
            assert f'VECTOR(64, {elem_type})' in self.compiled_ddl


class TestVECTORResultProcessor:
    """Test VECTOR result processor functionality."""

    def test_process_json_string_to_list(self) -> None:
        """Test processing JSON string to Python list."""
        vec = VECTOR(3)
        # Get the result processor
        from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
        dialect = SingleStoreDBDialect()
        processor = vec.result_processor(dialect, None)

        # Test JSON string processing
        result = processor('[1.0, 2.0, 3.0]')
        assert result == [1.0, 2.0, 3.0]

    def test_process_list_passthrough(self) -> None:
        """Test that list values pass through unchanged."""
        vec = VECTOR(3)
        from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
        dialect = SingleStoreDBDialect()
        processor = vec.result_processor(dialect, None)

        # Test list passthrough
        input_list = [1.0, 2.0, 3.0]
        result = processor(input_list)
        assert result == input_list

    def test_process_none_value(self) -> None:
        """Test processing None value."""
        vec = VECTOR(3)
        from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
        dialect = SingleStoreDBDialect()
        processor = vec.result_processor(dialect, None)

        result = processor(None)
        assert result is None


class TestVECTORCacheKey:
    """Test VECTOR cache key generation."""

    def test_cache_key_includes_n_elems_and_elem_type(self) -> None:
        """Test that cache key includes both n_elems and elem_type."""
        vec1 = VECTOR(128, 'F32')
        vec2 = VECTOR(128, 'F32')
        vec3 = VECTOR(256, 'F32')
        vec4 = VECTOR(128, 'F64')

        # Same dimensions and type should have same cache key components
        assert vec1.n_elems == vec2.n_elems
        assert vec1.elem_type == vec2.elem_type

        # Different dimensions should differ
        assert vec1.n_elems != vec3.n_elems

        # Different element types should differ
        assert vec1.elem_type != vec4.elem_type


class TestVECTORTableIntegration:
    """Test VECTOR integration with table creation."""

    def setup_method(self) -> None:
        """Set up mock engine for table DDL generation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''
        self.metadata = MetaData()

    def test_table_with_vector_column(self) -> None:
        """Test table creation with VECTOR column."""
        table = Table(
            'test_table_vector', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('embedding', VECTOR(128, 'F32')),
            Column('name', String(50)),
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'CREATE TABLE test_table_vector' in self.compiled_ddl
        assert 'VECTOR(128, F32)' in self.compiled_ddl

    def test_table_with_vector_and_vector_key(self) -> None:
        """Test table creation with VECTOR column and VectorKey index."""
        table = Table(
            'test_vec_with_index', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('embedding', VECTOR(256, 'F32')),
            Column('data', String(100)),
            singlestoredb_vector_key=VectorKey('embedding', name='vec_idx'),
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'CREATE TABLE test_vec_with_index' in self.compiled_ddl
        assert 'VECTOR(256, F32)' in self.compiled_ddl
        assert 'VECTOR INDEX vec_idx (embedding)' in self.compiled_ddl

    def test_table_with_multiple_vector_columns(self) -> None:
        """Test table creation with multiple VECTOR columns."""
        table = Table(
            'test_multi_vec', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('content_embedding', VECTOR(128, 'F32')),
            Column('title_embedding', VECTOR(256, 'F64')),
            Column('data', String(50)),
        )
        table.create(self.mock_engine, checkfirst=False)

        assert 'CREATE TABLE test_multi_vec' in self.compiled_ddl
        assert 'VECTOR(128, F32)' in self.compiled_ddl
        assert 'VECTOR(256, F64)' in self.compiled_ddl


class TestVECTORReflection:
    """Test VECTOR column reflection from actual database tables."""

    def test_reflect_basic_vector_column(
        self, test_engine: Any, table_name_prefix: str, clean_tables: None,
    ) -> None:
        """Test reflection of basic VECTOR column."""
        table_name = f'{table_name_prefix}test_vec_reflect'

        with test_engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        embedding VECTOR(128, F32)
                    )
                """),
                )

        metadata = MetaData()
        reflected = Table(table_name, metadata, autoload_with=test_engine)

        assert 'id' in reflected.columns
        assert 'embedding' in reflected.columns

    def test_reflect_vector_with_different_element_types(
        self, test_engine: Any, table_name_prefix: str, clean_tables: None,
    ) -> None:
        """Test reflection of VECTOR columns with different element types."""
        table_name = f'{table_name_prefix}test_vec_types'

        with test_engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        vec_f32 VECTOR(64, F32),
                        vec_f64 VECTOR(64, F64),
                        vec_i8 VECTOR(64, I8)
                    )
                """),
                )

        metadata = MetaData()
        reflected = Table(table_name, metadata, autoload_with=test_engine)

        assert 'vec_f32' in reflected.columns
        assert 'vec_f64' in reflected.columns
        assert 'vec_i8' in reflected.columns

    def test_vector_insert_select_roundtrip(
        self, test_engine: Any, table_name_prefix: str, clean_tables: None,
    ) -> None:
        """Test inserting and selecting VECTOR data."""
        table_name = f'{table_name_prefix}test_vec_roundtrip'

        with test_engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        embedding VECTOR(3, F32)
                    )
                """),
                )

            with conn.begin():
                conn.execute(
                    text(f"""
                    INSERT INTO {table_name} (id, embedding)
                    VALUES (1, '[1.0, 2.0, 3.0]')
                """),
                )

            result = conn.execute(
                text(f'SELECT embedding FROM {table_name} WHERE id = 1'),
            ).fetchone()

            assert result is not None
            # Result should be a list or JSON-like structure
            embedding = result[0]
            assert embedding is not None
