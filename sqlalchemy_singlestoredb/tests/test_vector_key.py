#!/usr/bin/env python3
"""
Comprehensive tests for VectorKey DDL element and integration.

These tests cover VectorKey creation, compilation, table integration,
and reflection functionality for SingleStore VECTOR INDEX support.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import Column
from sqlalchemy import create_mock_engine
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import text
from sqlalchemy.orm import declarative_base

from sqlalchemy_singlestoredb import Table
from sqlalchemy_singlestoredb import VECTOR
from sqlalchemy_singlestoredb import VectorKey


class TestVectorKeyConstruction:
    """Test VectorKey DDL element construction and representation."""

    def test_basic_vector_key(self) -> None:
        """Test basic vector key with single column."""
        vector_key = VectorKey('vec_idx', 'embedding')
        assert vector_key.name == 'vec_idx'
        assert vector_key.columns == ('embedding',)
        assert vector_key.index_options is None
        assert repr(vector_key) == "VectorKey('vec_idx', 'embedding')"

    def test_vector_key_with_options(self) -> None:
        """Test vector key with index options."""
        vector_key = VectorKey(
            'vec_idx', 'embedding',
            index_options='{"metric_type":"EUCLIDEAN_DISTANCE"}',
        )
        assert vector_key.name == 'vec_idx'
        assert vector_key.columns == ('embedding',)
        assert vector_key.index_options == '{"metric_type":"EUCLIDEAN_DISTANCE"}'
        assert repr(vector_key) == (
            "VectorKey('vec_idx', 'embedding', "
            "index_options='{\"metric_type\":\"EUCLIDEAN_DISTANCE\"}')"
        )

    def test_multi_column_vector_key(self) -> None:
        """Test vector key with multiple columns (if supported)."""
        vector_key = VectorKey('multi_vec_idx', 'embedding1', 'embedding2')
        assert vector_key.name == 'multi_vec_idx'
        assert vector_key.columns == ('embedding1', 'embedding2')
        assert vector_key.index_options is None
        expected_repr = "VectorKey('multi_vec_idx', 'embedding1', 'embedding2')"
        assert repr(vector_key) == expected_repr

    def test_vector_key_with_complex_options(self) -> None:
        """Test vector key with complex JSON index options."""
        vector_key = VectorKey(
            'complex_idx', 'embedding',
            index_options='{"metric_type":"DOT_PRODUCT","index_type":"IVF_PQFS"}',
        )
        assert vector_key.name == 'complex_idx'
        assert vector_key.columns == ('embedding',)
        expected_options = '{"metric_type":"DOT_PRODUCT","index_type":"IVF_PQFS"}'
        assert vector_key.index_options == expected_options


class TestVectorKeyCompiler:
    """Test VectorKey DDL compilation."""

    def setup_method(self) -> None:
        """Set up mock engine for DDL compilation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_sql = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_sql = ''

    def test_compile_basic_vector_key(self) -> None:
        """Test compilation of basic vector key."""
        from sqlalchemy_singlestoredb.ddlelement import compile_vector_key

        vector_key = VectorKey('vec_idx', 'embedding')
        result = compile_vector_key(vector_key, None)
        assert result == 'VECTOR INDEX vec_idx (embedding)'

    def test_compile_vector_key_with_options(self) -> None:
        """Test compilation of vector key with index options."""
        from sqlalchemy_singlestoredb.ddlelement import compile_vector_key

        vector_key = VectorKey(
            'vec_idx', 'embedding',
            index_options='{"metric_type":"EUCLIDEAN_DISTANCE"}',
        )
        result = compile_vector_key(vector_key, None)
        expected = (
            'VECTOR INDEX vec_idx (embedding) '
            "INDEX_OPTIONS='{\"metric_type\":\"EUCLIDEAN_DISTANCE\"}'"
        )
        assert result == expected

    def test_compile_multi_column_vector_key(self) -> None:
        """Test compilation of multi-column vector key."""
        from sqlalchemy_singlestoredb.ddlelement import compile_vector_key

        vector_key = VectorKey('multi_vec_idx', 'embedding1', 'embedding2')
        result = compile_vector_key(vector_key, None)
        assert result == 'VECTOR INDEX multi_vec_idx (embedding1, embedding2)'

    def test_compile_vector_key_with_complex_options(self) -> None:
        """Test compilation of vector key with complex index options."""
        from sqlalchemy_singlestoredb.ddlelement import compile_vector_key

        vector_key = VectorKey(
            'complex_idx', 'embedding',
            index_options='{"metric_type":"DOT_PRODUCT","index_type":"IVF_PQFS"}',
        )
        result = compile_vector_key(vector_key, None)
        expected = (
            'VECTOR INDEX complex_idx (embedding) '
            'INDEX_OPTIONS=\'{"metric_type":"DOT_PRODUCT","index_type":"IVF_PQFS"}\''
        )
        assert result == expected


class TestVectorKeyTableIntegration:
    """Test VectorKey integration with table creation."""

    def setup_method(self) -> None:
        """Set up mock engine for table DDL generation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''
        self.metadata = MetaData()

    def test_table_with_basic_vector_index(self) -> None:
        """Test table creation with basic vector index."""
        Base = declarative_base()

        class MyTable(Base):  # type: ignore
            __tablename__ = 'test_table_vector'

            id = Column(Integer, primary_key=True)
            embedding = Column(VECTOR(128, 'F32'))
            data = Column(String(50))

            __table_args__ = {
                'info': {
                    'singlestoredb_vector_indexes': [
                        VectorKey('vec_idx', 'embedding'),
                    ],
                },
            }

        Base.metadata.create_all(self.mock_engine, checkfirst=False)

        # Check that VECTOR INDEX appears in the DDL
        assert 'VECTOR INDEX vec_idx (embedding)' in self.compiled_ddl
        assert 'CREATE TABLE test_table_vector' in self.compiled_ddl

    def test_table_with_vector_index_options(self) -> None:
        """Test table creation with vector index including options."""
        Base = declarative_base()

        class MyTable(Base):  # type: ignore
            __tablename__ = 'test_table_vector_opts'

            id = Column(Integer, primary_key=True)
            embedding = Column(VECTOR(256, 'F32'))
            data = Column(String(50))

            __table_args__ = {
                'info': {
                    'singlestoredb_vector_indexes': [
                        VectorKey(
                            'vec_idx', 'embedding',
                            index_options='{"metric_type":"EUCLIDEAN_DISTANCE"}',
                        ),
                    ],
                },
            }

        Base.metadata.create_all(self.mock_engine, checkfirst=False)

        # Check that VECTOR INDEX with options appears in the DDL
        assert 'VECTOR INDEX vec_idx (embedding)' in self.compiled_ddl
        expected_options = 'INDEX_OPTIONS=\'{"metric_type":"EUCLIDEAN_DISTANCE"}\''
        assert expected_options in self.compiled_ddl
        assert 'CREATE TABLE test_table_vector_opts' in self.compiled_ddl

    def test_table_with_multiple_vector_indexes(self) -> None:
        """Test table creation with multiple vector indexes."""
        Base = declarative_base()

        class MyTable(Base):  # type: ignore
            __tablename__ = 'test_table_multi_vector'

            id = Column(Integer, primary_key=True)
            content_embedding = Column(VECTOR(128, 'F32'))
            title_embedding = Column(VECTOR(256, 'F32'))
            data = Column(String(50))

            __table_args__ = {
                'info': {
                    'singlestoredb_vector_indexes': [
                        VectorKey('content_vec_idx', 'content_embedding'),
                        VectorKey(
                            'title_vec_idx', 'title_embedding',
                            index_options='{"metric_type":"DOT_PRODUCT"}',
                        ),
                    ],
                },
            }

        Base.metadata.create_all(self.mock_engine, checkfirst=False)

        # Check that both VECTOR INDEX definitions appear in the DDL
        assert 'VECTOR INDEX content_vec_idx (content_embedding)' in self.compiled_ddl
        assert 'VECTOR INDEX title_vec_idx (title_embedding)' in self.compiled_ddl
        assert 'INDEX_OPTIONS=\'{"metric_type":"DOT_PRODUCT"}\'' in self.compiled_ddl  # noqa: E501
        assert 'CREATE TABLE test_table_multi_vector' in self.compiled_ddl


class TestVectorKeyReflection:
    """Test VECTOR INDEX reflection from actual database tables."""

    def test_reflect_basic_vector_index(
            self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of basic VECTOR INDEX."""
        table_name = 'test_vector_reflection_basic'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with VECTOR INDEX
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        doc_id INT PRIMARY KEY,
                        embedding VECTOR(128, F32),
                        title VARCHAR(100),
                        VECTOR INDEX vec_idx (embedding)
                    )
                """),
                )

            # Reflect the table
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Verify basic structure
            assert len(reflected_table.columns) == 3
            assert 'doc_id' in reflected_table.columns
            assert 'embedding' in reflected_table.columns
            assert 'title' in reflected_table.columns

    def test_reflect_vector_index_with_options(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of VECTOR INDEX with INDEX_OPTIONS."""
        table_name = 'test_vector_reflection_options'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with VECTOR INDEX including options
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        doc_id INT PRIMARY KEY,
                        embedding VECTOR(256, F32),
                        content TEXT,
                        VECTOR INDEX vec_idx (embedding) INDEX_OPTIONS='{{"metric_type":"EUCLIDEAN_DISTANCE"}}'  # noqa: E501
                    )
                """),
                )

            # Reflect the table
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Verify structure
            assert len(reflected_table.columns) == 3
            assert 'doc_id' in reflected_table.columns
            assert 'embedding' in reflected_table.columns
            assert 'content' in reflected_table.columns

    def test_reflect_multiple_vector_indexes(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of table with multiple VECTOR INDEX definitions."""
        table_name = 'test_vector_reflection_multiple'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with multiple VECTOR INDEX definitions
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        doc_id INT PRIMARY KEY,
                        content_embedding VECTOR(128, F32),
                        title_embedding VECTOR(256, F32),
                        metadata_text TEXT,
                        VECTOR INDEX content_vec_idx (content_embedding),
                        VECTOR INDEX title_vec_idx (title_embedding) INDEX_OPTIONS='{{"metric_type":"DOT_PRODUCT"}}'  # noqa: E501
                    )
                """),
                )

            # Reflect the table
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Verify structure
            assert len(reflected_table.columns) == 4
            assert set(col.name for col in reflected_table.columns) == {
                'doc_id', 'content_embedding', 'title_embedding', 'metadata_text',
            }

    def test_reflect_complex_table_with_all_keys(
        self, test_engine: Any, clean_tables: Any,
    ) -> None:
        """Test reflection of complex table with SHARD KEY, SORT KEY, and VECTOR INDEX."""
        table_name = 'test_vector_reflection_complex'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create complex table with all key types
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        user_id INT,
                        doc_id INT,
                        content_embedding VECTOR(128, F32),
                        title VARCHAR(200),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        priority INT,
                        PRIMARY KEY (user_id, doc_id),
                        SHARD KEY (user_id),
                        SORT KEY (created_at, priority),
                        VECTOR INDEX content_vec_idx (content_embedding) INDEX_OPTIONS='{{"metric_type":"DOT_PRODUCT"}}',  # noqa: E501
                        KEY idx_title (title),
                        KEY idx_priority (priority)
                    )
                """),
                )

            # Reflect the table
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Verify structure
            assert len(reflected_table.columns) == 6
            assert set(col.name for col in reflected_table.primary_key.columns) == {
                'user_id', 'doc_id',
            }

            # Verify indexes can be retrieved without warnings
            indexes = test_engine.dialect.get_indexes(
                conn, table_name, schema=None, dbname=None,
            )
            # Should have regular indexes (not shard/sort keys or vector indexes)
            index_names = {idx['name'] for idx in indexes}
            assert 'idx_title' in index_names
            assert 'idx_priority' in index_names


class TestVectorKeyReflectionParser:
    """Test the reflection parser directly with various VECTOR INDEX patterns."""

    def test_parser_vector_index_variants(self) -> None:
        """Test that the reflection parser correctly identifies VECTOR INDEX variants."""
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
                'line': '  VECTOR INDEX vec_idx (embedding)',
                'expected_type': 'vector_key',
                'expected_name': 'vec_idx',
                'expected_columns': ['embedding'],
                'expected_options': None,
            },
            {
                'line': '  VECTOR INDEX content_vec (content_embedding) INDEX_OPTIONS=\'{"metric_type":"EUCLIDEAN_DISTANCE"}\'',  # noqa: E501
                'expected_type': 'vector_key',
                'expected_name': 'content_vec',
                'expected_columns': ['content_embedding'],
                'expected_options': '{"metric_type":"EUCLIDEAN_DISTANCE"}',
            },
            {
                'line': '  VECTOR INDEX multi_vec (embedding1, embedding2)',
                'expected_type': 'vector_key',
                'expected_name': 'multi_vec',
                'expected_columns': ['embedding1', 'embedding2'],
                'expected_options': None,
            },
            {
                'line': '  VECTOR INDEX complex_idx (embedding) INDEX_OPTIONS=\'{"metric_type":"DOT_PRODUCT","index_type":"IVF_PQFS"}\'',  # noqa: E501
                'expected_type': 'vector_key',
                'expected_name': 'complex_idx',
                'expected_columns': ['embedding'],
                'expected_options': '{"metric_type":"DOT_PRODUCT","index_type":"IVF_PQFS"}',  # noqa: E501
            },
        ]

        for case in test_cases:
            type_, spec = parser._parse_constraints(str(case['line']))  # type: ignore

            assert type_ == case['expected_type'], (  # type: ignore
                f"Line: {case['line']}, Expected type: "  # type: ignore
                f"{case['expected_type']}, Got: {type_}"
            )
            assert spec['name'] == case['expected_name'], (  # type: ignore
                f"Line: {case['line']}, Expected name: "  # type: ignore
                f"{case['expected_name']}, Got: {spec['name']}"
            )
            assert spec['columns'] == case['expected_columns'], (  # type: ignore
                f"Line: {case['line']}, Expected columns: "  # type: ignore
                f"{case['expected_columns']}, Got: {spec['columns']}"
            )
            assert spec.get('index_options') == \
                case['expected_options'], (  # type: ignore
                f"Line: {case['line']}, Expected options: "  # type: ignore
                f"{case['expected_options']}, Got: {spec.get('index_options')}"
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
                'line': '  VECTOR INDEX vec_idx (`embedding`)',
                'expected_columns': ['embedding'],
            },
            {
                'line': '  VECTOR INDEX multi_vec (`content_embedding`, `title_embedding`)',  # noqa: E501
                'expected_columns': ['content_embedding', 'title_embedding'],
            },
            {
                # Column name with special characters
                'line': '  VECTOR INDEX vec_idx (`content-embedding`)',
                'expected_columns': ['content-embedding'],
            },
        ]

        for case in test_cases:
            type_, spec = parser._parse_constraints(str(case['line']))
            assert type_ == 'vector_key'
            assert spec['columns'] == case['expected_columns'], (
                f"Line: {case['line']}, Expected: "
                f"{case['expected_columns']}, Got: {spec['columns']}"
            )


class TestVectorKeyTableConstructorIntegration:
    """Test SingleStore Table constructor with vector_indexes parameter integration."""

    def setup_method(self) -> None:
        """Set up mock engine for DDL generation."""
        def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
            self.compiled_ddl = str(sql.compile(dialect=self.mock_engine.dialect))

        self.mock_engine = create_mock_engine('singlestoredb://', dump)
        self.compiled_ddl = ''
        self.metadata = MetaData()

    def test_table_constructor_basic_vector_index(self) -> None:
        """Test Table constructor with basic vector index parameter."""
        table = Table(
            'test_basic_vector', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('embedding', VECTOR(128, 'F32')),
            Column('data', String(50)),
            VectorKey('vec_idx', 'embedding'),
        )

        # Verify info is set correctly
        assert 'singlestoredb_vector_indexes' in table.info
        vector_indexes = table.info['singlestoredb_vector_indexes']
        assert len(vector_indexes) == 1
        assert isinstance(vector_indexes[0], VectorKey)
        assert vector_indexes[0].name == 'vec_idx'
        assert vector_indexes[0].columns == ('embedding',)

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'VECTOR INDEX vec_idx (embedding)' in self.compiled_ddl
        assert 'CREATE TABLE test_basic_vector' in self.compiled_ddl

    def test_table_constructor_vector_index_with_options(self) -> None:
        """Test Table constructor with vector index including options."""
        table = Table(
            'test_vector_opts', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('embedding', VECTOR(256, 'F32')),
            Column('data', String(100)),
            VectorKey(
                'vec_idx', 'embedding',
                index_options='{"metric_type":"EUCLIDEAN_DISTANCE"}',
            ),
        )

        # Verify info is set correctly
        vector_indexes = table.info['singlestoredb_vector_indexes']
        vector_index = vector_indexes[0]
        assert vector_index.name == 'vec_idx'
        assert vector_index.columns == ('embedding',)
        assert vector_index.index_options == '{"metric_type":"EUCLIDEAN_DISTANCE"}'

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'VECTOR INDEX vec_idx (embedding)' in self.compiled_ddl
        assert 'INDEX_OPTIONS=\'{"metric_type":"EUCLIDEAN_DISTANCE"}\'' in self.compiled_ddl  # noqa: E501
        assert 'CREATE TABLE test_vector_opts' in self.compiled_ddl

    def test_table_constructor_multiple_vector_indexes(self) -> None:
        """Test Table constructor with multiple vector indexes."""
        table = Table(
            'test_multi_vector', self.metadata,
            Column('doc_id', Integer, primary_key=True),
            Column('content_embedding', VECTOR(128, 'F32')),
            Column('title_embedding', VECTOR(256, 'F32')),
            VectorKey('content_vec_idx', 'content_embedding'),
            VectorKey(
                'title_vec_idx', 'title_embedding',
                index_options='{"metric_type":"DOT_PRODUCT"}',
            ),
        )

        # Verify info is set correctly
        vector_indexes = table.info['singlestoredb_vector_indexes']
        assert len(vector_indexes) == 2

        content_idx = vector_indexes[0]
        assert content_idx.name == 'content_vec_idx'
        assert content_idx.columns == ('content_embedding',)
        assert content_idx.index_options is None

        title_idx = vector_indexes[1]
        assert title_idx.name == 'title_vec_idx'
        assert title_idx.columns == ('title_embedding',)
        assert title_idx.index_options == '{"metric_type":"DOT_PRODUCT"}'

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'VECTOR INDEX content_vec_idx (content_embedding)' in self.compiled_ddl
        assert 'VECTOR INDEX title_vec_idx (title_embedding)' in self.compiled_ddl
        assert 'INDEX_OPTIONS=\'{"metric_type":"DOT_PRODUCT"}\'' in self.compiled_ddl  # noqa: E501
        assert 'CREATE TABLE test_multi_vector' in self.compiled_ddl

    def test_table_constructor_mixed_keys(self) -> None:
        """Test Table constructor with shard key, sort key, and vector indexes."""
        from sqlalchemy_singlestoredb import ShardKey, SortKey

        table = Table(
            'test_mixed_keys', self.metadata,
            Column('user_id', Integer, primary_key=True),
            Column('doc_id', Integer, primary_key=True),
            Column('embedding', VECTOR(128, 'F32')),
            Column('created_at', String(50)),
            ShardKey('user_id'),
            SortKey('created_at'),
            VectorKey(
                'vec_idx', 'embedding',
                index_options='{"metric_type":"DOT_PRODUCT"}',
            ),
        )

        # Verify all info is set correctly
        assert 'singlestoredb_shard_key' in table.info
        assert 'singlestoredb_sort_key' in table.info
        assert 'singlestoredb_vector_indexes' in table.info

        shard_key = table.info['singlestoredb_shard_key']
        sort_key = table.info['singlestoredb_sort_key']
        vector_indexes = table.info['singlestoredb_vector_indexes']

        assert shard_key.columns == ('user_id',)
        assert sort_key.columns == ('created_at',)
        assert len(vector_indexes) == 1
        assert vector_indexes[0].name == 'vec_idx'

        # Test DDL generation
        table.create(self.mock_engine, checkfirst=False)
        assert 'SHARD KEY (user_id)' in self.compiled_ddl
        assert 'SORT KEY (created_at)' in self.compiled_ddl
        assert 'VECTOR INDEX vec_idx (embedding)' in self.compiled_ddl
        assert 'INDEX_OPTIONS=\'{"metric_type":"DOT_PRODUCT"}\'' in self.compiled_ddl  # noqa: E501
        assert 'CREATE TABLE test_mixed_keys' in self.compiled_ddl

    def test_table_constructor_preserves_existing_info(self) -> None:
        """Test that Table constructor preserves existing info dictionary."""
        table = Table(
            'test_preserve_vector', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('embedding', VECTOR(128, 'F32')),
            Column('data', String(50)),
            VectorKey('vec_idx', 'embedding'),
            info={'custom_key': 'custom_value'},
        )

        # Verify both custom info and vector indexes are preserved
        assert table.info['custom_key'] == 'custom_value'
        assert 'singlestoredb_vector_indexes' in table.info
        vector_indexes = table.info['singlestoredb_vector_indexes']
        assert len(vector_indexes) == 1
        assert vector_indexes[0].name == 'vec_idx'

    def test_table_constructor_no_vector_indexes(self) -> None:
        """Test that Table constructor works normally without vector index parameters."""
        table = Table(
            'test_normal_vector', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
        )

        # Should not have vector indexes info
        assert 'singlestoredb_vector_indexes' not in table.info

        # Test DDL generation (should work normally)
        table.create(self.mock_engine, checkfirst=False)
        assert 'CREATE TABLE test_normal_vector' in self.compiled_ddl
        assert 'VECTOR INDEX' not in self.compiled_ddl
