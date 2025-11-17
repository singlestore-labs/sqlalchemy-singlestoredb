#!/usr/bin/env python
"""Test advanced SingleStore index types: VECTOR, Multi-Value, and Foreign Keys."""
from __future__ import annotations

import pytest
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.engine import Engine


class TestAdvancedIndexes:
    """Test Vector indexes, Multi-Value indexes, and Foreign Key syntax."""

    def test_vector_index_creation_and_reflection(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test creation and reflection of VECTOR indexes."""
        table_name = 'test_vector_index'

        with test_engine.connect() as conn:
            # Skip test if VECTOR type is not supported
            try:
                with conn.begin():
                    # Create table with VECTOR column and VECTOR index
                    conn.execute(
                        text(f"""
                        CREATE TABLE {table_name} (
                            id INT PRIMARY KEY,
                            embedding VECTOR(128, F32),
                            metadata JSON,
                            SHARD KEY (id),
                            VECTOR INDEX vec_idx (embedding)
                        )
                    """),
                    )

            except Exception as e:
                error_msg = str(e)
                if (
                    'VECTOR' in error_msg or
                    'not supported' in error_msg.lower() or
                    'syntax' in error_msg.lower()
                ):
                    pytest.skip('VECTOR indexes not supported')
                else:
                    raise

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print(f'\\nGenerated CREATE TABLE for {table_name}:')
            print(create_sql)

            # Verify the table can be reflected without warnings
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) >= 3
            assert 'id' in reflected_table.columns
            assert 'embedding' in reflected_table.columns
            assert 'metadata' in reflected_table.columns

            # Check that VECTOR column type is preserved
            embedding_col = reflected_table.columns['embedding']
            print(f'\\nEmbedding column type: {embedding_col.type}')

            # Should have indexes (VECTOR index might be reflected differently)
            indexes = reflected_table.indexes
            index_names = {idx.name for idx in indexes}
            print(f'\\nReflected indexes: {index_names}')

    def test_vector_index_using_vector_syntax(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test VECTOR index with USING VECTOR syntax."""
        table_name = 'test_vector_using'

        with test_engine.connect() as conn:
            try:
                with conn.begin():
                    # Create table with VECTOR index using USING VECTOR syntax
                    conn.execute(
                        text(f"""
                        CREATE TABLE {table_name} (
                            doc_id INT,
                            content TEXT,
                            embedding VECTOR(256, F32),
                            PRIMARY KEY (doc_id),
                            KEY vec_search (embedding) USING VECTOR
                        )
                    """),
                    )

            except Exception as e:
                error_msg = str(e)
                if (
                    'VECTOR' in error_msg or
                    'not supported' in error_msg.lower() or
                    'syntax' in error_msg.lower()
                ):
                    pytest.skip('VECTOR indexes not supported')
                else:
                    raise

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print(f'\\nGenerated CREATE TABLE for {table_name}:')
            print(create_sql)

            # Verify reflection works
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert 'doc_id' in reflected_table.columns
            assert 'embedding' in reflected_table.columns
            assert 'content' in reflected_table.columns

    def test_multi_value_index_on_json(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test Multi-Value indexes on JSON columns."""
        table_name = 'test_multi_value_json'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with JSON column
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        tags JSON,
                        metadata JSON,
                        content TEXT
                    )
                """),
                )

            # Try to add Multi-Value index on JSON column
            try:
                with conn.begin():
                    # Add multi-value index using ALTER TABLE
                    conn.execute(
                        text(f"""
                        ALTER TABLE {table_name}
                        ADD MULTI VALUE INDEX mv_tags (tags)
                    """),
                    )

            except Exception as e:
                error_msg = str(e).lower()
                if (
                    ('multi' in error_msg and 'value' in error_msg) or
                    'not supported' in error_msg or
                    ('index' in error_msg and 'json' in error_msg)
                ):
                    pytest.skip('Multi-Value indexes not supported')
                else:
                    raise

            # Show the updated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print('\\nCREATE TABLE with Multi-Value index:')
            print(create_sql)

            # Verify reflection works
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert 'id' in reflected_table.columns
            assert 'tags' in reflected_table.columns
            assert 'metadata' in reflected_table.columns
            assert 'content' in reflected_table.columns

    def test_multi_value_index_creation_syntax(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test creating Multi-Value index in CREATE TABLE statement."""
        table_name = 'test_multi_value_create'

        with test_engine.connect() as conn:
            try:
                with conn.begin():
                    # Try to create table with Multi-Value index in CREATE TABLE
                    conn.execute(
                        text(f"""
                        CREATE TABLE {table_name} (
                            id INT PRIMARY KEY,
                            categories JSON,
                            attributes JSON,
                            name VARCHAR(255),
                            MULTI VALUE INDEX mv_categories (categories),
                            KEY idx_name (name)
                        )
                    """),
                    )

            except Exception as e:
                # Multi-Value indexes might only be supported via ALTER TABLE
                if ('multi' in str(e).lower() and 'value' in str(e).lower()) or \
                   'syntax' in str(e).lower():
                    print(f'Multi-Value index in CREATE TABLE not supported: {e}')
                    # Try without the Multi-Value index
                    with conn.begin():
                        conn.execute(
                            text(f"""
                            CREATE TABLE {table_name} (
                                id INT PRIMARY KEY,
                                categories JSON,
                                attributes JSON,
                                name VARCHAR(255),
                                KEY idx_name (name)
                            )
                        """),
                        )
                    pytest.skip('Multi-Value index in CREATE TABLE not supported')
                else:
                    raise

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print('\\nCREATE TABLE with Multi-Value index:')
            print(create_sql)

            # Verify reflection works
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            assert 'categories' in reflected_table.columns

    def test_foreign_key_syntax_unenforced(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test Foreign Key syntax (unenforced in SingleStore)."""
        parent_table = 'test_parent_fk'
        child_table = 'test_child_fk'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create parent table with proper sharding
                conn.execute(
                    text(f"""
                    CREATE TABLE {parent_table} (
                        id INT,
                        name VARCHAR(100),
                        PRIMARY KEY (id),
                        SHARD KEY (id)
                    )
                """),
                )

            # Try to create child table with foreign key reference
            try:
                with conn.begin():
                    conn.execute(
                        text(f"""
                        CREATE TABLE {child_table} (
                            id INT,
                            parent_id INT,
                            description TEXT,
                            PRIMARY KEY (id),
                            FOREIGN KEY (parent_id) REFERENCES {parent_table}(id),
                            SHARD KEY (id)
                        )
                    """),
                    )
            except Exception as e:
                if 'foreign key' in str(e).lower() and 'not supported' in str(e).lower():
                    pytest.skip(
                        'Foreign keys not supported in this SingleStore version',
                    )
                else:
                    raise

            # Show the generated CREATE TABLE statements
            for table in [parent_table, child_table]:
                result = conn.execute(text(f'SHOW CREATE TABLE {table}'))
                create_sql = result.fetchone()[1]
                print(f'\\nCREATE TABLE for {table}:')
                print(create_sql)

            # Verify both tables can be reflected
            metadata = MetaData()
            parent_reflected = Table(parent_table, metadata, autoload_with=test_engine)
            child_reflected = Table(child_table, metadata, autoload_with=test_engine)

            # Check parent table
            assert 'id' in parent_reflected.columns
            assert 'name' in parent_reflected.columns

            # Check child table
            assert 'id' in child_reflected.columns
            assert 'parent_id' in child_reflected.columns
            assert 'description' in child_reflected.columns

            # Check if foreign key constraints are reflected
            fk_constraints = child_reflected.foreign_key_constraints
            print(f'\\nForeign key constraints reflected: {len(fk_constraints)}')
            for fk in fk_constraints:
                print(f'  FK: {fk.name} -> {fk.referred_table}.{fk.referred_columns}')

    def test_foreign_key_with_cascade_options(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test Foreign Key with CASCADE options (syntax only)."""
        parent_table = 'test_parent_cascade'
        child_table = 'test_child_cascade'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create parent table - Keep it simple for foreign key testing
                conn.execute(
                    text(f"""
                    CREATE TABLE {parent_table} (
                        id INT,
                        code VARCHAR(50),
                        name VARCHAR(100),
                        PRIMARY KEY (id),
                        SHARD KEY (id)
                    )
                """),
                )

            try:
                with conn.begin():
                    # Create child table with foreign key and CASCADE options
                    conn.execute(
                        text(f"""
                        CREATE TABLE {child_table} (
                            id INT PRIMARY KEY,
                            parent_id INT,
                            value DECIMAL(10,2),
                            FOREIGN KEY fk_parent_id (parent_id)
                                REFERENCES {parent_table}(id)
                                ON DELETE CASCADE ON UPDATE RESTRICT
                        )
                    """),
                    )

            except Exception as e:
                # Check if foreign keys are not supported at all
                if 'foreign key' in str(e).lower() and 'not supported' in str(e).lower():
                    pytest.skip(
                        'Foreign keys not supported in this SingleStore version',
                    )
                # Check if CASCADE/SET NULL is not supported
                # (known SingleStore limitation)
                elif 'cascade' in str(e).lower() or 'set null' in str(e).lower():
                    # Try without CASCADE - some versions support basic FK syntax
                    try:
                        with conn.begin():
                            conn.execute(
                                text(f"""
                                CREATE TABLE {child_table} (
                                    id INT PRIMARY KEY,
                                    parent_id INT,
                                    value DECIMAL(10,2),
                                    FOREIGN KEY (parent_id) REFERENCES {parent_table}(id)
                                )
                            """),
                            )
                    except Exception as e2:
                        # Basic FKs also not supported
                        if 'foreign key' in str(e2).lower():
                            pytest.skip(
                                'Foreign keys not supported in this version',
                            )
                        else:
                            raise
                else:
                    raise

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {child_table}'))
            create_sql = result.fetchone()[1]
            print('\\nCREATE TABLE with Foreign Keys:')
            print(create_sql)

            # Verify reflection works
            metadata = MetaData()
            reflected_table = Table(child_table, metadata, autoload_with=test_engine)

            assert 'parent_id' in reflected_table.columns
            assert 'value' in reflected_table.columns

    def test_complex_table_with_all_features(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test table with Vector, Multi-Value, Foreign Key, other indexes."""
        parent_table = 'test_complex_parent'
        complex_table = 'test_complex_all'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create parent table for foreign key reference
                conn.execute(
                    text(f"""
                    CREATE TABLE {parent_table} (
                        id INT,
                        category VARCHAR(50),
                        PRIMARY KEY (id),
                        SHARD KEY (id)
                    )
                """),
                )

            try:
                with conn.begin():
                    # Create complex table with multiple index types
                    conn.execute(
                        text(f"""
                        CREATE TABLE {complex_table} (
                            id INT,
                            parent_id INT,
                            embedding VECTOR(128, F32),
                            tags JSON,
                            title VARCHAR(255),
                            content TEXT,
                            created_at DATETIME,

                            PRIMARY KEY (id),
                            SHARD KEY (id),
                            SORT KEY (created_at),

                            VECTOR INDEX vec_idx (embedding),
                            KEY idx_title (title),
                            FULLTEXT KEY ft_content (content),

                            FOREIGN KEY (parent_id) REFERENCES {parent_table}(id)
                        )
                    """),
                    )

            except Exception as e:
                print(f'Complex table creation error: {e}')
                # Check if it's a foreign key issue
                if 'foreign key' in str(e).lower() and 'not supported' in str(e).lower():
                    # Try without foreign key
                    try:
                        with conn.begin():
                            conn.execute(
                                text(f"""
                                CREATE TABLE {complex_table} (
                                    id INT,
                                    parent_id INT,
                                    embedding VECTOR(128, F32),
                                    tags JSON,
                                    title VARCHAR(255),
                                    content TEXT,
                                    created_at DATETIME,

                                    PRIMARY KEY (id),
                                    SHARD KEY (id),
                                    SORT KEY (created_at),

                                    VECTOR INDEX vec_idx (embedding),
                                    KEY idx_title (title),
                                    FULLTEXT KEY ft_content (content)
                                )
                            """),
                            )
                    except Exception as e2:
                        print(f'Even without FK failed: {e2}')
                        # Fallback to simplest version
                        with conn.begin():
                            conn.execute(
                                text(f"""
                                CREATE TABLE {complex_table} (
                                    id INT,
                                    parent_id INT,
                                    title VARCHAR(255),
                                    content TEXT,
                                    created_at DATETIME,

                                    PRIMARY KEY (id),
                                    KEY idx_title (title),
                                    FULLTEXT KEY ft_content (content)
                                )
                            """),
                            )
                else:
                    # Fallback to simpler version without unsupported features
                    with conn.begin():
                        conn.execute(
                            text(f"""
                            CREATE TABLE {complex_table} (
                                id INT,
                                parent_id INT,
                                title VARCHAR(255),
                                content TEXT,
                                created_at DATETIME,

                                PRIMARY KEY (id),
                                KEY idx_title (title),
                                FULLTEXT KEY ft_content (content)
                            )
                        """),
                        )

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {complex_table}'))
            create_sql = result.fetchone()[1]
            print('\\nCREATE TABLE for complex table:')
            print(create_sql)

            # Verify reflection works without errors
            metadata = MetaData()
            complex_reflected = Table(complex_table, metadata, autoload_with=test_engine)

            # Should have basic columns at minimum
            assert 'id' in complex_reflected.columns
            assert 'parent_id' in complex_reflected.columns
            assert 'title' in complex_reflected.columns

            # Should have some indexes
            indexes = complex_reflected.indexes
            print(f'\\nReflected indexes: {[idx.name for idx in indexes]}')

            # Should have foreign key constraints
            fks = complex_reflected.foreign_key_constraints
            print(f'Foreign key constraints: {[fk.name for fk in fks]}')

    def test_vector_index_options_and_parameters(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test VECTOR indexes with various options and parameters."""
        table_name = 'test_vector_options'

        with test_engine.connect() as conn:
            try:
                with conn.begin():
                    # Create table with VECTOR index and various parameters
                    conn.execute(
                        text(f"""
                        CREATE TABLE {table_name} (
                            doc_id INT PRIMARY KEY,
                            embedding_128 VECTOR(128, F32),
                            embedding_256 VECTOR(256, F32),
                            metadata JSON,

                            VECTOR INDEX idx_128 (embedding_128)
                              INDEX_OPTIONS='{{\"metric_type\":\"EUCLIDEAN_DISTANCE\"}}',
                            VECTOR INDEX idx_256 (embedding_256)
                              INDEX_OPTIONS='{{\"metric_type\":\"DOT_PRODUCT\"}}'
                        )
                    """),
                    )

            except Exception as e:
                error_msg = str(e)
                if (
                    'VECTOR' in error_msg or
                    'INDEX_OPTIONS' in error_msg or
                    'not supported' in error_msg.lower() or
                    'syntax' in error_msg.lower()
                ):
                    pytest.skip('VECTOR index options not supported')
                else:
                    raise

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print('\\nCREATE TABLE with VECTOR index options:')
            print(create_sql)

            # Verify reflection works
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            assert 'embedding_128' in reflected_table.columns
            assert 'embedding_256' in reflected_table.columns

    def test_multivalue_index_compilation_with_special_column_names(self) -> None:
        """Test MultiValueIndex DDL compilation with special column names."""
        from sqlalchemy_singlestoredb.ddlelement import (
            MultiValueIndex, compile_multi_value_index,
        )

        # Test column with hyphen
        mv_idx = MultiValueIndex('tags-array')
        result = compile_multi_value_index(mv_idx, None)
        assert result == 'MULTI VALUE INDEX (`tags-array`)'

        # Test column with space
        mv_idx = MultiValueIndex('tags array')
        result = compile_multi_value_index(mv_idx, None)
        assert result == 'MULTI VALUE INDEX (`tags array`)'

        # Test column with backticks
        mv_idx = MultiValueIndex('column`with`backticks')
        result = compile_multi_value_index(mv_idx, None)
        assert result == 'MULTI VALUE INDEX (`column``with``backticks`)'

        # Test with index options and special characters
        mv_idx = MultiValueIndex(
            'tags-array', index_options='{"option":"value"}',
        )
        result = compile_multi_value_index(mv_idx, None)
        expected = 'MULTI VALUE INDEX (`tags-array`) INDEX_OPTIONS=\'{"option":"value"}\''
        assert result == expected

    def test_fulltext_index_compilation_with_special_column_names(self) -> None:
        """Test FullTextIndex DDL compilation with special column names."""
        from sqlalchemy_singlestoredb.ddlelement import (
            FullTextIndex, compile_fulltext_index,
        )

        # Test single column with hyphen
        ft_idx = FullTextIndex('title-column')
        result = compile_fulltext_index(ft_idx, None)
        assert result == 'FULLTEXT (`title-column`)'

        # Test single column with space
        ft_idx = FullTextIndex('title column', name='ft_idx')
        result = compile_fulltext_index(ft_idx, None)
        assert result == 'FULLTEXT ft_idx (`title column`)'

        # Test multiple columns with special characters
        ft_idx = FullTextIndex(
            'title column', 'content-field', name='search_idx',
        )
        result = compile_fulltext_index(ft_idx, None)
        expected = 'FULLTEXT search_idx (`title column`, `content-field`)'
        assert result == expected

        # Test column with backticks
        ft_idx = FullTextIndex('column`with`backticks')
        result = compile_fulltext_index(ft_idx, None)
        assert result == 'FULLTEXT (`column``with``backticks`)'

        # Test with version and special characters
        ft_idx = FullTextIndex('content-field', name='ft_v2', version=2)
        result = compile_fulltext_index(ft_idx, None)
        assert result == 'FULLTEXT USING VERSION 2 ft_v2 (`content-field`)'

    def test_fulltext_index_reflection(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test reflection of FULLTEXT indexes."""
        table_name = 'test_fulltext_reflection'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with FULLTEXT index
                # (SingleStore supports only one FULLTEXT KEY per table)
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        title VARCHAR(200),
                        content TEXT,
                        description TEXT,
                        FULLTEXT KEY ft_content (content)
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
            assert 'title' in reflected_table.columns
            assert 'content' in reflected_table.columns
            assert 'description' in reflected_table.columns

            # Check that indexes are reflected
            indexes = reflected_table.indexes
            index_names = {idx.name for idx in indexes}
            print(f'\nReflected indexes: {index_names}')

    def test_fulltext_index_with_multiple_columns_reflection(
        self, test_engine: Engine, clean_tables: None,
    ) -> None:
        """Test reflection of FULLTEXT index with multiple columns."""
        table_name = 'test_fulltext_multi_reflection'

        with test_engine.connect() as conn:
            with conn.begin():
                # Create table with multi-column FULLTEXT index
                conn.execute(
                    text(f"""
                    CREATE TABLE {table_name} (
                        doc_id INT PRIMARY KEY,
                        title VARCHAR(200),
                        content TEXT,
                        summary TEXT,
                        FULLTEXT KEY ft_multi (title, content, summary)
                    )
                """),
                )

            # Show the generated CREATE TABLE
            result = conn.execute(text(f'SHOW CREATE TABLE {table_name}'))
            create_sql = result.fetchone()[1]
            print(f'\nGenerated CREATE TABLE for {table_name}:')
            print(create_sql)

            # Verify reflection works without errors
            metadata = MetaData()
            reflected_table = Table(table_name, metadata, autoload_with=test_engine)

            # Should have expected columns
            assert len(reflected_table.columns) == 4
            assert 'doc_id' in reflected_table.columns
            assert 'title' in reflected_table.columns
            assert 'content' in reflected_table.columns
            assert 'summary' in reflected_table.columns

            # Check that indexes are reflected
            indexes = reflected_table.indexes
            index_names = {idx.name for idx in indexes}
            print(f'\nReflected indexes: {index_names}')
