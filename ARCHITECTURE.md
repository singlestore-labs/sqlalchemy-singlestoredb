# Architecture

This document describes the architecture of the `sqlalchemy-singlestoredb` project, a SQLAlchemy dialect for [SingleStore](https://www.singlestore.com/) (formerly MemSQL), a distributed SQL database designed for high-performance transactional and analytical workloads.

## Overview

The dialect extends SQLAlchemy's MySQL dialect with SingleStore-specific features:

- **Distributed data management** - Shard keys, sort keys for data distribution
- **AI/ML support** - Native VECTOR type and vector indexes for similarity search
- **Table types** - RowStore (OLTP) and ColumnStore (OLAP) storage
- **Advanced indexing** - Full-text indexes, multi-value indexes, column groups
- **Computed columns** - PERSISTED computed columns

## Directory Structure

```
sqlalchemy-singlestoredb/
├── sqlalchemy_singlestoredb/     # Main package
│   ├── __init__.py               # Public API exports
│   ├── base.py                   # Core dialect and compilers
│   ├── ddlelement.py             # DDL elements (ShardKey, etc.)
│   ├── dtypes.py                 # Custom data types (JSON, VECTOR)
│   ├── column.py                 # PersistedColumn implementation
│   ├── reflection.py             # Table introspection/reflection
│   ├── compat.py                 # SQLAlchemy version compatibility
│   └── tests/                    # Test suite
├── examples/                     # Usage examples
├── docs/                         # Documentation
└── dev-docs/                     # Developer documentation
```

## Core Modules

### `__init__.py`

Package entry point that exports the public API:

- `dialect` - The `SingleStoreDBDialect` class registered with SQLAlchemy
- `array` - Array construct for building SQL arrays
- `__version__` - Package version string

**Exports:** `SingleStoreDBDialect`, `SingleStoreDBCompiler`, `SingleStoreDBDDLCompiler`, `SingleStoreDBTypeCompiler`, `SingleStoreDBIdentifierPreparer`, `SingleStoreDBExecutionContext`, `ShardKey`, `SortKey`, `VectorKey`, `MultiValueIndex`, `FullTextIndex`, `ColumnGroup`, `RowStore`, `ColumnStore`, `JSON`, `VECTOR`, `PersistedColumn`

### `base.py`

The core dialect implementation, containing the primary classes for SQL compilation and dialect behavior.

**Classes:**

| Class | Base | Purpose |
|-------|------|---------|
| `SingleStoreDBDialect` | `MySQLDialect` | Main dialect class; handles connection, type mapping, and introspection |
| `SingleStoreDBCompiler` | `MySQLCompiler` | SQL statement compilation |
| `SingleStoreDBDDLCompiler` | `MySQLDDLCompiler` | DDL statement compilation (CREATE TABLE, etc.) |
| `SingleStoreDBTypeCompiler` | `MySQLTypeCompiler` | Data type compilation (DATETIME, TIMESTAMP, VECTOR) |
| `SingleStoreDBIdentifierPreparer` | `MySQLIdentifierPreparer` | Identifier quoting and escaping |
| `SingleStoreDBExecutionContext` | `MySQLExecutionContext` | Query execution context |
| `CaseInsensitiveDict` | `MutableMapping` | Case-insensitive dictionary for connection parameters |
| `Array` | `ColumnElement` | SQL array expression construct |
| `_myconnpyBIT` | `_Binary` | MySQL Connector/Python BIT type processor |

**Key features:**

- Extends `ischema_names` with SingleStore types (JSON, VECTOR)
- Custom `visit_*` methods for SingleStore SQL syntax
- Table option handling for SingleStore-specific options
- Server-side cursor support for large result sets

### `ddlelement.py`

DDL (Data Definition Language) elements for SingleStore-specific table features. These are used as table arguments when defining tables.

**Classes:**

| Class | Purpose |
|-------|---------|
| `ShardKey` | Defines the shard key for data distribution across nodes |
| `SortKey` | Defines the sort key for data ordering within shards |
| `VectorKey` | Defines a vector index for similarity search (ANN) |
| `MultiValueIndex` | Index on JSON array values |
| `FullTextIndex` | Full-text search index |
| `ColumnGroup` | Groups columns for ColumnStore optimization |
| `TableType` | Base class for table storage types |
| `RowStore` | Row-oriented storage (OLTP workloads) |
| `ColumnStore` | Column-oriented storage (OLAP workloads) |

**Compiler functions:**

- `compile_shard_key()` - Generates `SHARD KEY (...)` clause
- `compile_sort_key()` - Generates `SORT KEY (...)` clause
- `compile_vector_key()` - Generates `VECTOR INDEX ... USING ...` clause
- `compile_multi_value_index()` - Generates `INDEX ... USING HASH ...` clause
- `compile_fulltext_index()` - Generates `FULLTEXT KEY ... USING VERSION ...` clause
- `compile_column_group()` - Generates `KEY ... (...) USING CLUSTERED COLUMNSTORE` clause

**Example usage:**

```python
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy_singlestoredb import ShardKey, SortKey, ColumnStore

metadata = MetaData()
users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True),
    Column('tenant_id', Integer),
    Column('name', String(100)),
    ShardKey('tenant_id'),
    SortKey('id'),
    ColumnStore(),
)
```

### `dtypes.py`

Custom SQLAlchemy data types for SingleStore-specific column types.

**Classes:**

| Class | Base | Purpose |
|-------|------|---------|
| `JSON` | `TypeEngine` | JSON column type with proper serialization/deserialization |
| `VECTOR` | `TypeEngine` | Vector type for AI/ML embeddings and similarity search |

**JSON type:**

- Handles JSON encoding/decoding
- Supports custom JSON deserializers
- Literal binding for SQL queries

**VECTOR type:**

- Supports fixed-dimension vectors: `VECTOR(dimensions, element_type)`
- Element types: `F32` (float32), `F64` (float64), `I8` (int8), `I16` (int16), `I32` (int32), `I64` (int64)
- Result processing for Python list conversion
- Cache key generation for query caching

**Example usage:**

```python
from sqlalchemy import Column, Table
from sqlalchemy_singlestoredb import JSON, VECTOR

Table(
    'items', metadata,
    Column('id', Integer, primary_key=True),
    Column('metadata', JSON),
    Column('embedding', VECTOR(1536, 'F32')),
)
```

### `column.py`

Custom column types for SingleStore-specific column behavior.

**Classes:**

| Class | Base | Purpose |
|-------|------|---------|
| `PersistedColumn` | `Column` | Column with a PERSISTED computed value |

**PersistedColumn:**

Defines columns whose values are computed from an expression and stored persistently (materialized). This is SingleStore's equivalent to MySQL's generated stored columns, but with different syntax.

**Example usage:**

```python
from sqlalchemy_singlestoredb import PersistedColumn

Table(
    'orders', metadata,
    Column('price', Numeric),
    Column('quantity', Integer),
    PersistedColumn('total', Numeric, 'price * quantity'),
)
```

### `reflection.py`

Table introspection and reflection support, enabling SQLAlchemy to read existing table definitions from the database.

**Classes:**

| Class | Purpose |
|-------|---------|
| `SingleStoreDBTableDefinitionParser` | Parses `SHOW CREATE TABLE` output to extract table structure |

**Key methods:**

- `parse()` - Main entry point for parsing table definitions
- `_parse_column()` - Extracts column definitions
- `_parse_constraints()` - Extracts keys, indexes, and constraints
- `_parse_table_name()` - Parses table name from DDL

**Handled features:**

- Primary keys, unique keys, indexes
- Shard keys and sort keys
- Vector indexes
- Full-text indexes
- Multi-value indexes
- Column groups
- RowStore/ColumnStore table types
- PERSISTED computed columns

### `compat.py`

SQLAlchemy version compatibility layer, supporting both SQLAlchemy 1.4.x and 2.x.

**Constants:**

- `SQLALCHEMY_VERSION` - Parsed version tuple
- `SQLALCHEMY_1_4` - Boolean flag for 1.4.x compatibility
- `SQLALCHEMY_2_0` - Boolean flag for 2.x compatibility
- `HAS_CACHE_KEY` - Whether cache key support is available

**Functions:**

| Function | Purpose |
|----------|---------|
| `get_sqlalchemy_version()` | Returns the installed SQLAlchemy version |
| `has_feature(name)` | Checks if a SQLAlchemy feature is available |
| `get_insert_class()` | Returns the appropriate Insert class |
| `get_update_class()` | Returns the appropriate Update class |
| `get_delete_class()` | Returns the appropriate Delete class |
| `make_cache_key()` | Creates a cache key for compiled queries |
| `supports_statement_cache()` | Checks if statement caching is supported |
| `get_dialect_features()` | Returns dialect feature flags |
| `get_dialect_attribute()` | Gets a dialect attribute safely |
| `set_dialect_attribute()` | Sets a dialect attribute safely |
| `warn_version_compatibility()` | Emits deprecation warnings for old versions |

## Class Hierarchy

```
MySQLDialect (sqlalchemy.dialects.mysql)
└── SingleStoreDBDialect
    ├── statement_compiler = SingleStoreDBCompiler
    ├── ddl_compiler = SingleStoreDBDDLCompiler
    ├── type_compiler = SingleStoreDBTypeCompiler
    ├── preparer = SingleStoreDBIdentifierPreparer
    └── execution_ctx_cls = SingleStoreDBExecutionContext

MySQLCompiler
└── SingleStoreDBCompiler
    Methods: visit_array, visit_typeclause, visit_cast,
             post_process_text, visit_textclause, visit_primary_key_constraint

MySQLDDLCompiler
└── SingleStoreDBDDLCompiler
    Methods: post_create_table, visit_create_table, get_column_specification

MySQLTypeCompiler
└── SingleStoreDBTypeCompiler
    Methods: visit_DATETIME, visit_TIMESTAMP, visit_VECTOR

TypeEngine (sqlalchemy)
├── JSON
│   Methods: result_processor, _cached_literal_processor, _gen_cache_key
└── VECTOR
    Methods: result_processor, _cached_literal_processor, _gen_cache_key

DDLElement (sqlalchemy.schema)
├── ShardKey
├── SortKey
├── VectorKey
├── MultiValueIndex
├── FullTextIndex
├── ColumnGroup
└── TableType
    ├── RowStore
    └── ColumnStore

Column (sqlalchemy)
└── PersistedColumn
```

## Entry Points

The dialect is registered with SQLAlchemy via entry points in `pyproject.toml`:

```toml
[project.entry-points.'sqlalchemy.dialects']
singlestoredb = 'sqlalchemy_singlestoredb:dialect'
'singlestoredb.mysql' = 'sqlalchemy_singlestoredb:dialect'
'singlestoredb.http' = 'sqlalchemy_singlestoredb:dialect'
'singlestoredb.https' = 'sqlalchemy_singlestoredb:dialect'
```

**Connection URLs:**

- `singlestoredb://user:pass@host:3306/db` - Default (uses singlestoredb Python driver)
- `singlestoredb+mysql://user:pass@host:3306/db` - MySQL compatibility alias
- `singlestoredb+http://user:pass@host:9000/db` - HTTP protocol
- `singlestoredb+https://user:pass@host:9000/db` - HTTPS protocol

## Test Organization

Tests are located in `sqlalchemy_singlestoredb/tests/`:

| File | Purpose |
|------|---------|
| `conftest.py` | Pytest fixtures and configuration |
| `test_basics.py` | Basic dialect functionality |
| `test_shard_key.py` | ShardKey DDL element |
| `test_sort_key.py` | SortKey DDL element |
| `test_vector_key.py` | VectorKey DDL element |
| `test_vector_type.py` | VECTOR data type |
| `test_json_type.py` | JSON data type |
| `test_table_types.py` | RowStore/ColumnStore |
| `test_column_group.py` | ColumnGroup DDL element |
| `test_advanced_indexes.py` | MultiValueIndex, FullTextIndex |
| `test_persisted_column.py` | PersistedColumn |
| `test_key_reflection.py` | Key/index reflection |
| `test_declarative_reflection.py` | ORM declarative reflection |
| `test_table_options.py` | Table option handling |
| `test_error_handling.py` | Error handling |
| `test_integration_features.py` | Integration tests |
| `test_priority1_features.py` | High-priority feature tests |
| `test_database_fixtures.py` | Database fixture tests |

Run tests with:

```bash
pytest sqlalchemy_singlestoredb/tests/
```

## Configuration and Build

**Dependencies:**

- `singlestoredb>=1.0.0` - SingleStore Python driver
- `sqlalchemy>=1.4.0,<3.0.0dev` - SQLAlchemy ORM

**Development tools:**

- `pytest` - Test framework
- `pytest-cov` - Coverage reporting
- `pre-commit` - Git hooks for code quality
- `flake8` - Linter
- `autopep8` - Code formatter
- `mypy` - Static type checking

**Build:**

```bash
pip install build
python -m build
```

## Data Flow

### Table Creation

```
Table Definition (Python)
    │
    ▼
SingleStoreDBDDLCompiler.visit_create_table()
    │
    ├── get_column_specification() for each column
    │       │
    │       └── PersistedColumn → AS (expr) PERSISTED
    │
    └── post_create_table()
            │
            ├── compile_shard_key() → SHARD KEY (...)
            ├── compile_sort_key() → SORT KEY (...)
            ├── compile_vector_key() → VECTOR INDEX ... USING ...
            ├── compile_multi_value_index() → INDEX ... USING HASH ...
            ├── compile_fulltext_index() → FULLTEXT KEY ...
            └── compile_column_group() → KEY ... USING CLUSTERED COLUMNSTORE
    │
    ▼
CREATE TABLE SQL Statement
```

### Table Reflection

```
SHOW CREATE TABLE (SQL)
    │
    ▼
SingleStoreDBTableDefinitionParser.parse()
    │
    ├── _parse_table_name()
    ├── _parse_column() for each column
    │       │
    │       └── Detects PERSISTED columns
    │
    └── _parse_constraints()
            │
            ├── Primary key → PrimaryKeyConstraint
            ├── SHARD KEY → ShardKey
            ├── SORT KEY → SortKey
            ├── VECTOR INDEX → VectorKey
            ├── FULLTEXT KEY → FullTextIndex
            └── USING HASH → MultiValueIndex
    │
    ▼
Table object with reflected columns and constraints
```

## Version Compatibility

The dialect supports both SQLAlchemy 1.4.x and 2.x through the compatibility layer in `compat.py`. Key differences handled:

| Feature | SQLAlchemy 1.4 | SQLAlchemy 2.0 |
|---------|----------------|----------------|
| Insert class | `sqlalchemy.dialects.mysql.Insert` | `sqlalchemy.dialects.mysql.Insert` |
| Cache key support | Partial | Full |
| Type system | Legacy | Unified |

The `compat.py` module abstracts these differences so the rest of the codebase can use a consistent API.
