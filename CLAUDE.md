# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Testing
- Run all tests: `pytest sqlalchemy_singlestoredb/tests/`
- Run specific test: `pytest sqlalchemy_singlestoredb/tests/test_basics.py`
- Run tests with coverage: `pytest --cov=sqlalchemy_singlestoredb sqlalchemy_singlestoredb/tests/`

### Code Quality
- Run pre-commit checks: `pre-commit run --all-files`
- Run linting with flake8: `flake8 sqlalchemy_singlestoredb/`
- Type checking with mypy: `mypy sqlalchemy_singlestoredb/`
- Auto-format with autopep8: `autopep8 --in-place --recursive sqlalchemy_singlestoredb/`

### Package Management
- Install in development mode: `pip install -e .`
- Install test dependencies: `pip install -r test-requirements.txt`
- Build package: `python setup.py sdist bdist_wheel`

### Documentation
- Build docs: `cd docs/src && make html`
- View built docs in `docs/` directory

## Architecture Overview

This is a SQLAlchemy dialect for SingleStoreDB, extending MySQL dialect functionality with SingleStore-specific features.

### Core Components

- **`base.py`**: Main dialect implementation (`SingleStoreDBDialect`) extending `MySQLDialect`
  - Handles connection parameters via `singlestoredb` driver
  - Custom compiler, DDL compiler, type compiler, and identifier preparer
  - Supports SingleStore-specific cast syntax (`:>` operator)

- **`dtypes.py`**: Custom data types
  - `JSON`: Enhanced JSON type with custom deserialization
  - `VECTOR`: Vector type for AI/ML workloads with configurable dimensions and element types

- **`column.py`**: SingleStore-specific column features
  - `PersistedColumn`: Computed columns that are stored (persisted) in the database

- **`ddlelement.py`**: DDL elements for table creation
  - `ShardKey`: Defines sharding strategy for distributed tables
  - `SortKey`: Defines sorting strategy for optimized queries

- **`reflection.py`**: Database introspection utilities extending MySQL reflection

### Key Features

- **Vector Support**: Native VECTOR data type with F16/F32/F64 and integer element types
- **JSON Enhancements**: Improved JSON handling with custom serialization/deserialization
- **Distributed Table Support**: SHARD KEY and SORT KEY for table distribution and optimization
- **Persisted Columns**: Computed columns that are materialized and stored
- **Cast Operator**: SingleStore's `:>` cast syntax for type conversions

### Connection Configuration

Uses the `singlestoredb` Python driver. Connection strings use the format:
```
singlestoredb://user:password@host:port/database
```

The dialect automatically detects SingleStore cloud connections (host='singlestore.com') and adjusts behavior accordingly.

### Testing Setup

Tests require:
- `SINGLESTOREDB_URL` environment variable with connection details
- `singlestoredb.tests.utils` for database setup/teardown
- Test database is created from `test.sql` file

#### Local Development Database
For local testing, use the following connection string:
```bash
export SINGLESTOREDB_URL="singlestoredb://root:@127.0.0.1:9306"
```

This connects to a local SingleStore instance running on the default port 9306 with root user and no password.

#### Running Tests

The comprehensive test runner requires both SINGLESTOREDB_URL and PYTHONPATH to be set:

```bash
# Set required environment variables
export SINGLESTOREDB_URL="singlestoredb://root:@127.0.0.1:9306"
export PYTHONPATH="/path/to/sqlalchemy-singlestoredb:/path/to/singlestoredb-python"

# Run comprehensive test suite
cd sqlalchemy_singlestoredb/tests
python run_all_tests.py

# Run individual test files
python test_priority1_features.py
python test_basics.py  # requires singlestoredb driver
python test_integration_features.py
python test_error_handling.py
python test_sql_compilation.py

# Alternative: Use pytest (may not include all custom tests)
pytest sqlalchemy_singlestoredb/tests/
pytest -v sqlalchemy_singlestoredb/tests/test_basics.py
```

**Note**: The test runner no longer sets PYTHONPATH or SINGLESTOREDB_URL automatically. These must be set by the user in their environment before running tests.

Pre-commit hooks run flake8, autopep8, mypy, and import sorting automatically.
