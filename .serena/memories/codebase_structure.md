# Codebase Structure

## Root Directory Structure
```
├── sqlalchemy_singlestoredb/     # Main package directory
├── docs/                         # Documentation
├── examples/                     # Example code
├── .github/                      # GitHub Actions workflows
├── conda.recipe/                 # Conda packaging
├── resources/                    # Assets (logos, etc.)
├── setup.py                      # Package setup (minimal, uses setup.cfg)
├── setup.cfg                     # Main package configuration
├── requirements.txt              # Runtime dependencies
├── test-requirements.txt         # Test dependencies
├── .pre-commit-config.yaml       # Pre-commit hook configuration
└── CLAUDE.md                     # Claude-specific instructions
```

## Main Package Structure (`sqlalchemy_singlestoredb/`)
```
├── __init__.py                   # Package initialization, exports
├── base.py                       # Core dialect implementation
├── dtypes.py                     # Custom data types (JSON, VECTOR)
├── column.py                     # Column features (PersistedColumn)
├── ddlelement.py                 # DDL elements (ShardKey, SortKey)
├── reflection.py                 # Database introspection
├── tests/                        # Test suite
│   ├── __init__.py
│   ├── test_basics.py           # Basic functionality tests
│   └── test.sql                 # Test database schema
└── examples/                     # Usage examples
```

## Key Files Description

### Core Implementation
- **`base.py`**: Contains `SingleStoreDBDialect` class extending MySQL dialect
- **`dtypes.py`**: Custom SingleStore data types (JSON with custom deserializer, VECTOR type)
- **`column.py`**: SingleStore-specific column features like persisted computed columns
- **`ddlelement.py`**: DDL elements for distributed tables (shard keys, sort keys)
- **`reflection.py`**: Database introspection and metadata reflection

### Configuration Files
- **`setup.cfg`**: Main configuration (dependencies, flake8, mypy settings)
- **`.pre-commit-config.yaml`**: Code quality automation
- **`CLAUDE.md`**: Development guidance and common commands

### Testing
- Tests use unittest framework extending `unittest.TestCase`
- Require `SINGLESTOREDB_URL` environment variable
- Test database setup/teardown handled automatically
