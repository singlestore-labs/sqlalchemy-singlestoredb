# Pytest Fixtures for SingleStoreDB Tests

This directory contains pytest fixtures that provide isolated test databases and automatic cleanup for SingleStoreDB integration tests.

## Available Fixtures

### `base_connection_url`
- **Scope**: session
- **Purpose**: Provides the base SingleStoreDB connection URL
- **Usage**: Automatically used by other fixtures

### `test_database`
- **Scope**: function
- **Purpose**: Creates a test database with a random name for each test
- **Usage**: Provides the database name as a string
- **Cleanup**: Automatically drops the database after the test

### `test_engine`
- **Scope**: function
- **Purpose**: Creates a SQLAlchemy engine connected to the test database
- **Usage**: Use for SQLAlchemy operations
- **Cleanup**: Automatically disposes of the engine

### `test_connection`
- **Scope**: function
- **Purpose**: Creates a raw singlestoredb connection to the test database
- **Usage**: Use for low-level database operations
- **Cleanup**: Automatically closes the connection

### `clean_tables`
- **Scope**: function
- **Purpose**: Ensures all tables created during the test are dropped
- **Usage**: Add as a fixture to tests that create tables
- **Cleanup**: Drops all tables in the test database after the test

### `dialect`
- **Scope**: function
- **Purpose**: Creates a fresh SingleStoreDBDialect instance
- **Usage**: Use for testing dialect-specific functionality

## Usage Examples

### Basic Test with Isolated Database

```python
def test_with_database(test_engine):
    """Each test gets its own database."""
    with test_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
```

### Test with Automatic Table Cleanup

```python
def test_create_table(test_engine, clean_tables):
    """Tables are automatically dropped after the test."""
    metadata = MetaData()
    table = Table('test_table', metadata,
                  Column('id', Integer, primary_key=True))

    metadata.create_all(test_engine)
    # Table will be automatically dropped after test
```

### Test with Raw Connection

```python
def test_raw_connection(test_connection):
    """Use raw singlestoredb connection."""
    cursor = test_connection.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    assert result[0] == 1
```

## Environment Setup

Before running tests, set the `SINGLESTOREDB_URL` environment variable:

```bash
export SINGLESTOREDB_URL="singlestoredb://user:password@host:port"
```

The fixtures will automatically:
1. Create a test database with a random name (e.g., `test_sqlalchemy_abc123de`)
2. Connect your tests to this isolated database
3. Clean up all tables and drop the database after each test

## Benefits

1. **Test Isolation**: Each test runs in its own database, preventing interference
2. **Automatic Cleanup**: No manual cleanup code needed
3. **Random Names**: Database names include random suffixes to avoid conflicts
4. **Parallel Testing**: Tests can run in parallel without conflicts
5. **No Manual Setup**: Fixtures handle all database creation and teardown

## Running Tests

```bash
# Run all tests with pytest
pytest sqlalchemy_singlestoredb/tests/

# Run specific test file
pytest sqlalchemy_singlestoredb/tests/test_database_fixtures.py

# Run with verbose output
pytest -v sqlalchemy_singlestoredb/tests/

# Run tests in parallel (requires pytest-xdist)
pytest -n auto sqlalchemy_singlestoredb/tests/
```

## Migration from unittest

To migrate existing unittest tests to use these fixtures:

1. Change test classes to not inherit from `unittest.TestCase`
2. Replace `setUp()` with fixture parameters
3. Replace `self.assert*` with `assert` statements
4. Use `pytest.skip()` instead of `unittest.skip()`
5. Add appropriate fixtures as function parameters

Example migration:

```python
# Old unittest style
class TestFeature(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(get_connection_url())
        self.connection = self.engine.connect()

    def tearDown(self):
        self.connection.close()
        self.engine.dispose()

    def test_something(self):
        result = self.connection.execute(text("SELECT 1"))
        self.assertEqual(result.scalar(), 1)

# New pytest style
class TestFeature:
    def test_something(self, test_engine):
        with test_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
```
