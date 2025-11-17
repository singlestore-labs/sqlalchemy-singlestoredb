# Pytest Fixtures for SingleStoreDB Tests

This directory contains pytest fixtures that provide isolated test databases and automatic cleanup for SingleStoreDB integration tests.

## Available Fixtures

### `_docker_server`
- **Scope**: session
- **Purpose**: Automatically starts a SingleStoreDB Docker container if `SINGLESTOREDB_URL` is not set
- **Usage**: Automatically used by `base_connection_url`
- **Cleanup**: Automatically stops and removes the Docker container after all tests
- **Note**: This fixture enables running tests without any manual setup

### `base_connection_url`
- **Scope**: session
- **Purpose**: Provides the base SingleStoreDB connection URL from environment or Docker
- **Usage**: Automatically used by other fixtures
- **Connection Methods**:
  - Uses `SINGLESTOREDB_URL` if set (existing server)
  - Auto-starts Docker container if `SINGLESTOREDB_URL` is not set
  - Supports both Data API (HTTP) and Direct (MySQL protocol) connections via `USE_DATA_API`

### `test_database`
- **Scope**: session
- **Purpose**: Creates a shared test database with a random name for the entire test session
- **Usage**: Provides the database name as a string
- **Cleanup**: Automatically drops the database after all tests complete
- **Note**: All tests share the same database for better performance; table isolation is handled via `table_name_prefix`

### `test_engine`
- **Scope**: session
- **Purpose**: Creates a shared SQLAlchemy engine connected to the test database
- **Usage**: Use for SQLAlchemy operations
- **Cleanup**: Automatically disposes of the engine after all tests
- **Note**: Shared across all tests in the session

### `table_name_prefix`
- **Scope**: function
- **Purpose**: Generates a unique random prefix for each test's tables
- **Usage**: Use this prefix when creating table names to ensure test isolation
- **Example**: `table_name = f'{table_name_prefix}users'`
- **Note**: Works with `clean_tables` fixture for automatic cleanup

### `test_connection`
- **Scope**: function
- **Purpose**: Creates a raw singlestoredb connection to the test database
- **Usage**: Use for low-level database operations
- **Cleanup**: Automatically closes the connection

### `clean_tables`
- **Scope**: function
- **Purpose**: Ensures all tables created with the test's `table_name_prefix` are dropped
- **Usage**: Add as a fixture to tests that create tables
- **Cleanup**: Drops only tables matching the test's unique prefix after the test
- **Note**: Requires using `table_name_prefix` when creating tables

### `dialect`
- **Scope**: function
- **Purpose**: Creates a fresh SingleStoreDBDialect instance
- **Usage**: Use for testing dialect-specific functionality

## Usage Examples

### Basic Test with Shared Database

```python
def test_with_database(test_engine):
    """All tests share the same database; table isolation via prefixes."""
    with test_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
```

### Test with Automatic Table Cleanup

```python
def test_create_table(test_engine, table_name_prefix, clean_tables):
    """Tables with the prefix are automatically dropped after the test."""
    metadata = MetaData()
    # Use table_name_prefix to ensure isolation
    table = Table(f'{table_name_prefix}test_table', metadata,
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

The test infrastructure supports three connection methods:

### 1. Docker Auto-Start (Recommended for Development)

No environment variables needed! If Docker is installed and running, the fixtures will automatically:
- Start a SingleStoreDB Docker container
- Create a test database with a random name
- Run all tests
- Clean up tables and drop the database
- Stop and remove the Docker container

```bash
# Just run pytest - Docker will be auto-started
pytest sqlalchemy_singlestoredb/tests/
```

### 2. Using an Existing SingleStoreDB Server

Set the `SINGLESTOREDB_URL` environment variable to use an existing server:

```bash
# For MySQL protocol (direct connection)
export SINGLESTOREDB_URL="singlestoredb://user:password@host:port"
pytest sqlalchemy_singlestoredb/tests/

# For Data API (HTTP connection)
export SINGLESTOREDB_INIT_DB_URL="user:password@host:port"
export SINGLESTOREDB_URL="singlestoredb+http://user:password@host:port"
pytest sqlalchemy_singlestoredb/tests/
```

### 3. Connection Mode Options

Choose between two connection modes using `USE_DATA_API`:

```bash
# Direct connection (MySQL protocol) - default
pytest sqlalchemy_singlestoredb/tests/

# Data API (HTTP) connection
USE_DATA_API=1 pytest sqlalchemy_singlestoredb/tests/
```

### Additional Environment Variables

- **`SINGLESTOREDB_URL`**: Connection URL for existing server (optional if using Docker)
- **`USE_DATA_API`**: Set to `1`, `true`, or `on` to use HTTP Data API instead of MySQL protocol
- **`SINGLESTOREDB_INIT_DB_URL`**: Optional URL for database creation if different from connection URL

### What Happens During Test Execution

The fixtures will automatically:
1. Start Docker container (if `SINGLESTOREDB_URL` not set) OR connect to existing server
2. Create a shared test database with a random name (e.g., `test_sqlalchemy_abc123de`)
3. Connect your tests to this database
4. Provide unique table prefixes per test for isolation
5. Clean up tables after each test
6. Drop the database after all tests complete
7. Stop Docker container (if auto-started)

## Benefits

1. **Test Isolation**: Each test gets a unique table prefix, preventing interference between tests
2. **Performance**: Shared session-scoped database reduces overhead of creating/dropping databases
3. **Automatic Cleanup**: No manual cleanup code needed for tables or database
4. **Docker Auto-Start**: Run tests without any setup - Docker container auto-starts
5. **Flexible Connection**: Supports MySQL protocol, Data API (HTTP), and Docker auto-start
6. **Random Names**: Database and table prefixes use random suffixes to avoid conflicts
7. **Parallel Testing**: Table prefix isolation enables safe parallel test execution
8. **No Manual Setup**: Fixtures handle all database, container, and table management

## Running Tests

```bash
# Run all tests (Docker will auto-start if no SINGLESTOREDB_URL set)
# Logging is enabled by default, showing connection info
pytest sqlalchemy_singlestoredb/tests/

# Run specific test file
pytest sqlalchemy_singlestoredb/tests/test_database_fixtures.py

# Run with Data API (HTTP) instead of MySQL protocol
USE_DATA_API=1 pytest sqlalchemy_singlestoredb/tests/

# Run against an existing server
SINGLESTOREDB_URL="user:password@host:port" pytest sqlalchemy_singlestoredb/tests/

# Run tests in parallel (requires pytest-xdist)
pytest -n auto sqlalchemy_singlestoredb/tests/

# Disable logging output (if you don't want to see connection info)
pytest --log-cli-level=WARNING sqlalchemy_singlestoredb/tests/
```

### Connection Logging (Enabled by Default)

The test fixtures include detailed logging that is **automatically shown** during test runs:
- Which connection method is being used (Docker vs existing server)
- API type (Data API/HTTP vs Direct/MySQL protocol)
- Connection URLs
- Database creation/cleanup operations

This logging is configured in `pyproject.toml` and runs automatically. To disable it, use:

```bash
pytest --log-cli-level=WARNING sqlalchemy_singlestoredb/tests/
```
