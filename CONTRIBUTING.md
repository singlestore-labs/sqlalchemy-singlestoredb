# SingleStoreDB SQLAlchemy Dialect Contributing Guide

Fork this repo and commit your changes to the forked repo.
From there make a Pull Request with your submission keeping the following in mind:

## Setting up a development environment

Use [uv](https://docs.astral.sh/uv/) to create a virtual environment and install
development dependencies:

```bash
# Create and activate a virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install the package with all development dependencies
uv pip install -e ".[dev]"

# Install pre-commit hooks (required for contributions)
pre-commit install
```

Alternatively, use standard pip:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
```

## Pre-commit checks on the clone of this repo

The CI pipeline in this repo runs a bunch of validation checks and code reformatting
with pre-commit checks. If you don't install those checks in your clone of the
repo, the code will likely not pass. To install the pre-commit tool in your
clone run the following from your clone directory. This will force the checks
before you can push.

```bash
pip install pre-commit==3.7.1
pre-commit install
```

The checks run automatically when you attempt to commit, but you can run them
manually as well with the following:
```bash
pre-commit run --all-files
```

## Running tests

### Prerequisites

Before running tests, ensure you have:
- **Development environment set up** (see above)
- **Docker installed and running** (for automatic test database management)

### Environment Variables

The following environment variables control test behavior:

| Variable | Description |
|----------|-------------|
| `SINGLESTOREDB_URL` | Database connection URL. If not set, a Docker container is started automatically. MySQL format: `user:password@host:3306`. HTTP format: `http://user:password@host:9000` |
| `USE_DATA_API` | Set to `1`, `true`, or `on` to run tests via HTTP Data API instead of MySQL protocol |
| `SINGLESTOREDB_INIT_DB_URL` | MySQL connection URL for setup operations (auto-set in HTTP Data API mode). Used for operations that require MySQL protocol even when testing via HTTP |
| `SINGLESTORE_LICENSE` | Optional license key for Docker container. If not provided, container runs without a license |

### Test Connection Methods

The test infrastructure supports three connection methods:

**1. Docker Auto-Start (Recommended for Development)**

If Docker is installed and running, the test fixtures will automatically start a SingleStoreDB container:
```bash
# Docker container will auto-start, run tests, then stop
pytest -v sqlalchemy_singlestoredb/tests
```

**2. Using an Existing Server**

Set `SINGLESTOREDB_URL` to use an existing SingleStoreDB server instead of Docker:
```bash
# Direct connection using MySQL protocol
SINGLESTOREDB_URL=user:pw@127.0.0.1:3306 pytest -v sqlalchemy_singlestoredb/tests

# Data API connection using HTTP protocol
SINGLESTOREDB_INIT_DB_URL=user:pwd@127.0.0.1:3306 \
    SINGLESTOREDB_URL=http://user:pw@127.0.0.1:8090 \
    pytest -v sqlalchemy_singlestoredb/tests
```

### Required Test Modes

The tests must be run in **two connection modes** to ensure compatibility:

**1. Direct Connection (MySQL Protocol)** - Default mode:
```bash
# With Docker auto-start
pytest -v sqlalchemy_singlestoredb/tests

# With existing server
SINGLESTOREDB_URL=user:pw@host:port pytest -v sqlalchemy_singlestoredb/tests
```

**2. Data API (HTTP Protocol)** - Use `USE_DATA_API=1`:
```bash
# With Docker auto-start (will use HTTP Data API)
USE_DATA_API=1 pytest -v sqlalchemy_singlestoredb/tests

# With existing Data API server
SINGLESTOREDB_INIT_DB_URL=user:pwd@127.0.0.1:3306 \
    SINGLESTOREDB_URL=http://user:pw@host:port \
    pytest -v sqlalchemy_singlestoredb/tests
```

### Testing Best Practices

1. **Test both protocols**: Always run tests with both MySQL protocol and HTTP Data API before submitting:
   ```bash
   pytest -v sqlalchemy_singlestoredb/tests
   USE_DATA_API=1 pytest -v sqlalchemy_singlestoredb/tests
   ```

2. **Run with coverage**: Verify test coverage for new code:
   ```bash
   pytest -v --cov=sqlalchemy_singlestoredb sqlalchemy_singlestoredb/tests
   ```

3. **HTTP mode differences**: Some operations behave differently over HTTP. Tests that require MySQL-specific features will be skipped automatically in HTTP mode.

4. **Test with SQLAlchemy 1.4 and 2.0**: The package supports both SQLAlchemy 1.4 and 2.0. Test with both versions to ensure compatibility:
   ```bash
   # Test with SQLAlchemy 1.4
   uv pip install 'sqlalchemy>=1.4,<2.0'
   pytest -v sqlalchemy_singlestoredb/tests

   # Test with SQLAlchemy 2.0
   uv pip install 'sqlalchemy>=2.0,<3.0'
   pytest -v sqlalchemy_singlestoredb/tests
   ```

### Examples

```bash
# Standard workflow - test both protocols
pytest -v sqlalchemy_singlestoredb/tests
USE_DATA_API=1 pytest -v sqlalchemy_singlestoredb/tests

# Run with coverage
pytest -v --cov=sqlalchemy_singlestoredb sqlalchemy_singlestoredb/tests

# Run single test file
pytest -v sqlalchemy_singlestoredb/tests/test_dialect.py

# Test against specific server (skips Docker)
SINGLESTOREDB_URL=admin:pass@localhost:3306 pytest -v sqlalchemy_singlestoredb/tests

# Debug mode with verbose output
pytest -vv -s sqlalchemy_singlestoredb/tests/test_dialect.py
```

## Version Bumping and Releases

This section documents the process for creating new releases.

### Bumping the Version

Use the `resources/bump_version.py` script to increment the version number
and prepare release notes.

```bash
# Bump patch version (1.2.3 -> 1.2.4)
resources/bump_version.py patch

# Bump minor version (1.2.3 -> 1.3.0)
resources/bump_version.py minor

# Bump major version (1.2.3 -> 2.0.0)
resources/bump_version.py major
```

The script performs the following steps:
1. Reads the current version from `pyproject.toml`
2. Calculates the new version based on bump type
3. Updates version in both `pyproject.toml` and `sqlalchemy_singlestoredb/__init__.py`
4. Generates release notes from git history
5. Opens an editor to customize release notes for `docs/src/whatsnew.rst`
6. Builds the documentation
7. Stages all modified files for commit

### Creating a Release

After version bumping and CI tests pass, use the `resources/create_release.py`
script to create a GitHub release.

```bash
# Create release using version from pyproject.toml
resources/create_release.py

# Preview without creating (dry run)
resources/create_release.py --dry-run
```

The script:
1. Checks prerequisites (gh CLI installed, authenticated with GitHub)
2. Extracts version from `pyproject.toml`
3. Reads release notes from `docs/src/whatsnew.rst`
4. Creates a GitHub release with tag `v<version>`

**Prerequisites**: The GitHub CLI (`gh`) must be installed and authenticated.
Install from https://cli.github.com/ and run `gh auth login`.

### Complete Release Workflow

1. **Bump the version**:
   ```bash
   resources/bump_version.py patch
   ```

2. **Review and edit release notes** in the editor that opens

3. **Commit and push**:
   ```bash
   git commit -m "Prepare for vX.X.X release" && git push
   ```

4. **Wait for CI tests to pass** on GitHub Actions

5. **Run final testing**: Execute the full
   [Coverage tests](https://github.com/singlestore-labs/sqlalchemy-singlestoredb/actions/workflows/coverage.yml)
   and [Smoke tests](https://github.com/singlestore-labs/sqlalchemy-singlestoredb/actions/workflows/smoke-test.yml)
   which verify the code works at all advertised Python versions.

6. **Create the GitHub release**:
   ```bash
   resources/create_release.py
   ```

7. **Verify PyPI publish** workflow completes successfully
   (triggered automatically by the GitHub release). Check the
   [PyPI package page](https://pypi.org/project/sqlalchemy-singlestoredb/) to confirm
   the new version is available.

### Version File Locations

Version numbers are stored in two locations that must stay in sync:
- `pyproject.toml`: Package metadata version
- `sqlalchemy_singlestoredb/__init__.py`: `__version__` variable

The `bump_version.py` script handles updating both files automatically.
Never edit these manually unless you update both locations.
