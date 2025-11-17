# SingleStore SQLAlchemy Dialect Contributing Guide

Fork this repo and commit your changes to the forked repo.
From there make a Pull Request with your submission keeping the following in mind:

## Pre-commit checks on the clone of this repo

The CI pipeline in this repo runs a bunch of validation checks and code reformatting
with pre-commit checks. If you don't install those checks in your clone of the
repo, the code will likely not pass. To install the pre-commit tool in your
clone run the following from your clone directory. This will force the checks
before you can push.

```
pip3 install pre-commit==3.7.1
pre-commit install
```

The checks run automatically when you attempt to commit, but you can run them
manually as well with the following:
```
pre-commit run --all-files
```

### Running tests

To execute tests, run the following to install the package with development dependencies:
```
pip install -e ".[dev]"
```

This installs the package in editable mode along with all testing dependencies (pytest, pytest-cov, coverage).

#### Test Connection Methods

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

#### Required Test Modes

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

#### Test Output and Logging

The test fixtures include detailed logging that is **enabled by default**, showing:
- Which connection method is being used (Docker vs existing server)
- API type (Data API/HTTP vs Direct/MySQL protocol)
- Connection URLs
- Database creation/cleanup operations

This logging is configured in `pyproject.toml` and appears automatically when running tests. To disable it, use:
```bash
pytest --log-cli-level=WARNING sqlalchemy_singlestoredb/tests
```
