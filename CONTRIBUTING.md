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

The tests must be run in multiple modes: standard protocol and Data API (HTTP).
If you have Docker installed, you can simply run the following:
```
pytest -v sqlalchemy_singlestoredb/tests
USE_DATA_API=1 pytest -v sqlalchemy_singlestoredb/tests
```

You can also specify a SingleStoreDB server if you need to run against a
specific server type:
```
SINGLESTOREDB_URL=user:pw@127.0.0.1:3306 pytest -v sqlalchemy_singlestoredb/tests
SINGLESTOREDB_URL=http://user:pw@127.0.0.1:8090 pytest -v sqlalchemy_singlestoredb/tests
```
