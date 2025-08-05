# Suggested Development Commands

## Testing Commands
```bash
# Run all tests
pytest sqlalchemy_singlestoredb/tests/

# Run specific test file
pytest sqlalchemy_singlestoredb/tests/test_basics.py

# Run tests with coverage
pytest --cov=sqlalchemy_singlestoredb sqlalchemy_singlestoredb/tests/
```

## Code Quality Commands
```bash
# Run all pre-commit checks
pre-commit run --all-files

# Run linting with flake8
flake8 sqlalchemy_singlestoredb/

# Type checking with mypy
mypy sqlalchemy_singlestoredb/

# Auto-format with autopep8
autopep8 --in-place --recursive sqlalchemy_singlestoredb/
```

## Package Management Commands
```bash
# Install in development mode
pip install -e .

# Install test dependencies
pip install -r test-requirements.txt

# Build package
python setup.py sdist bdist_wheel
```

## Documentation Commands
```bash
# Build documentation
cd docs/src && make html
# View built docs in docs/ directory
```

## System Commands (Linux)
```bash
# Standard Linux commands available
ls          # List directory contents
cd          # Change directory
grep        # Search text patterns
find        # Find files
git         # Version control
```

## Environment Setup
- Requires `SINGLESTOREDB_URL` environment variable for testing
- Test database created from `test.sql` file automatically
