# Task Completion Checklist

## Required Steps After Code Changes

### 1. Code Quality Checks (MANDATORY)
```bash
# Run pre-commit checks - MUST pass before committing
pre-commit run --all-files
```

The pre-commit hooks include:
- flake8 linting
- autopep8 formatting
- mypy type checking
- import sorting (reorder-python-imports)
- trailing whitespace removal
- various other code quality checks

### 2. Type Checking
```bash
# Ensure type annotations are correct
mypy sqlalchemy_singlestoredb/
```

### 3. Testing
```bash
# Run relevant tests based on changes made
pytest sqlalchemy_singlestoredb/tests/

# For comprehensive testing with coverage
pytest --cov=sqlalchemy_singlestoredb sqlalchemy_singlestoredb/tests/
```

### 4. Manual Testing (if applicable)
- Test database connection with `SINGLESTOREDB_URL`
- Verify SingleStore-specific features work correctly
- Test vector types, JSON handling, cast operators as relevant

## Before Committing
1. All pre-commit hooks must pass
2. Type checking must pass without errors
3. Relevant tests must pass
4. No lint errors or warnings

## CI/CD Considerations
- GitHub Actions likely run the same checks
- Ensure local checks pass to avoid CI failures
