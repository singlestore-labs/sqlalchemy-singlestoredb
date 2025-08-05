# Code Style and Conventions

## General Style
- **Line Length**: 90 characters max (configured in setup.cfg)
- **Code Style**: Follows PEP 8 with autopep8 for formatting
- **Import Style**: Uses `from __future__ import annotations` at the top of files
- **Quotes**: Double quotes preferred (enforced by pre-commit hook)

## Type Annotations
- **Type Hints**: Required for all function parameters and return types
- **Mypy Configuration**: Strict typing enabled with:
  - `check_untyped_defs = true`
  - `disallow_any_generics = true`
  - `disallow_incomplete_defs = true`
  - `disallow_untyped_defs = true`
  - Exceptions for test files (typing can be less strict)

## Docstrings
- Classes and methods use triple-quoted docstrings
- Example from codebase: `"""SingleStoreDB SQLAlchemy dialect."""`
- Detailed docstrings for complex methods include parameter descriptions and return types

## Naming Conventions
- **Classes**: PascalCase (e.g., `SingleStoreDBDialect`, `SingleStoreDBCompiler`)
- **Methods/Functions**: snake_case (e.g., `create_connect_args`, `_extract_error_code`)
- **Variables**: snake_case (e.g., `default_paramstyle`, `supports_sane_rowcount`)
- **Private Methods**: Leading underscore (e.g., `_extract_error_code`, `_tabledef_parser`)
- **Constants**: UPPER_SNAKE_CASE

## Code Organization
- Imports organized by pre-commit hook (reorder-python-imports)
- Classes extend appropriate SQLAlchemy base classes
- Methods follow SQLAlchemy dialect patterns and naming
- Custom functionality clearly separated from inherited behavior
