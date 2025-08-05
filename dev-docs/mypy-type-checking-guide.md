# Mypy Type Checking Guide for SQLAlchemy Dialect Development

This guide covers mypy configuration and best practices for type checking in SQLAlchemy dialect projects.

## Basic Mypy Configuration

### Core Configuration Options

Based on the project's setup.cfg, here are essential mypy settings:

```ini
[mypy]
check_untyped_defs = true
disallow_any_generics = true
disallow_incomplete_defs = true
disallow_untyped_defs = true
no_implicit_optional = true
warn_redundant_casts = true
warn_unused_ignores = true
```

### Per-Module Configuration

Configure different rules for different parts of your codebase:

```ini
# Don't require type annotations for test files
[mypy-tests.*]
disallow_untyped_defs = false

# Ignore missing imports for external libraries
[mypy-external_lib.*]
ignore_missing_imports = true

# Ignore all errors in legacy modules
[mypy-legacy_package.*]
ignore_errors = true
```

## Type Checking Strictness Levels

### Strict Mode Configuration

Enable all optional error checking flags:

```ini
[mypy]
strict = true
```

This is equivalent to enabling:
- `warn_unused_configs`
- `disallow_any_generics`
- `disallow_subclassing_any`
- `disallow_untyped_calls`
- `disallow_untyped_defs`
- `disallow_incomplete_defs`
- `check_untyped_defs`
- `disallow_untyped_decorators`
- `warn_redundant_casts`
- `warn_unused_ignores`
- `warn_return_any`
- `no_implicit_reexport`
- `strict_equality`

### Gradual Typing Approach

For existing codebases, start with basic checks:

```ini
[mypy]
# Start with basic type checking
check_untyped_defs = true
disallow_incomplete_defs = true

# Gradually enable stricter checks
# disallow_untyped_defs = true
# disallow_any_generics = true
```

## Error Handling and Suppression

### Ignoring Specific Errors

Use `# type: ignore` with specific error codes:

```python
import external_library  # type: ignore[import]

def legacy_function():  # type: ignore[no-untyped-def]
    return "untyped"

x: list = []  # type: ignore[type-arg]
```

### File-Level Error Suppression

For files that need to skip type checking entirely:

```python
# mypy: ignore-errors
# This file has complex dynamic typing that's hard to annotate
```

### Disabling Specific Error Codes

```ini
[mypy]
disable_error_code = var-annotated, has-type

# Or per-module
[mypy-tests.*]
disable_error_code = no-untyped-def, no-untyped-call
```

## Type Annotations Best Practices

### Function Type Annotations

```python
from typing import Optional, List, Dict, Any
from sqlalchemy.engine import Engine
from sqlalchemy.sql import ClauseElement

def create_engine_connection(url: str) -> Engine:
    """Create and return a database engine."""
    return create_engine(url)

def compile_clause(clause: ClauseElement, dialect: Optional[str] = None) -> str:
    """Compile a SQL clause to string."""
    if dialect:
        return str(clause.compile(dialect=dialect))
    return str(clause.compile())
```

### Generic Types

```python
from typing import TypeVar, Generic, List, Dict

T = TypeVar('T')

class TypeCompiler(Generic[T]):
    def process(self, type_: T) -> str:
        """Process a type and return SQL representation."""
        return str(type_)

# Usage
compiler: TypeCompiler[int] = TypeCompiler()
result: str = compiler.process(42)
```

### Union Types and Optional

```python
from typing import Union, Optional
from sqlalchemy.sql.elements import ClauseElement

def process_clause(clause: Union[str, ClauseElement]) -> str:
    """Process either a string or a clause element."""
    if isinstance(clause, str):
        return clause
    return str(clause.compile())

def get_table_name(table_name: Optional[str] = None) -> str:
    """Get table name with optional default."""
    if table_name is None:
        return "default_table"
    return table_name
```

## Working with SQLAlchemy Types

### Common SQLAlchemy Type Patterns

```python
from typing import Any, Optional, Type, Dict
from sqlalchemy import Column, Integer, String
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql.compiler import SQLCompiler

class CustomDialect:
    name: str = "custom"
    colspecs: Dict[Type[TypeEngine[Any]], Type[TypeEngine[Any]]] = {}

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def create_connect_args(self, url: Any) -> tuple[list[Any], dict[str, Any]]:
        """Create connection arguments."""
        return [], {}

def create_column(name: str, type_: TypeEngine[Any]) -> Column[Any]:
    """Create a typed column."""
    return Column(name, type_)
```

### Type Checking with Protocol Classes

```python
from typing import Protocol, Any

class DialectProtocol(Protocol):
    name: str

    def create_connect_args(self, url: Any) -> tuple[list[Any], dict[str, Any]]:
        ...

    def initialize(self, connection: Any) -> None:
        ...

def use_dialect(dialect: DialectProtocol) -> str:
    """Function that works with any dialect-like object."""
    return dialect.name
```

## Handling Dynamic Code

### Using `Any` Type

When dealing with highly dynamic code:

```python
from typing import Any

def handle_dynamic_attribute(obj: Any, attr_name: str) -> Any:
    """Handle dynamic attribute access."""
    return getattr(obj, attr_name)

# Better: Use more specific types when possible
def handle_engine_attribute(engine: Engine, attr_name: str) -> Any:
    """Handle engine attribute access with some type safety."""
    if hasattr(engine, attr_name):
        return getattr(engine, attr_name)
    raise AttributeError(f"Engine has no attribute {attr_name}")
```

### Type Guards

```python
from typing import TypeGuard

def is_string_list(val: list[Any]) -> TypeGuard[list[str]]:
    """Check if a list contains only strings."""
    return all(isinstance(item, str) for item in val)

def process_string_list(items: list[Any]) -> list[str]:
    """Process a list that should contain only strings."""
    if is_string_list(items):
        # Type checker knows items is list[str] here
        return [item.upper() for item in items]
    raise ValueError("All items must be strings")
```

## Advanced Mypy Features

### Conditional Imports with TYPE_CHECKING

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.dialects.mysql.base import MySQLDialect
    from sqlalchemy.engine import Engine

def extend_mysql_dialect() -> None:
    """Function that extends MySQL dialect."""
    # Use forward references in annotations
    pass

def create_connection() -> 'Engine':
    """Create a database connection."""
    from sqlalchemy import create_engine
    return create_engine("sqlite:///:memory:")
```

### Overloads for Function Variants

```python
from typing import overload, Optional

@overload
def get_column_type(column: str) -> Optional[str]: ...

@overload
def get_column_type(column: str, default: str) -> str: ...

def get_column_type(column: str, default: Optional[str] = None) -> Optional[str]:
    """Get column type with optional default."""
    # Implementation
    return default
```

## Common Error Codes and Solutions

### Missing Type Arguments (`type-arg`)

```python
# Error: Missing type parameters for generic type "list"
def process_items(items: list) -> None:  # type: ignore[type-arg]
    pass

# Fixed:
def process_items(items: list[str]) -> None:
    pass
```

### Incompatible Types (`assignment`)

```python
# Error: Incompatible types in assignment
x: int = "string"  # type: ignore[assignment]

# Fixed with proper types:
x: str = "string"
```

### Missing Return Statement (`return`)

```python
def get_value(condition: bool) -> str:
    if condition:
        return "yes"
    # Error: Missing return statement
    # Fixed:
    return "no"
```

### Unused Type Ignore (`unused-ignore`)

```python
# Error: unused 'type: ignore' comment
x = 1  # type: ignore[assignment]  # Remove this comment

# Fixed:
x = 1
```

## Testing with Mypy

### Running Mypy

```bash
# Check specific files
mypy sqlalchemy_singlestoredb/base.py

# Check entire package
mypy sqlalchemy_singlestoredb/

# Check with specific configuration
mypy --config-file setup.cfg sqlalchemy_singlestoredb/

# Show error codes
mypy --show-error-codes sqlalchemy_singlestoredb/
```

### CI/CD Integration

```bash
# In your CI pipeline
mypy sqlalchemy_singlestoredb/ --no-error-summary
```

### Pre-commit Hook

```yaml
# .pre-commit-config.yaml
repos:
- repo: https://github.com/pre-commit/mirrors-mypy
  rev: v1.16.0
  hooks:
  - id: mypy
    additional_dependencies: [types-requests]
```

## SQLAlchemy-Specific Type Patterns

### Dialect Development

```python
from typing import Any, Optional, Type
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.sql.compiler import SQLCompiler, DDLCompiler

class SingleStoreDBDialect(MySQLDialect):
    name: str = 'singlestoredb'

    def __init__(
        self,
        isolation_level: Optional[str] = None,
        json_serializer: Optional[callable] = None,
        json_deserializer: Optional[callable] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            isolation_level=isolation_level,
            json_serializer=json_serializer,
            json_deserializer=json_deserializer,
            **kwargs
        )
```

### Custom Types

```python
from typing import Any, Optional, Type
from sqlalchemy.types import TypeDecorator, JSON as SQLAlchemy_JSON

class JSON(TypeDecorator[Any]):
    impl = SQLAlchemy_JSON
    cache_ok = True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.pop('collate', None)
        super().__init__(*args, **kwargs)

    def result_processor(
        self,
        dialect: Any,
        coltype: Any
    ) -> Optional[callable]:
        return self._str_impl.result_processor(dialect, coltype)
```

## Performance Considerations

### Incremental Mode

```ini
[mypy]
incremental = true
cache_dir = .mypy_cache
```

### Parallel Processing

```bash
# Use multiple processes for faster checking
mypy --jobs 4 sqlalchemy_singlestoredb/
```

## Best Practices Summary

1. **Start Gradually**: Begin with basic type checking and gradually increase strictness
2. **Use Specific Types**: Prefer `list[str]` over `list` or `list[Any]`
3. **Handle Optionals**: Use `Optional[T]` or `T | None` for nullable values
4. **Document Ignores**: Always comment why you're using `# type: ignore`
5. **Use Error Codes**: Be specific with `# type: ignore[error-code]`
6. **Test Configuration**: Regularly run mypy to catch regressions
7. **Update Dependencies**: Keep mypy and type stubs updated
8. **Share Configuration**: Use consistent mypy configuration across team

This configuration and approach will help maintain type safety while developing SQLAlchemy dialects with proper error handling and gradual adoption strategies.
