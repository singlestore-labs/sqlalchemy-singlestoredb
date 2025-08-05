# SQLAlchemy Dialect Development Documentation

This documentation covers key concepts for developing custom SQLAlchemy dialects, with focus on extending MySQL dialects and creating custom types and DDL compilation.

## Core Concepts for Dialect Development

### Creating Custom DDL Elements

You can create custom DDL elements by subclassing `DDLElement` and using `sqlalchemy.ext.compiler`:

```python
from sqlalchemy.schema import DDLElement
from sqlalchemy.ext.compiler import compiles

class AlterColumn(DDLElement):
    def __init__(self, column, cmd):
        self.column = column
        self.cmd = cmd

@compiles(AlterColumn)
def visit_alter_column(element, compiler, **kw):
    return "ALTER TABLE %s ALTER COLUMN %s %s ..." % (
        element.column.table.name,
        element.column.name,
        element.cmd,
    )

engine.execute(AlterColumn(table.c.mycolumn, "SET DEFAULT 'test'"))
```

### Dialect-Specific Compilation

Create dialect-specific compilation rules for different database systems:

```python
from sqlalchemy.schema import DDLElement
from sqlalchemy.ext.compiler import compiles

class AlterColumn(DDLElement):
    inherit_cache = False

    def __init__(self, column, cmd):
        self.column = column
        self.cmd = cmd

@compiles(AlterColumn)
def visit_alter_column(element, compiler, **kw):
    return "ALTER COLUMN %s ..." % element.column.name

@compiles(AlterColumn, "postgresql")
def visit_alter_column_postgresql(element, compiler, **kw):
    return "ALTER TABLE %s ALTER COLUMN %s ..." % (
        element.table.name,
        element.column.name,
    )
```

### Custom Type Compilation

Override type compilation for specific database dialects:

```python
from sqlalchemy.types import String, VARCHAR
from sqlalchemy.ext.compiler import compiles

@compiles(String, "mssql")
@compiles(VARCHAR, "mssql")
def compile_varchar(element, compiler, **kw):
    if element.length == "max":
        return "VARCHAR('max')"
    else:
        # Call the default compiler for VARCHAR
        return compiler.visit_VARCHAR(element, **kw)
```

### Extending MySQL Dialect

For MySQL dialect extensions, you can customize foreign key constraints:

```python
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import ForeignKeyConstraint

@compiles(ForeignKeyConstraint, "mysql", "mariadb")
def process(element, compiler, **kw):
    element.deferrable = element.initially = None
    return compiler.visit_foreign_key_constraint(element, **kw)
```

## MySQL Data Types Reference

Common MySQL data types available in SQLAlchemy:

```python
from sqlalchemy.dialects.mysql import (
    BIGINT, BINARY, BIT, BLOB, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL,
    DOUBLE, ENUM, FLOAT, INTEGER, JSON, LONGBLOB, LONGTEXT, MEDIUMBLOB,
    MEDIUMINT, MEDIUMTEXT, NCHAR, NUMERIC, NVARCHAR, REAL, SET, SMALLINT,
    TEXT, TIME, TIMESTAMP, TINYBLOB, TINYINT, TINYTEXT, VARBINARY, VARCHAR, YEAR
)
```

### MySQL-Specific Type Features

- **DATETIME/TIME/TIMESTAMP**: Support fractional seconds precision (fsp parameter)
- **SET**: Requires list of valid values, supports `retrieve_as_bitwise` flag
- **JSON**: Native JSON type support in MySQL 5.7+
- **VARCHAR**: Supports charset, collation, ascii, unicode, binary parameters

## Type System Architecture

SQLAlchemy 2.0 type system features:
- Dialects expose public types as UPPERCASE names
- Internal types use underscore identifiers
- SQL/DDL type expression handled by compiler system
- Reduced number of type objects per dialect

## Custom Type Development

### TypeDecorator for Custom Types

```python
from sqlalchemy.types import String
from sqlalchemy import func

class LowerString(String):
    def bind_expression(self, bindvalue):
        return func.lower(bindvalue)

    def column_expression(self, col):
        return func.lower(col)
```

### Type Caching

Control statement caching with the `cache_ok` attribute:
- `None`: Emits warning and disallows caching
- `False`: Disables caching without warning
- `True`: Uses object's class and state for cache key

## DDL Compilation

### Custom Column Compilation

```python
from sqlalchemy import schema
from sqlalchemy.ext.compiler import compiles

@compiles(schema.CreateColumn)
def compile(element, compiler, **kw):
    column = element.element

    if "special" not in column.info:
        return compiler.visit_create_column(element, **kw)

    text = "%s SPECIAL DIRECTIVE %s" % (
        column.name,
        compiler.type_compiler.process(column.type),
    )
    default = compiler.get_column_default_string(column)
    if default is not None:
        text += " DEFAULT " + default

    if not column.nullable:
        text += " NOT NULL"

    return text
```

### Conditional DDL Execution

```python
from sqlalchemy.schema import DDL

def only_pg_14(ddl_element, target, bind, dialect, **kw):
    return dialect.name == "postgresql" and dialect.server_version_info >= (14,)

my_table = Table(
    "my_table", meta,
    Column("id", Integer, primary_key=True),
    Index("my_pg_index", "data").ddl_if(callable_=only_pg_14),
)
```

## Key Dialect Classes

- **MySQLDialect**: Base MySQL dialect class
- **MySQLCompiler**: SQL statement compiler
- **MySQLDDLCompiler**: DDL statement compiler
- **MySQLTypeCompiler**: Type-to-SQL compiler
- **MySQLIdentifierPreparer**: Identifier quotation and preparation

## Best Practices

1. Always set `inherit_cache = False` for custom DDL elements that can't be cached
2. Use `@compiles` decorator for dialect-specific compilation rules
3. Override `bind_processor` and `result_processor` for custom type handling
4. Implement proper type comparison methods for custom types
5. Use `with_variant()` for cross-dialect type compatibility
6. Test custom dialects with multiple SQLAlchemy versions
