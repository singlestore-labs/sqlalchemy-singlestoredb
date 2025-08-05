# SQLAlchemy SingleStoreDB Dialect - Project Overview

## Purpose
This project is a SQLAlchemy dialect for SingleStoreDB, extending MySQL dialect functionality with SingleStore-specific features. It allows developers to use SQLAlchemy's ORM and Core APIs with SingleStoreDB databases.

## Tech Stack
- **Language**: Python 3.9+
- **Main Dependencies**:
  - `sqlalchemy>=1.4.0,<3.0.0dev` - The core SQLAlchemy library
  - `singlestoredb>=1.0.0` - SingleStoreDB Python driver
- **Testing**: pytest, pytest-cov, coverage
- **Code Quality**: flake8, mypy, autopep8, pre-commit hooks

## Key Features
- **Vector Support**: Native VECTOR data type with F16/F32/F64 and integer element types
- **JSON Enhancements**: Improved JSON handling with custom serialization/deserialization
- **Distributed Table Support**: SHARD KEY and SORT KEY for table distribution and optimization
- **Persisted Columns**: Computed columns that are materialized and stored
- **Cast Operator**: SingleStore's `:>` cast syntax for type conversions

## Core Components
- `base.py`: Main dialect implementation (`SingleStoreDBDialect`) extending `MySQLDialect`
- `dtypes.py`: Custom data types (JSON, VECTOR)
- `column.py`: SingleStore-specific column features (PersistedColumn)
- `ddlelement.py`: DDL elements (ShardKey, SortKey)
- `reflection.py`: Database introspection utilities

## Connection Configuration
Uses `singlestoredb://user:password@host:port/database` connection strings with the singlestoredb Python driver.
