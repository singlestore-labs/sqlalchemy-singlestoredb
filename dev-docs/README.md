# Developer Documentation

This directory contains developer documentation for the SQLAlchemy SingleStoreDB dialect project.

## Documentation Contents

### [SQLAlchemy Dialect Development](./sqlalchemy-dialect-development.md)
Comprehensive guide for developing custom SQLAlchemy dialects, including:
- Creating custom DDL elements and compilation rules
- Extending MySQL dialect for SingleStore-specific features
- Custom type development and type system architecture
- DDL compilation and conditional execution
- Best practices for dialect development

### [Pytest Testing Guide](./pytest-testing-guide.md)
Complete testing framework guide covering:
- Test structure and fixtures
- Parametrization techniques (basic, class-level, module-level)
- Fixture parametrization and custom test IDs
- Advanced parametrization with pytest_generate_tests
- Integration with unittest.TestCase
- Built-in fixtures and configuration
- SQLAlchemy-specific testing patterns

### [Mypy Type Checking Guide](./mypy-type-checking-guide.md)
Type checking configuration and best practices:
- Core mypy configuration options
- Error handling and suppression strategies
- Type annotation patterns for SQLAlchemy
- Handling dynamic code with type safety
- Advanced mypy features (TYPE_CHECKING, overloads)
- Common error codes and solutions
- Performance considerations

### [SingleStore Usage Examples](./singlestore-usage-examples.md)
Real-world SingleStore usage patterns:
- Database connection methods (singlestoredb, SQLAlchemy, mysql.connector)
- Database management operations
- Query patterns (basic, JSON, vector data)
- Advanced queries (joins, aggregations, subqueries)
- Data ingestion (pipelines, CDC)
- Performance analysis and monitoring
- Python integration patterns
- Best practices and security considerations

### [SingleStore Python SDK Reference](./singlestore-python-sdk-reference.md)
Comprehensive Python SDK documentation covering:
- Installation and setup procedures
- Multiple connection methods and patterns
- Database operations and query execution
- Data types and conversions (JSON, VECTOR)
- Vector operations and similarity functions
- Integration patterns (Langchain, MongoDB API, FastAPI)
- Performance optimization techniques
- Error handling and retry logic
- Testing patterns and best practices

## How to Use This Documentation

1. **New to SQLAlchemy Dialects?** Start with the [SQLAlchemy Dialect Development](./sqlalchemy-dialect-development.md) guide
2. **Setting up Testing?** Follow the [Pytest Testing Guide](./pytest-testing-guide.md)
3. **Want Type Safety?** Configure mypy using the [Type Checking Guide](./mypy-type-checking-guide.md)
4. **Working with SingleStore?** Check [Usage Examples](./singlestore-usage-examples.md) for patterns and best practices
5. **Need SDK Reference?** Use the [Python SDK Reference](./singlestore-python-sdk-reference.md) for comprehensive API documentation

## Contributing to Documentation

When updating this documentation:

1. Keep examples concise and focused
2. Include error handling in code examples
3. Reference the actual codebase when possible
4. Update this README when adding new documentation files
5. Follow the established markdown formatting style

## Related Resources

- [Main Project README](../README.md)
- [CLAUDE.md](../CLAUDE.md) - Claude-specific development instructions
- [setup.cfg](../setup.cfg) - Project configuration including mypy settings
- [.pre-commit-config.yaml](../.pre-commit-config.yaml) - Code quality automation
