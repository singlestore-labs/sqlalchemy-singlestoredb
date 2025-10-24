# SingleStoreDB SQLAlchemy Dialect - Development Tasks

This document outlines the tasks needed to bring the SingleStoreDB SQLAlchemy dialect up to date with the latest SQLAlchemy 2.0+ features and MySQL dialect capabilities.

## Analysis Summary

**Current State:**
- Based on SQLAlchemy 1.4+ with MySQL dialect inheritance
- Version: 1.1.2
- Basic SingleStore-specific features: VECTOR data type, SHARD KEY, SORT KEY, JSON enhancements
- Limited feature coverage compared to modern MySQL dialect

**Target State:**
- Dual compatibility with SQLAlchemy 1.4+ and 2.0+
- Comprehensive feature parity with MySQL dialect
- SingleStore-specific enhancements and optimizations
- Modern testing and development infrastructure

---

## Priority 1: Critical Features & Compatibility

### 1.1 SQLAlchemy Dual Compatibility (1.4+ and 2.0+)
- [x] **Maintain SQLAlchemy 1.4+ compatibility**
  - [x] Keep current `setup.cfg` requirement: `sqlalchemy>=1.4.0,<3.0.0dev`
  - [x] Ensure all existing 1.4 patterns continue to work
  - [x] Test extensively with SQLAlchemy 1.4.x versions

- [x] **Add SQLAlchemy 2.0+ compatibility**
  - [x] Add conditional imports for SQLAlchemy 2.0 API changes
  - [x] Implement version-aware code paths where needed
  - [x] Use `sqlalchemy.util.compat` utilities for version detection
  - [x] Test compatibility with SQLAlchemy 2.0+ engine interface

- [x] **Python version support (maintain 3.9+)**
  - [x] Keep minimum Python version at 3.9+ as specified
  - [x] Add support for Python 3.11+ and 3.12+
  - [x] Update type annotations for modern Python while maintaining 3.9 compatibility

### 1.2 Statement Caching Support (Version-Aware)
- [x] **Enable statement caching with version detection**
  - [x] Set `supports_statement_cache = True` in dialect for SQLAlchemy 1.4+
  - [x] Implement proper cache key generation for custom types (1.4 and 2.0 compatible)
  - [x] Add cache key support to VECTOR and custom DDL elements
  - [x] Test caching behavior with complex queries across both versions
  - [x] Handle version differences in caching mechanism

### 1.3 Server-Side Cursors / Stream Results
- [x] **Implement streaming support**
  - [x] Add `supports_server_side_cursors` flag
  - [x] Implement `stream_results` execution option support
  - [x] Add configuration for server-side cursor behavior
  - [x] Test with large result sets

### 1.4 Connection Pool Enhancements
- [x] **Modern connection management**
  - [x] Implement `connection.ping()` method for pre-ping
  - [x] Add soft invalidation support
  - [x] Implement proper connection lifecycle events
  - [x] Add connection pool optimization for SingleStore

---

## Priority 2: MySQL Dialect Feature Parity

### 2.1 INSERT Enhancements (Cross-Version Compatible)
- [ ] **ON DUPLICATE KEY UPDATE support**
  - [ ] Create `SingleStoreDBInsert` class extending `Insert` (compatible with both 1.4 and 2.0)
  - [ ] Implement `on_duplicate_key_update()` method
  - [ ] Add `inserted` namespace for VALUES() function
  - [ ] Handle version differences in INSERT construct inheritance
  - [ ] Add comprehensive tests for INSERT ... ON DUPLICATE KEY UPDATE across versions

- [ ] **Multi-value INSERT optimization (version-aware)**
  - [ ] Enable `use_insertmanyvalues = True` for SQLAlchemy 2.0+
  - [ ] Implement fallback bulk insert optimization for 1.4
  - [ ] Add `insert_executemany_returning` support where available
  - [ ] Optimize batch insert performance across versions

### 2.2 UPDATE/DELETE Enhancements
- [ ] **LIMIT clause support**
  - [ ] Add `with_dialect_options()` support for UPDATE/DELETE LIMIT
  - [ ] Implement MySQL-style LIMIT syntax for UPDATE/DELETE
  - [ ] Add tests for limited UPDATE/DELETE operations

- [ ] **RETURNING clause support (if supported by SingleStore)**
  - [ ] Investigate SingleStore support for RETURNING
  - [ ] Implement `update_returning` if supported
  - [ ] Add `update_executemany_returning` if supported

### 2.3 JSON Enhancements
- [ ] **Modern JSON support**
  - [ ] Enhance JSON type with MySQL 5.7+ features
  - [ ] Add JSON path expressions support
  - [ ] Implement JSON_EXTRACT optimizations
  - [ ] Add JSON indexing support if available in SingleStore

### 2.4 Full-Text Search
- [ ] **MATCH ... AGAINST support**
  - [ ] Create `match()` construct for full-text search
  - [ ] Implement `in_boolean_mode()` modifier
  - [ ] Implement `in_natural_language_mode()` modifier
  - [ ] Implement `with_query_expansion()` modifier
  - [ ] Add FULLTEXT index support in DDL

---

## Priority 3: SingleStore-Specific Features

### 3.1 Vector Enhancements
- [ ] **Extended VECTOR support**
  - [ ] Add comprehensive vector similarity functions (DOT_PRODUCT, EUCLIDEAN_DISTANCE, etc.)
  - [ ] Implement vector indexing options
  - [ ] Add vector aggregation functions
  - [ ] Optimize vector data serialization/deserialization
  - [ ] Add support for different vector element types (F16, F32, F64, I8, I16, I32, I64)

### 3.2 Distributed Features
- [ ] **Enhanced SHARD KEY support**
  - [ ] Add validation for SHARD KEY syntax
  - [ ] Support multiple column SHARD KEYs
  - [ ] Add SHARD KEY modification support
  - [ ] Implement SHARD KEY reflection improvements

- [ ] **Enhanced SORT KEY support**
  - [ ] Add SORT KEY validation
  - [ ] Support multiple column SORT KEYs
  - [ ] Implement SORT KEY reflection
  - [ ] Add SORT KEY modification support

### 3.3 Computed Columns Enhancement
- [ ] **PersistedColumn improvements**
  - [ ] Add comprehensive computed column syntax support
  - [ ] Implement computed column reflection
  - [ ] Add validation for computed expressions
  - [ ] Support for virtual vs stored computed columns

### 3.4 SingleStore-Specific Data Types
- [ ] **Additional data types**
  - [ ] Investigate and add any SingleStore-specific types
  - [ ] Add geospatial types if supported
  - [ ] Add time series types if available
  - [ ] Add UUID type support if native

---

## Priority 4: Advanced Features

### 4.1 Index and Constraint Support
- [ ] **Modern index features**
  - [ ] Add FULLTEXT index support
  - [ ] Implement index hints (USE INDEX, FORCE INDEX, IGNORE INDEX)
  - [ ] Add partial index support if available
  - [ ] Implement covering indexes

- [ ] **Constraint enhancements**
  - [ ] Add foreign key constraint support (if available in SingleStore)
  - [ ] Implement check constraint support
  - [ ] Add constraint comment support
  - [ ] Improve constraint reflection

### 4.2 DDL Enhancements
- [ ] **Table options**
  - [ ] Add comprehensive ENGINE options
  - [ ] Implement CHARACTER SET and COLLATION support
  - [ ] Add table comment support
  - [ ] Support for table-level options (ROW_FORMAT, etc.)

- [ ] **Schema operations**
  - [ ] Implement ALTER TABLE enhancements
  - [ ] Add partition support if available
  - [ ] Implement table copying/cloning operations

### 4.3 Query Optimization Features
- [ ] **Hint support**
  - [ ] Implement query hints via `with_hint()`
  - [ ] Add optimizer hints support
  - [ ] Implement join hints
  - [ ] Add index hints for queries

### 4.4 Transaction Features
- [ ] **Isolation levels**
  - [ ] Add AUTOCOMMIT isolation level support
  - [ ] Implement transaction savepoints
  - [ ] Add read-only transaction support

---

## Priority 5: Testing and Quality

### 5.1 Comprehensive Test Suite
- [ ] **Expand test coverage**
  - [ ] Add tests for all MySQL dialect features
  - [ ] Create performance benchmarks
  - [ ] Add integration tests with real SingleStore clusters
  - [ ] Implement test matrix for different SingleStore versions

- [ ] **Specific test areas**
  - [ ] Vector operations and similarity functions
  - [ ] SHARD KEY and SORT KEY functionality
  - [ ] JSON operations and indexing
  - [ ] Full-text search capabilities
  - [ ] Connection pooling and lifecycle
  - [ ] Error handling and recovery

### 5.2 Performance Testing
- [ ] **Benchmarks**
  - [ ] Bulk insert performance
  - [ ] Vector similarity search performance
  - [ ] Connection pool efficiency
  - [ ] Query compilation caching

### 5.3 Compatibility Testing
- [ ] **Multi-version support**
  - [ ] Test with different SingleStore versions (8.0+, 8.1+, 8.5+)
  - [ ] Test with SQLAlchemy 1.4.x versions (1.4.0, 1.4.latest)
  - [ ] Test with SQLAlchemy 2.0+ versions (2.0.0, 2.0.latest)
  - [ ] Test with Python versions (3.9, 3.10, 3.11, 3.12+)
  - [ ] Test with different drivers (if multiple available)
  - [ ] Create CI matrix for version combinations

---

## Priority 6: Documentation and Development

### 6.1 Documentation Updates
- [ ] **API documentation**
  - [ ] Update docstrings for all public APIs with version compatibility notes
  - [ ] Add examples for SingleStore-specific features (1.4 and 2.0 compatible)
  - [ ] Create compatibility guide for SQLAlchemy 1.4 vs 2.0 usage
  - [ ] Add performance tuning guide with version-specific recommendations

- [ ] **User guides**
  - [ ] Vector database usage patterns
  - [ ] Distributed table design best practices
  - [ ] JSON query optimization
  - [ ] Connection configuration guide

### 6.2 Development Infrastructure
- [ ] **Modern tooling**
  - [ ] Update pre-commit hooks
  - [ ] Add GitHub Actions CI/CD
  - [ ] Implement automated testing with SingleStore
  - [ ] Add code coverage reporting

- [ ] **Code quality**
  - [ ] Update mypy configuration for dual SQLAlchemy version support
  - [ ] Add comprehensive type annotations compatible with both 1.4 and 2.0
  - [ ] Implement linting improvements with version-aware rules
  - [ ] Add security scanning

---

## Implementation Guidelines

### Code Organization
1. **Maintain dual compatibility** with SQLAlchemy 1.4+ and 2.0+ simultaneously
2. **Use version detection** to implement appropriate code paths
3. **Follow SQLAlchemy patterns** for all new features across both versions
4. **Use composition over inheritance** for complex features
5. **Implement proper error handling** with informative messages
6. **Avoid breaking changes** that would affect 1.4 users

### Testing Strategy
1. **Test-driven development** for new features with dual version support
2. **Integration tests** with real SingleStore instances across versions
3. **Version matrix testing** for SQLAlchemy 1.4.x and 2.0+ compatibility
4. **Performance regression testing** for critical paths on both versions
5. **Cross-platform testing** (Linux, macOS, Windows)
6. **Automated CI/CD** with multiple version combinations

### Performance Considerations
1. **Statement caching** for all custom constructs
2. **Connection pooling** optimization
3. **Bulk operation** efficiency
4. **Vector operation** optimization

### Documentation Standards
1. **Comprehensive docstrings** with examples for both SQLAlchemy versions
2. **Type annotations** for all public APIs (1.4 and 2.0 compatible)
3. **Version compatibility notes** in all documentation
4. **Performance notes** for optimization features across versions
5. **Migration examples** showing 1.4 vs 2.0 usage patterns

---

## Estimated Timeline

- **Phase 1** (Priority 1): 2-3 months - Core compatibility and critical features
- **Phase 2** (Priority 2): 2-3 months - MySQL feature parity
- **Phase 3** (Priority 3): 1-2 months - SingleStore-specific enhancements
- **Phase 4** (Priority 4): 2-3 months - Advanced features
- **Phase 5** (Priority 5): 1-2 months - Testing and quality assurance
- **Phase 6** (Priority 6): 1 month - Documentation and tooling

**Total estimated timeline: 9-14 months** for complete implementation

---

## Success Criteria

1. ✅ **Dual SQLAlchemy compatibility** (1.4+ and 2.0+) with no deprecation warnings
2. ✅ **Feature parity** with MySQL dialect for applicable features across both versions
3. ✅ **SingleStore-specific features** work seamlessly with SQLAlchemy ORM on both versions
4. ✅ **Performance improvements** over current implementation
5. ✅ **Comprehensive test coverage** (>90%) with real database testing across versions
6. ✅ **Production readiness** with proper error handling and documentation
7. ✅ **Backward compatibility** maintained for existing 1.4 users

This roadmap provides a clear path to modernize the SingleStoreDB SQLAlchemy dialect while maintaining backward compatibility with SQLAlchemy 1.4+ and adding comprehensive MySQL dialect compatibility across both major SQLAlchemy versions.

## Key Implementation Strategy for Dual Compatibility

To maintain compatibility with both SQLAlchemy 1.4+ and 2.0+, the implementation will use:

1. **Version Detection Patterns**:
   ```python
   import sqlalchemy
   from sqlalchemy.util import compat

   # Use version checks for conditional functionality
   SQLALCHEMY_2_0 = sqlalchemy.__version__.startswith('2.')
   ```

2. **Conditional Imports**:
   ```python
   try:
       from sqlalchemy.sql.expression import Insert as SQLAlchemyInsert  # 2.0+
   except ImportError:
       from sqlalchemy.sql.dml import Insert as SQLAlchemyInsert  # 1.4
   ```

3. **Feature Flags with Fallbacks**:
   ```python
   # Enable modern features where available
   if hasattr(self, 'use_insertmanyvalues'):
       self.use_insertmanyvalues = True  # SQLAlchemy 2.0+
   ```

4. **Testing Matrix**:
   - CI/CD pipelines will test against both SQLAlchemy 1.4.x and 2.0+ versions
   - Separate test suites for version-specific functionality
   - Performance benchmarks across both versions
