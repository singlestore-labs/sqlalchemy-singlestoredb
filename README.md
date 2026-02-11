# <img src="https://raw.githubusercontent.com/singlestore-labs/singlestoredb-python/main/resources/singlestore-logo.png" height="60" valign="middle"/> SingleStoreDB SQLAlchemy Dialect

This project contains a [SQLAlchemy](https://sqlalchemy.org) dialect which allows
you to use the SQLAlchemy APIs with the [SingleStoreDB](https://singlestore.com) database.

## Features

- **SingleStore-specific data types**: VECTOR for AI/ML embeddings, JSON for semi-structured data
- **Computed columns**: PersistedColumn for server-side computed values
- **Data distribution**: ShardKey for optimal data partitioning across nodes
- **Query optimization**: SortKey for improved query performance
- **Vector similarity search**: VectorKey for AI/ML similarity operations
- **Full-text search**: FullTextIndex for text search capabilities
- **JSON array indexing**: MultiValueIndex for efficient JSON array queries
- **Table types**: ColumnStore (analytical) and RowStore (transactional) with reference table support
- **Wide table optimization**: ColumnGroup for tables with many columns

## Install

This package can be installed from PyPI using `pip`:
```bash
pip install sqlalchemy-singlestoredb
```

## Quick Start

### Connecting to SingleStoreDB

```python
from sqlalchemy import create_engine, text

# Basic connection
engine = create_engine('singlestoredb://user:password@host:3306/database')

# Test the connection
with engine.connect() as conn:
    result = conn.execute(text('SELECT 1'))
    print(result.fetchone())
```

### Creating Tables (Core API)

```python
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table
from sqlalchemy_singlestoredb import ShardKey, SortKey, VECTOR, JSON

metadata = MetaData()

users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(100)),
    Column('email', String(255)),
    Column('created_at', DateTime),
    singlestoredb_shard_key=ShardKey('id'),
    singlestoredb_sort_key=SortKey('created_at'),
)

metadata.create_all(engine)
```

### Creating Tables (ORM)

```python
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base
from sqlalchemy_singlestoredb import ShardKey, SortKey

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(255))
    created_at = Column(DateTime)

    __table_args__ = {
        'singlestoredb_shard_key': ShardKey('id'),
        'singlestoredb_sort_key': SortKey('created_at'),
    }

Base.metadata.create_all(engine)
```

## Key SingleStore Features

### Vector Search for AI/ML

SingleStoreDB supports vector similarity search for AI/ML applications:

```python
from sqlalchemy import Column, Integer, String, func, select
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy_singlestoredb import VECTOR, VectorKey, ColumnStore

Base = declarative_base()

class Document(Base):
    __tablename__ = 'documents'

    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    embedding = Column(VECTOR(1536))

    __table_args__ = {
        'singlestoredb_table_type': ColumnStore(),
        'singlestoredb_vector_keys': [
            VectorKey('embedding', index_options={'metric_type': 'COSINE_SIMILARITY'}),
        ],
    }

# Find similar documents
query_embedding = [0.1, 0.2, ...]  # Your embedding vector
with Session(engine) as session:
    stmt = (
        select(Document)
        .order_by(func.dot_product(Document.embedding, query_embedding).desc())
        .limit(10)
    )
    similar_docs = session.execute(stmt).scalars().all()
```

### Data Distribution with Shard Keys

Shard keys control how data is distributed across nodes:

```python
from sqlalchemy_singlestoredb import ShardKey

__table_args__ = {
    # Single column shard key
    'singlestoredb_shard_key': ShardKey('user_id'),

    # Multi-column shard key
    'singlestoredb_shard_key': ShardKey('user_id', 'region_id'),

    # Empty shard key for random distribution
    'singlestoredb_shard_key': ShardKey(),
}
```

### Table Types

Choose the right table type for your workload:

```python
from sqlalchemy_singlestoredb import ColumnStore, RowStore

# ColumnStore (default) - Best for analytical workloads
__table_args__ = {'singlestoredb_table_type': ColumnStore()}

# RowStore - Best for transactional workloads
__table_args__ = {'singlestoredb_table_type': RowStore()}

# Reference table - Replicated to all nodes for fast joins
__table_args__ = {'singlestoredb_table_type': RowStore(reference=True)}
```

## License

This library is licensed under the [Apache 2.0 License](https://raw.githubusercontent.com/singlestore-labs/singlestoredb-python/main/LICENSE?token=GHSAT0AAAAAABMGV6QPNR6N23BVICDYK5LAYTVK5EA).

## Resources

* [Documentation](https://sqlalchemy-singlestoredb.labs.singlestore.com)
* [SingleStore](https://www.singlestore.com)
* [SQLAlchemy](https://sqlalchemy.org)
