.. currentmodule:: sqlalchemy_singlestoredb

Getting Started
===============

This guide covers the basics of using SQLAlchemy with SingleStoreDB.

Connecting to SingleStoreDB
---------------------------

Connections to SingleStoreDB are made using URLs just like any other
SQLAlchemy dialect.

.. code-block:: python

    from sqlalchemy import create_engine

    # Basic connection
    engine = create_engine('singlestoredb://user:password@host:3306/database')

    # With additional options
    engine = create_engine(
        'singlestoredb://user:password@host:3306/database',
        pool_size=5,
        pool_recycle=3600,
    )

    # Test the connection
    with engine.connect() as conn:
        result = conn.execute(text('SELECT 1'))
        print(result.fetchone())

More examples of connection usage can be found at the
`SQLAlchemy <https://docs.sqlalchemy.org/en/20/index.html>`_ site.


Creating Tables (Core API)
--------------------------

Use SQLAlchemy's Table construct with SingleStoreDB-specific options:

.. code-block:: python

    from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, text
    from sqlalchemy_singlestoredb import (
        ColumnStore,
        JSON,
        MultiValueIndex,
        ShardKey,
        SortKey,
        VECTOR,
        VectorKey,
    )

    metadata = MetaData()

    # Basic table with shard key and sort key
    users = Table(
        'users', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(100)),
        Column('email', String(255)),
        Column('created_at', DateTime),
        singlestoredb_shard_key=ShardKey('id'),
        singlestoredb_sort_key=SortKey('created_at'),
    )

    # Table with vector embeddings for AI/ML
    documents = Table(
        'documents', metadata,
        Column('id', Integer, primary_key=True),
        Column('title', String(200)),
        Column('embedding', VECTOR(1536)),
        Column('tags', JSON),
        singlestoredb_table_type=ColumnStore(),
        singlestoredb_shard_key=ShardKey('id'),
        singlestoredb_vector_keys=[
            VectorKey('embedding', index_options={'metric_type': 'COSINE_SIMILARITY'}),
        ],
        singlestoredb_multi_value_indexes=[MultiValueIndex('tags')],
    )

    # Create all tables
    metadata.create_all(engine)


Creating Tables (ORM)
---------------------

Use SQLAlchemy's declarative ORM with ``__table_args__``:

.. code-block:: python

    from sqlalchemy import Column, DateTime, Integer, String
    from sqlalchemy.orm import declarative_base
    from sqlalchemy_singlestoredb import (
        ColumnStore,
        JSON,
        MultiValueIndex,
        ShardKey,
        SortKey,
        VECTOR,
        VectorKey,
    )

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

    class Document(Base):
        __tablename__ = 'documents'

        id = Column(Integer, primary_key=True)
        title = Column(String(200))
        embedding = Column(VECTOR(1536))
        tags = Column(JSON)

        __table_args__ = {
            'singlestoredb_table_type': ColumnStore(),
            'singlestoredb_shard_key': ShardKey('id'),
            'singlestoredb_vector_keys': [
                VectorKey('embedding', index_options={'metric_type': 'COSINE_SIMILARITY'}),
            ],
            'singlestoredb_multi_value_indexes': [MultiValueIndex('tags')],
        }

    # Create all tables
    Base.metadata.create_all(engine)


Basic CRUD Operations
---------------------

**Insert data:**

.. code-block:: python

    from sqlalchemy.orm import Session

    with Session(engine) as session:
        user = User(name='Alice', email='alice@example.com')
        session.add(user)
        session.commit()

**Query data:**

.. code-block:: python

    with Session(engine) as session:
        # Get all users
        users = session.query(User).all()

        # Filter by condition
        user = session.query(User).filter(User.email == 'alice@example.com').first()

        # Using select() (SQLAlchemy 2.0 style)
        from sqlalchemy import select
        stmt = select(User).where(User.name == 'Alice')
        result = session.execute(stmt).scalars().first()

**Update data:**

.. code-block:: python

    with Session(engine) as session:
        user = session.query(User).filter(User.id == 1).first()
        user.name = 'Alice Smith'
        session.commit()

**Delete data:**

.. code-block:: python

    with Session(engine) as session:
        user = session.query(User).filter(User.id == 1).first()
        session.delete(user)
        session.commit()


Vector Similarity Search
------------------------

SingleStoreDB supports vector similarity search for AI/ML applications:

.. code-block:: python

    from sqlalchemy import func, select

    # Assuming you have a query embedding from your ML model
    query_embedding = [0.1, 0.2, ...]  # 1536-dimension vector

    with Session(engine) as session:
        # Find similar documents using dot product
        stmt = (
            select(Document)
            .order_by(func.dot_product(Document.embedding, query_embedding).desc())
            .limit(10)
        )
        similar_docs = session.execute(stmt).scalars().all()


Table Type Selection
--------------------

Choose the right table type for your workload:

**ColumnStore** (default) - Best for analytical workloads:

.. code-block:: python

    class Analytics(Base):
        __tablename__ = 'analytics'
        # ... columns ...

        __table_args__ = {
            'singlestoredb_table_type': ColumnStore(),
        }

**RowStore** - Best for transactional workloads:

.. code-block:: python

    from sqlalchemy_singlestoredb import RowStore

    class Session(Base):
        __tablename__ = 'sessions'
        # ... columns ...

        __table_args__ = {
            'singlestoredb_table_type': RowStore(),
        }

**Reference tables** - Replicated to all nodes for fast joins:

.. code-block:: python

    class Country(Base):
        __tablename__ = 'countries'
        # ... columns ...

        __table_args__ = {
            'singlestoredb_table_type': RowStore(reference=True),
        }


Next Steps
----------

- See the :ref:`API Reference <api>` for detailed documentation of all
  SingleStoreDB-specific features
- Learn about :class:`ShardKey` for optimal data distribution
- Explore :class:`VectorKey` for AI/ML similarity search
- Check :class:`PersistedColumn` for computed columns
