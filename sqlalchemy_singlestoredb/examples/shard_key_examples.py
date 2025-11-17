"""
Examples demonstrating all SingleStore SHARD KEY variants.
Shows basic, multi-column, empty, and SHARD KEY ONLY syntax.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import Column
from sqlalchemy import create_mock_engine
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import String
from sqlalchemy.orm import declarative_base

from sqlalchemy_singlestoredb import ShardKey
from sqlalchemy_singlestoredb import SortKey

Base = declarative_base()


def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
    """Print generated DDL."""
    print(sql.compile(dialect=mock_engine.dialect))
    print()  # Add blank line for readability


mock_engine = create_mock_engine('singlestoredb://', dump)


# Example 1: Basic SHARD KEY with single column
class BasicShardTable(Base):  # type: ignore
    """Example with basic SHARD KEY on single column."""
    __tablename__ = 'basic_shard_table'

    user_id = Column(Integer, primary_key=True)
    data = Column(String(50))

    __table_args__ = {
        'singlestoredb_shardkey': ShardKey('user_id'),
    }


# Example 2: Multi-column SHARD KEY
class MultiColumnShardTable(Base):  # type: ignore
    """Example with SHARD KEY on multiple columns."""
    __tablename__ = 'multi_column_shard_table'

    user_id = Column(Integer, primary_key=True)
    category_id = Column(Integer, primary_key=True)
    amount = Column(Numeric(10, 2))

    __table_args__ = {
        'singlestoredb_shardkey': ShardKey('user_id', 'category_id'),
    }


# Example 3: Empty SHARD KEY for keyless/random sharding
class KeylessShardTable(Base):  # type: ignore
    """Example with empty SHARD KEY for random distribution."""
    __tablename__ = 'keyless_shard_table'

    id = Column(Integer, primary_key=True)
    data = Column(String(100))

    __table_args__ = {
        'singlestoredb_shardkey': ShardKey(),
    }


# Example 4: SHARD KEY ONLY (prevents index creation)
class ShardKeyOnlyTable(Base):  # type: ignore
    """Example with SHARD KEY ONLY to save memory by not creating index."""
    __tablename__ = 'shard_key_only_table'

    user_id = Column(Integer, primary_key=True)
    large_data = Column(String(1000))

    __table_args__ = {
        'singlestoredb_shardkey': ShardKey('user_id', metadata_only=True),
    }


# Example 5: Complex table with SHARD KEY and SORT KEY
class ComplexTable(Base):  # type: ignore
    """Example with both SHARD KEY and SORT KEY for optimization."""
    __tablename__ = 'complex_table'

    user_id = Column(Integer, primary_key=True)
    order_id = Column(Integer, primary_key=True)
    product_name = Column(String(100))
    amount = Column(Numeric(10, 2))

    __table_args__ = {
        'singlestoredb_shardkey': ShardKey('user_id'),
        'singlestoredb_sortkey': SortKey('order_id', 'amount'),
    }


# Example 6: Multi-column SHARD KEY ONLY
class MultiColumnOnlyTable(Base):  # type: ignore
    """Example with multi-column SHARD KEY ONLY."""
    __tablename__ = 'multi_column_only_table'

    user_id = Column(Integer, primary_key=True)
    region_id = Column(Integer, primary_key=True)
    data = Column(String(50))

    __table_args__ = {
        'singlestoredb_shardkey': ShardKey(
            'user_id', 'region_id', metadata_only=True,
        ),
    }


def main() -> None:
    """Generate DDL for all example tables."""
    print('=' * 60)
    print('SHARD KEY Examples - DDL Generation')
    print('=' * 60)
    print()

    print('1. Basic SHARD KEY (single column):')
    BasicShardTable.__table__.create(mock_engine, checkfirst=False)

    print('2. Multi-column SHARD KEY:')
    MultiColumnShardTable.__table__.create(mock_engine, checkfirst=False)

    print('3. Empty SHARD KEY (keyless sharding):')
    KeylessShardTable.__table__.create(mock_engine, checkfirst=False)

    print('4. SHARD KEY ONLY (no index creation):')
    ShardKeyOnlyTable.__table__.create(mock_engine, checkfirst=False)

    print('5. Complex table with SHARD KEY and SORT KEY:')
    ComplexTable.__table__.create(mock_engine, checkfirst=False)

    print('6. Multi-column SHARD KEY ONLY:')
    MultiColumnOnlyTable.__table__.create(mock_engine, checkfirst=False)

    print('=' * 60)
    print('All examples generated successfully!')
    print('=' * 60)


if __name__ == '__main__':
    main()
