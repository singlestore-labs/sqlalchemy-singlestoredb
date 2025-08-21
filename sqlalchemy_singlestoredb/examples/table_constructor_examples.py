"""
Examples demonstrating SingleStore Table constructor integration.
Shows how to use shard_key and sort_key parameters directly in Table constructor.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import Column
from sqlalchemy import create_mock_engine
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table

from sqlalchemy_singlestoredb import ShardKey
from sqlalchemy_singlestoredb import SortKey


def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
    """Print generated DDL."""
    print(sql.compile(dialect=mock_engine.dialect))
    print()


mock_engine = create_mock_engine('singlestoredb://', dump)
# Bind metadata to enable dialect options
metadata = MetaData(bind=mock_engine)


def main() -> None:
    """Demonstrate Table constructor integration."""
    print('=' * 70)
    print('SingleStore Table Constructor Integration Examples')
    print('=' * 70)
    print()

    # Example 1: Basic shard key using dialect options parameter
    print('1. Basic SHARD KEY:')
    table1 = Table(
        'users', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(100)),
        singlestoredb_shardkey=ShardKey('id'),
    )
    table1.create(mock_engine, checkfirst=False)

    # Example 2: SHARD KEY ONLY
    print('2. SHARD KEY ONLY:')
    table2 = Table(
        'large_data', metadata,
        Column('user_id', Integer, primary_key=True),
        Column('content', String(1000)),
        singlestoredb_shardkey=ShardKey('user_id', metadata_only=True),
    )
    table2.create(mock_engine, checkfirst=False)

    # Example 3: Empty shard key
    print('3. Empty SHARD KEY (keyless sharding):')
    table3 = Table(
        'random_data', metadata,
        Column('id', Integer, primary_key=True),
        Column('value', String(50)),
        singlestoredb_shardkey=ShardKey(),
    )
    table3.create(mock_engine, checkfirst=False)

    # Example 4: Multi-column shard key
    print('4. Multi-column SHARD KEY:')
    table4 = Table(
        'orders', metadata,
        Column('user_id', Integer, primary_key=True),
        Column('region_id', Integer, primary_key=True),
        Column('amount', Integer),
        singlestoredb_shardkey=ShardKey('user_id', 'region_id'),
    )
    table4.create(mock_engine, checkfirst=False)

    # Example 5: Both shard key and sort key
    print('5. SHARD KEY + SORT KEY:')
    table5 = Table(
        'events', metadata,
        Column('user_id', Integer, primary_key=True),
        Column('event_id', Integer, primary_key=True),
        Column('timestamp', String(50)),
        Column('event_type', String(50)),
        singlestoredb_shardkey=ShardKey('user_id'),
        singlestoredb_sortkey=SortKey('timestamp'),
    )
    table5.create(mock_engine, checkfirst=False)

    # Example 6: Preserving existing info
    print('6. Preserving existing info:')
    table6 = Table(
        'metadata_table', metadata,
        Column('id', Integer, primary_key=True),
        Column('data', String(100)),
        singlestoredb_shardkey=ShardKey('id', metadata_only=True),
        info={'custom_metadata': 'example_value'},
    )
    table6.create(mock_engine, checkfirst=False)
    print(f"   Custom info preserved: {table6.info.get('custom_metadata')}")
    print()

    print('=' * 70)
    print('All Table constructor examples completed successfully!')
    print('=' * 70)


if __name__ == '__main__':
    main()
