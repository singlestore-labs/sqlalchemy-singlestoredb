"""SingleStore-specific Table extensions for natural SHARD KEY integration."""
from __future__ import annotations

from typing import Any
from typing import Optional

from sqlalchemy import MetaData
from sqlalchemy import Table as SQLATable

from .ddlelement import ShardKey
from .ddlelement import SortKey


class Table(SQLATable):
    """SingleStore-enhanced Table that supports shard_key and sort_key parameters.

    This extends SQLAlchemy's Table to provide natural integration with SingleStore
    SHARD KEY and SORT KEY syntax.

    Examples:
        # Basic usage with shard key
        table = Table('users', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            singlestoredb_shard_key=ShardKey('id')
        )

        # With both shard key and sort key
        table = Table('orders', metadata,
            Column('user_id', Integer),
            Column('order_id', Integer),
            Column('created_at', DateTime),
            singlestoredb_shard_key=ShardKey('user_id'),
            singlestoredb_sort_key=SortKey('created_at')
        )

        # All SHARD KEY variants supported
        Table('table1', metadata, Column('id', Integer),
              singlestoredb_shard_key=ShardKey('id'))                    # Basic
        Table('table2', metadata, Column('id', Integer),
              singlestoredb_shard_key=ShardKey('id', only=True))         # SHARD KEY ONLY
        Table('table3', metadata, Column('id', Integer),
              singlestoredb_shard_key=ShardKey())                        # Empty (keyless)
    """

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: Any,
        singlestoredb_shard_key: Optional[ShardKey] = None,
        singlestoredb_sort_key: Optional[SortKey] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SingleStore Table with optional shard_key and sort_key.

        Args:
            name: Table name
            metadata: SQLAlchemy MetaData instance
            *args: Columns and other table elements
            singlestoredb_shard_key: Optional ShardKey instance
            singlestoredb_sort_key: Optional SortKey instance
            **kwargs: Other standard Table arguments
        """
        # Handle info dictionary
        info = kwargs.get('info', {})

        # Add SingleStore-specific keys to info
        if singlestoredb_shard_key is not None:
            info['singlestoredb_shard_key'] = singlestoredb_shard_key

        if singlestoredb_sort_key is not None:
            info['singlestoredb_sort_key'] = singlestoredb_sort_key

        # Update kwargs with the enhanced info
        if info:
            kwargs['info'] = info

        # Call parent constructor
        super().__init__(name, metadata, *args, **kwargs)
