"""SingleStore-specific Table extensions for natural SHARD KEY integration."""
from __future__ import annotations

from typing import Any
from typing import Optional
from typing import Type

from sqlalchemy import MetaData
from sqlalchemy import Table as SQLATable

from .ddlelement import ShardKey
from .ddlelement import SortKey


class Table(SQLATable):
    """SingleStore-enhanced Table that supports shard_key and sort_key parameters.

    This extends SQLAlchemy's Table to provide natural integration with SingleStore
    SHARD KEY and SORT KEY syntax.

    Examples
    --------
    Basic usage with shard key:

    >>> table = Table('users', metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('name', String(50)),
    ...     singlestoredb_shard_key=ShardKey('id')
    ... )

    With both shard key and sort key:

    >>> table = Table('orders', metadata,
    ...     Column('user_id', Integer),
    ...     Column('order_id', Integer),
    ...     Column('created_at', DateTime),
    ...     singlestoredb_shard_key=ShardKey('user_id'),
    ...     singlestoredb_sort_key=SortKey('created_at')
    ... )

    All SHARD KEY variants supported:

    >>> Table('table1', metadata, Column('id', Integer),
    ...       singlestoredb_shard_key=ShardKey('id'))                    # Basic
    >>> Table('table2', metadata, Column('id', Integer),
    ...       singlestoredb_shard_key=ShardKey('id', only=True))         # SHARD KEY ONLY
    >>> Table('table3', metadata, Column('id', Integer),
    ...       singlestoredb_shard_key=ShardKey())                        # Empty (keyless)

    """

    def __new__(
        cls: Type['Table'],
        name: str,
        metadata: MetaData,
        *args: Any,
        singlestoredb_shard_key: Optional[ShardKey] = None,
        singlestoredb_sort_key: Optional[SortKey] = None,
        **kwargs: Any,
    ) -> 'Table':
        """Handle SingleStore-specific parameters for SQLAlchemy 1.4 compatibility.

        In SQLAlchemy 1.4, Table.__new__ processes the info parameter, so we need
        to handle our custom parameters here as well.

        Parameters
        ----------
        cls : Type['Table']
            Table class
        name : str
            Table name
        metadata : MetaData
            SQLAlchemy MetaData instance
        *args : Any
            Columns and other table elements
        singlestoredb_shard_key : Optional[ShardKey], default None
            Optional ShardKey instance
        singlestoredb_sort_key : Optional[SortKey], default None
            Optional SortKey instance
        **kwargs : Any
            Other standard Table arguments

        Returns
        -------
        Table
            New Table instance with SingleStore-specific parameters processed

        """

        # Handle info dictionary - create a copy to avoid mutating input
        info = kwargs.get('info', {}).copy()

        # Add SingleStore-specific keys to info
        if singlestoredb_shard_key is not None:
            info['singlestoredb_shard_key'] = singlestoredb_shard_key

        if singlestoredb_sort_key is not None:
            info['singlestoredb_sort_key'] = singlestoredb_sort_key

        # Always update kwargs with info if we added SingleStore keys
        if (
            singlestoredb_shard_key is not None or
            singlestoredb_sort_key is not None or info
        ):
            kwargs['info'] = info

        # Call parent constructor
        return super().__new__(cls, name, metadata, *args, **kwargs)

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: Any,
        singlestoredb_shard_key: Optional[ShardKey] = None,
        singlestoredb_sort_key: Optional[SortKey] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SingleStore Table.

        The SingleStore-specific parameters are already handled in __new__ for
        SQLAlchemy 1.4 compatibility. This method handles them for SQLAlchemy 2.0+.

        Parameters
        ----------
        name : str
            Table name
        metadata : MetaData
            SQLAlchemy MetaData instance
        *args : Any
            Columns and other table elements
        singlestoredb_shard_key : Optional[ShardKey], default None
            Optional ShardKey instance
        singlestoredb_sort_key : Optional[SortKey], default None
            Optional SortKey instance
        **kwargs : Any
            Other standard Table arguments

        """
        # In SQLAlchemy 2.0+, __new__ might not be called with our custom parameters,
        # so we need to handle them here as well
        if singlestoredb_shard_key is not None or singlestoredb_sort_key is not None:

            # Handle info dictionary - create a copy to avoid mutating input
            info = kwargs.get('info', {}).copy()

            # Add SingleStore-specific keys to info
            if singlestoredb_shard_key is not None:
                info['singlestoredb_shard_key'] = singlestoredb_shard_key

            if singlestoredb_sort_key is not None:
                info['singlestoredb_sort_key'] = singlestoredb_sort_key

            # Always update kwargs with info if we added SingleStore keys
            if (
                singlestoredb_shard_key is not None or
                singlestoredb_sort_key is not None or info
            ):
                kwargs['info'] = info

        # Call parent constructor
        super().__init__(name, metadata, *args, **kwargs)
