"""SingleStore-specific Table extensions for natural SHARD KEY integration."""
from __future__ import annotations

from typing import Any
from typing import Type

from sqlalchemy import MetaData
from sqlalchemy import Table as SQLATable

from .ddlelement import FullTextIndex
from .ddlelement import MultiValueIndex
from .ddlelement import ShardKey
from .ddlelement import SortKey
from .ddlelement import VectorKey


class Table(SQLATable):
    """SingleStore Table that supports SingleStore-specific DDL elements.

    This extends SQLAlchemy's Table to provide natural integration with SingleStore
    SHARD KEY, SORT KEY, VECTOR INDEX, and FULLTEXT INDEX syntax.

    Examples
    --------
    Basic usage with shard key:

    >>> table = Table('users', metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('name', String(50)),
    ...     ShardKey('id')
    ... )

    With both shard key and sort key:

    >>> table = Table('orders', metadata,
    ...     Column('user_id', Integer),
    ...     Column('order_id', Integer),
    ...     Column('created_at', DateTime),
    ...     ShardKey('user_id'),
    ...     SortKey('created_at')
    ... )

    With vector indexes:

    >>> table = Table('documents', metadata,
    ...     Column('doc_id', Integer, primary_key=True),
    ...     Column('embedding', VECTOR(128, 'F32')),
    ...     Column('title_embedding', VECTOR(256, 'F32')),
    ...     VectorKey('embedding', name='vec_idx'),
    ...     VectorKey('title_embedding', name='title_vec_idx',
    ...               index_options='{"metric_type":"EUCLIDEAN_DISTANCE"}')
    ... )

    With fulltext indexes:

    >>> table = Table('articles', metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('title', String(200)),
    ...     Column('content', Text),
    ...     FullTextIndex('title', 'content'),  # Multiple columns, auto-named
    ...     FullTextIndex('title', name='ft_title', version=2)  # Single column, named v2
    ... )

    With multi-value indexes on JSON columns:

    >>> table = Table('products', metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('tags', JSON),
    ...     Column('categories', JSON),
    ...     ShardKey('id'),
    ...     MultiValueIndex('mv_tags', 'tags'),
    ...     MultiValueIndex('mv_categories', 'categories')
    ... )

    All SHARD KEY variants supported:

    >>> Table('table1', metadata, Column('id', Integer), ShardKey('id'))  # Basic
    >>> Table('table2', metadata, Column('id', Integer),
    ...       ShardKey('id', metadata_only=True))  # SHARD KEY METADATA_ONLY
    >>> Table('table3', metadata, Column('id', Integer), ShardKey())  # Empty (keyless)

    """

    def __new__(
        cls: Type['Table'],
        name: str,
        metadata: MetaData,
        *args: Any,
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
            Columns and SingleStore-specific elements (ShardKey, SortKey, VectorKey,
            FullTextIndex)
        **kwargs : Any
            Other standard Table arguments

        Returns
        -------
        Table
            New Table instance with SingleStore-specific parameters processed

        """
        # Separate regular args from SingleStore-specific args
        regular_args = []
        shard_key = None
        sort_key = None
        vector_keys = []
        multi_value_indexes = []
        fulltext_indexes = []

        for arg in args:
            if isinstance(arg, ShardKey):
                if shard_key is not None:
                    raise ValueError('Only one ShardKey can be specified per table')
                shard_key = arg
            elif isinstance(arg, SortKey):
                if sort_key is not None:
                    raise ValueError('Only one SortKey can be specified per table')
                sort_key = arg
            elif isinstance(arg, VectorKey):
                vector_keys.append(arg)
            elif isinstance(arg, MultiValueIndex):
                multi_value_indexes.append(arg)
            elif isinstance(arg, FullTextIndex):
                fulltext_indexes.append(arg)
            else:
                regular_args.append(arg)

        # Handle info dictionary - create a copy to avoid mutating input
        info = kwargs.get('info', {}).copy()

        # Add SingleStore-specific keys to info
        if shard_key is not None:
            info['singlestoredb_shard_key'] = shard_key

        if sort_key is not None:
            info['singlestoredb_sort_key'] = sort_key

        if vector_keys:
            info['singlestoredb_vector_indexes'] = vector_keys

        if multi_value_indexes:
            info['singlestoredb_multi_value_indexes'] = multi_value_indexes

        if fulltext_indexes:
            info['singlestoredb_fulltext_indexes'] = fulltext_indexes

        # Always update kwargs with info if we added SingleStore keys
        if (
            shard_key is not None or sort_key is not None or
            vector_keys or multi_value_indexes or fulltext_indexes or info
        ):
            kwargs['info'] = info

        # Call parent constructor with filtered args
        return super().__new__(cls, name, metadata, *regular_args, **kwargs)

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: Any,
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
            Columns and SingleStore-specific elements (ShardKey, SortKey, VectorKey,
            FullTextIndex)
        **kwargs : Any
            Other standard Table arguments

        """
        # In SQLAlchemy 2.0+, __new__ might not be called with our custom parameters,
        # so we need to handle them here as well

        # Separate regular args from SingleStore-specific args
        regular_args = []
        shard_key = None
        sort_key = None
        vector_keys = []
        multi_value_indexes = []
        fulltext_indexes = []

        for arg in args:
            if isinstance(arg, ShardKey):
                if shard_key is not None:
                    raise ValueError('Only one ShardKey can be specified per table')
                shard_key = arg
            elif isinstance(arg, SortKey):
                if sort_key is not None:
                    raise ValueError('Only one SortKey can be specified per table')
                sort_key = arg
            elif isinstance(arg, VectorKey):
                vector_keys.append(arg)
            elif isinstance(arg, MultiValueIndex):
                multi_value_indexes.append(arg)
            elif isinstance(arg, FullTextIndex):
                fulltext_indexes.append(arg)
            else:
                regular_args.append(arg)

        # Handle info dictionary if we found SingleStore-specific parameters
        if (
            shard_key is not None or sort_key is not None or
            vector_keys or multi_value_indexes or fulltext_indexes
        ):
            # Handle info dictionary - create a copy to avoid mutating input
            info = kwargs.get('info', {}).copy()

            # Add SingleStore-specific keys to info
            if shard_key is not None:
                info['singlestoredb_shard_key'] = shard_key

            if sort_key is not None:
                info['singlestoredb_sort_key'] = sort_key

            if vector_keys:
                info['singlestoredb_vector_indexes'] = vector_keys

            if multi_value_indexes:
                info['singlestoredb_multi_value_indexes'] = multi_value_indexes

            if fulltext_indexes:
                info['singlestoredb_fulltext_indexes'] = fulltext_indexes

            # Update kwargs with info
            kwargs['info'] = info

        # Call parent constructor with filtered args
        super().__init__(name, metadata, *regular_args, **kwargs)
