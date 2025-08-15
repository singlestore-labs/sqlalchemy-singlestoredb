from __future__ import annotations

from typing import Any
from typing import Optional

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement


class ShardKey(DDLElement):
    """SingleStore SHARD KEY DDL element.

    Represents a SHARD KEY constraint for distributing table data across partitions.

    Parameters
    ----------
    *columns : Any
        Column names to include in the shard key. If no columns are provided,
        creates an empty shard key for keyless (random) sharding.
    only : bool, default False
        If True, generates SHARD KEY ONLY syntax which prevents index creation
        on the shard key columns to save memory.

    Examples
    --------
    Basic shard key:

    >>> ShardKey('user_id')

    Multi-column shard key:

    >>> ShardKey('user_id', 'category_id')

    Empty shard key for keyless sharding:

    >>> ShardKey()

    SHARD KEY ONLY to prevent index creation:

    >>> ShardKey('user_id', only=True)

    """

    def __init__(self, *columns: Any, only: bool = False) -> None:
        self.columns = columns
        self.only = only

    def __repr__(self) -> str:
        args = ', '.join([repr(x) for x in self.columns])
        if self.only:
            if self.columns:
                return f'ShardKey({args}, only=True)'
            else:
                return 'ShardKey(only=True)'
        return f'ShardKey({args})'


@compiles(ShardKey, 'singlestoredb.mysql')
def compile_shard_key(element: Any, compiler: Any, **kw: Any) -> str:
    """Compile ShardKey DDL element to SQL.

    Handles all SingleStore SHARD KEY syntax variants including basic shard keys,
    empty shard keys for keyless sharding, and SHARD KEY ONLY variants.

    Parameters
    ----------
    element : ShardKey
        The ShardKey DDL element to compile
    compiler : DDLCompiler
        SQLAlchemy DDL compiler instance
    **kw : Any
        Additional compiler keyword arguments

    Returns
    -------
    str
        The compiled SQL string for the SHARD KEY clause

    Notes
    -----
    Supported syntax variants:
    - SHARD KEY (column1, column2) - basic shard key
    - SHARD KEY () - empty shard key for keyless sharding
    - SHARD KEY ONLY (column1, column2) - prevents index creation

    """
    if element.only:
        if element.columns:
            column_list = ', '.join([str(x) for x in element.columns])
            return f'SHARD KEY ONLY ({column_list})'
        else:
            # SHARD KEY ONLY with no columns doesn't make sense,
            # fallback to empty shard key
            return 'SHARD KEY ()'
    else:
        column_list = ', '.join([str(x) for x in element.columns])
        return f'SHARD KEY ({column_list})'


class SortKey(DDLElement):
    """SingleStore SORT KEY DDL element.

    Represents a SORT KEY constraint for optimizing query performance by
    pre-sorting data within partitions.

    Parameters
    ----------
    *columns : Any
        Column names to include in the sort key. The order matters as it
        determines the sorting priority.

    Examples
    --------
    Basic sort key:

    >>> SortKey('created_at')

    Multi-column sort key:

    >>> SortKey('user_id', 'created_at')

    """

    def __init__(self, *columns: Any) -> None:
        self.columns = columns

    def __repr__(self) -> str:
        return 'SortKey(%s)' % ', '.join([repr(x) for x in self.columns])


@compiles(SortKey, 'singlestoredb.mysql')
def compile_sort_key(element: Any, compiler: Any, **kw: Any) -> str:
    """Compile SortKey DDL element to SQL.

    Generates the SORT KEY clause for SingleStore table creation statements.

    Parameters
    ----------
    element : SortKey
        The SortKey DDL element to compile
    compiler : DDLCompiler
        SQLAlchemy DDL compiler instance
    **kw : Any
        Additional compiler keyword arguments

    Returns
    -------
    str
        The compiled SQL string for the SORT KEY clause

    Examples
    --------
    >>> compile_sort_key(SortKey('created_at'), compiler)
    'SORT KEY (created_at)'

    """
    return 'SORT KEY (%s)' % ', '.join([str(x) for x in element.columns])


class VectorKey(DDLElement):
    """SingleStore VECTOR INDEX DDL element.

    Represents a VECTOR INDEX for similarity search on vector data columns.

    Parameters
    ----------
    name : str
        Index name for the vector index
    *columns : Any
        Column names to include in the vector index. Usually a single vector column.
    index_options : str, optional
        JSON string containing vector index options such as metric_type.
        Common values: '{"metric_type":"EUCLIDEAN_DISTANCE"}',
        '{"metric_type":"DOT_PRODUCT"}', '{"metric_type":"COSINE_SIMILARITY"}'

    Examples
    --------
    Basic vector index:

    >>> VectorKey('vec_idx', 'embedding')

    Vector index with options:

    >>> VectorKey('vec_idx', 'embedding',
    ...           index_options='{"metric_type":"EUCLIDEAN_DISTANCE"}')

    Multi-column vector index (if supported):

    >>> VectorKey('vec_idx', 'embedding1', 'embedding2')

    """

    def __init__(
        self, name: str, *columns: Any, index_options: Optional[str] = None,
    ) -> None:
        self.name = name
        self.columns = columns
        self.index_options = index_options

    def __repr__(self) -> str:
        args = [repr(self.name)] + [repr(x) for x in self.columns]
        if self.index_options:
            args.append(f'index_options={repr(self.index_options)}')
        return f'VectorKey({", ".join(args)})'


@compiles(VectorKey, 'singlestoredb.mysql')
def compile_vector_key(element: Any, compiler: Any, **kw: Any) -> str:
    """Compile VectorKey DDL element to SQL.

    Generates the VECTOR INDEX clause for SingleStore table creation statements.

    Parameters
    ----------
    element : VectorKey
        The VectorKey DDL element to compile
    compiler : DDLCompiler
        SQLAlchemy DDL compiler instance
    **kw : Any
        Additional compiler keyword arguments

    Returns
    -------
    str
        The compiled SQL string for the VECTOR INDEX clause

    Notes
    -----
    Supported syntax variants:
    - VECTOR INDEX name (column) - basic vector index
    - VECTOR INDEX name (column) INDEX_OPTIONS='{"metric_type":"EUCLIDEAN_DISTANCE"}'

    Examples
    --------
    >>> compile_vector_key(VectorKey('vec_idx', 'embedding'), compiler)
    'VECTOR INDEX vec_idx (embedding)'

    """
    column_list = ', '.join([str(x) for x in element.columns])
    vector_index_sql = f'VECTOR INDEX {element.name} ({column_list})'

    if element.index_options:
        vector_index_sql += f" INDEX_OPTIONS='{element.index_options}'"

    return vector_index_sql
