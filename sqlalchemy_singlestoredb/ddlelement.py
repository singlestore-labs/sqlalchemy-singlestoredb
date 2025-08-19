from __future__ import annotations

from typing import Any
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

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
    columns : str or list[str]
        Column name (str) or list of column names (list[str]) to include in the
        vector index. Usually a single vector column.
    name : str, optional
        Index name for the vector index. If not provided, SingleStore will
        auto-generate a name.
    index_options : str, optional
        JSON string containing vector index options such as metric_type.
        Common values: '{"metric_type":"EUCLIDEAN_DISTANCE"}',
        '{"metric_type":"DOT_PRODUCT"}', '{"metric_type":"COSINE_SIMILARITY"}'

    Examples
    --------
    Single vector column, auto-named:

    >>> VectorKey('embedding')

    Single vector column, named:

    >>> VectorKey('embedding', name='vec_idx')

    Vector index with options:

    >>> VectorKey('embedding', name='vec_idx',
    ...           index_options='{"metric_type":"EUCLIDEAN_DISTANCE"}')

    Multiple vector columns (if supported):

    >>> VectorKey(['embedding1', 'embedding2'], name='vec_idx')

    """

    name: Optional[str]
    columns: Tuple[str, ...]
    index_options: Optional[str]

    def __init__(
        self,
        columns: Union[str, List[str]],
        *,
        name: Optional[str] = None,
        index_options: Optional[str] = None,
    ) -> None:
        if isinstance(columns, str):
            self.columns = (columns,)
        elif isinstance(columns, (list, tuple)):
            if not columns:
                raise ValueError(
                    'At least one column must be specified for VECTOR index',
                )
            self.columns = tuple(str(col) for col in columns)
        else:
            raise TypeError('columns must be a string or list of strings')

        self.name = name
        self.index_options = index_options

    def __repr__(self) -> str:
        if len(self.columns) == 1:
            columns_repr = repr(self.columns[0])
        else:
            columns_repr = repr(list(self.columns))

        args = [columns_repr]
        if self.name is not None:
            args.append(f'name={repr(self.name)}')
        if self.index_options is not None:
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
    - VECTOR INDEX (column) - auto-named vector index
    - VECTOR INDEX name (column) - named vector index
    - VECTOR INDEX name (column) INDEX_OPTIONS='{"metric_type":"EUCLIDEAN_DISTANCE"}'

    Examples
    --------
    >>> compile_vector_key(VectorKey('embedding'), compiler)
    'VECTOR INDEX (embedding)'

    >>> compile_vector_key(VectorKey('embedding', name='vec_idx'), compiler)
    'VECTOR INDEX vec_idx (embedding)'

    """
    column_list = ', '.join([str(x) for x in element.columns])

    # Start building the SQL
    sql_parts = ['VECTOR INDEX']

    # Add name if provided
    if element.name is not None:
        sql_parts.append(element.name)

    # Add column list
    sql_parts.append(f'({column_list})')

    vector_index_sql = ' '.join(sql_parts)

    if element.index_options:
        vector_index_sql += f" INDEX_OPTIONS='{element.index_options}'"

    return vector_index_sql


class MultiValueIndex(DDLElement):
    """SingleStore MULTI VALUE INDEX DDL element.

    Represents a MULTI VALUE INDEX for indexing JSON array values.

    Parameters
    ----------
    name : str
        Index name for the multi-value index
    column : str
        Column name to include in the multi-value index. Must be a JSON column.

    Examples
    --------
    Basic multi-value index:

    >>> MultiValueIndex('mv_tags', 'tags')

    Notes
    -----
    Multi-value indexes are used to index JSON arrays in SingleStore.
    They allow efficient queries on individual elements within JSON arrays.

    The column must be of JSON type for multi-value indexing to work properly.
    """

    def __init__(self, name: str, column: str) -> None:
        self.name = name
        self.column = column

    def __repr__(self) -> str:
        return f'MultiValueIndex({repr(self.name)}, {repr(self.column)})'


@compiles(MultiValueIndex, 'singlestoredb.mysql')
def compile_multi_value_index(element: Any, compiler: Any, **kw: Any) -> str:
    """Compile MultiValueIndex DDL element to SQL.

    Generates the MULTI VALUE INDEX clause for SingleStore table creation statements.

    Parameters
    ----------
    element : MultiValueIndex
        The MultiValueIndex DDL element to compile
    compiler : DDLCompiler
        SQLAlchemy DDL compiler instance
    **kw : Any
        Additional compiler keyword arguments

    Returns
    -------
    str
        The compiled SQL string for the MULTI VALUE INDEX clause

    Examples
    --------
    >>> compile_multi_value_index(MultiValueIndex('mv_tags', 'tags'), compiler)
    'MULTI VALUE INDEX mv_tags (tags)'

    """
    return f'MULTI VALUE INDEX {element.name} ({element.column})'


class FullTextIndex(DDLElement):
    """SingleStore FULLTEXT INDEX DDL element.

    Represents a FULLTEXT INDEX for full-text search on text columns.

    Parameters
    ----------
    columns : str or list[str]
        Column name (str) or list of column names (list[str]) to include in the
        fulltext index. Must be text columns (CHAR, VARCHAR, TEXT, LONGTEXT).
    name : str, optional
        Index name for the fulltext index. If not provided, SingleStore will
        auto-generate a name.
    version : int, optional
        FULLTEXT version to use. Version 1 (default) if not specified.
        Version 2 or higher requires explicit specification.

    Examples
    --------
    Single column, auto-named:

    >>> FullTextIndex('title')

    Single column, named:

    >>> FullTextIndex('title', name='ft_title')

    Multiple columns, auto-named:

    >>> FullTextIndex(['title', 'content'])

    Multiple columns, named:

    >>> FullTextIndex(['title', 'content'], name='ft_search')

    With version specification:

    >>> FullTextIndex(['title', 'content'], name='ft_v2', version=2)

    Future version support:

    >>> FullTextIndex('description', name='ft_v3', version=3)

    """

    name: Optional[str]
    columns: Tuple[str, ...]
    version: Optional[int]

    def __init__(
        self,
        columns: Union[str, List[str]],
        *,
        name: Optional[str] = None,
        version: Optional[int] = None,
    ) -> None:
        if isinstance(columns, str):
            self.columns = (columns,)
        elif isinstance(columns, (list, tuple)):
            if not columns:
                raise ValueError(
                    'At least one column must be specified for FULLTEXT index',
                )
            self.columns = tuple(str(col) for col in columns)
        else:
            raise TypeError('columns must be a string or list of strings')

        self.name = name
        self.version = version

    def __repr__(self) -> str:
        if len(self.columns) == 1:
            columns_repr = repr(self.columns[0])
        else:
            columns_repr = repr(list(self.columns))

        args = [columns_repr]
        if self.name is not None:
            args.append(f'name={repr(self.name)}')
        if self.version is not None:
            args.append(f'version={self.version}')
        return f'FullTextIndex({", ".join(args)})'


@compiles(FullTextIndex, 'singlestoredb.mysql')
def compile_fulltext_index(element: Any, compiler: Any, **kw: Any) -> str:
    """Compile FullTextIndex DDL element to SQL.

    Generates the FULLTEXT INDEX clause for SingleStore table creation statements.

    Parameters
    ----------
    element : FullTextIndex
        The FullTextIndex DDL element to compile
    compiler : DDLCompiler
        SQLAlchemy DDL compiler instance
    **kw : Any
        Additional compiler keyword arguments

    Returns
    -------
    str
        The compiled SQL string for the FULLTEXT INDEX clause

    Notes
    -----
    Supported syntax variants:
    - FULLTEXT (column1, column2) - version 1, auto-generated name
    - FULLTEXT index_name (column1, column2) - version 1, named
    - FULLTEXT USING VERSION 1 index_name (column1, column2) - explicit version 1
    - FULLTEXT USING VERSION 2 index_name (column1, column2) - version 2+

    Examples
    --------
    >>> compile_fulltext_index(FullTextIndex(['title', 'content']), compiler)
    'FULLTEXT (title, content)'

    >>> compile_fulltext_index(FullTextIndex(['title', 'content'], name='ft_idx'),
    ...                        compiler)
    'FULLTEXT ft_idx (title, content)'

    >>> compile_fulltext_index(FullTextIndex('title', name='ft_idx', version=2), compiler)
    'FULLTEXT USING VERSION 2 ft_idx (title)'

    """
    column_list = ', '.join([str(x) for x in element.columns])

    # Start building the SQL
    sql_parts = ['FULLTEXT']

    # Add version clause if specified (version 1 can be implicit or explicit)
    if element.version is not None and element.version != 1:
        # Version 2+ requires explicit specification
        sql_parts.append(f'USING VERSION {element.version}')
    elif element.version == 1:
        # Version 1 can optionally be explicit
        sql_parts.append('USING VERSION 1')
    # If version is None, default to version 1 (implicit)

    # Add name if provided
    if element.name is not None:
        sql_parts.append(element.name)

    # Add column list
    sql_parts.append(f'({column_list})')

    return ' '.join(sql_parts)
