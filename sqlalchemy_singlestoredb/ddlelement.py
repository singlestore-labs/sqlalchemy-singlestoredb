from __future__ import annotations

from typing import Any
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement


def _escape_column_name(column_name: str) -> str:
    """Escape column names with backticks if they contain special characters.

    Parameters
    ----------
    column_name : str
        The column name to escape

    Returns
    -------
    str
        The escaped column name with backticks if needed

    Notes
    -----
    Special characters that require escaping include spaces, hyphens, reserved
    words, and other non-alphanumeric characters. Any existing backticks in the
    column name are doubled to escape them.

    Examples
    --------
    >>> _escape_column_name('normal_column')
    'normal_column'

    >>> _escape_column_name('column with spaces')
    '`column with spaces`'

    >>> _escape_column_name('column-with-hyphens')
    '`column-with-hyphens`'

    >>> _escape_column_name('column`with`backticks')
    '`column``with``backticks`'
    """
    column_str = str(column_name)

    # Check if column name needs escaping
    # Need escaping if it contains non-alphanumeric characters (except underscore)
    # or starts with a digit
    needs_escaping = (
        not column_str.replace('_', '').replace('$', '').isalnum() or
        column_str[0].isdigit() if column_str else False
    )

    if needs_escaping:
        # Double any existing backticks to escape them
        escaped = column_str.replace('`', '``')
        return f'`{escaped}`'
    else:
        return column_str


class ShardKey(DDLElement):
    """SingleStore SHARD KEY DDL element.

    Represents a SHARD KEY constraint for distributing table data across partitions.

    Parameters
    ----------
    columns : str, list[str], or None
        Column specification. Supports these formats:
        - Single column: ShardKey('user_id')
        - List of columns: ShardKey(['user_id', 'tenant_id'])
        - Empty for keyless sharding: ShardKey() or ShardKey([])
    index_type : str, optional, keyword-only
        Index type for the shard key. Options: 'BTREE' or 'HASH'.
    metadata_only : bool, default False, keyword-only
        If True, generates SHARD KEY ... METADATA_ONLY syntax which prevents
        index creation on the shard key columns to save memory.

    Examples
    --------
    Basic shard key:

    >>> ShardKey('user_id')

    Multi-column shard key:

    >>> ShardKey(['user_id', 'category_id'])

    Empty shard key for keyless sharding:

    >>> ShardKey()
    >>> ShardKey([])

    With index type (keyword-only):

    >>> ShardKey('user_id', index_type='HASH')

    With METADATA_ONLY to prevent index creation (keyword-only):

    >>> ShardKey('user_id', metadata_only=True)

    Combined options (keyword-only):

    >>> ShardKey(['user_id', 'tenant_id'], index_type='BTREE', metadata_only=True)

    """

    columns: Tuple[str, ...]
    index_type: Optional[str]
    metadata_only: bool

    def __init__(
        self,
        columns: Optional[Union[str, List[str]]] = None,
        *,
        index_type: Optional[str] = None,
        metadata_only: bool = False,
    ) -> None:
        # Handle different column specification formats
        if columns is None:
            # ShardKey() - empty for keyless sharding
            self.columns = ()
        elif isinstance(columns, str):
            # ShardKey('user_id') - single column
            self.columns = (columns,)
        elif isinstance(columns, (list, tuple)):
            # ShardKey(['user_id', 'tenant_id']) - list of columns
            if not columns:
                self.columns = ()
            else:
                # Validate all items are strings
                for col in columns:
                    if not isinstance(col, str):
                        raise TypeError('All column names must be strings')
                self.columns = tuple(columns)
        else:
            raise TypeError('columns must be a string, a list of strings, or None')

        if index_type is not None and index_type.upper() not in ('BTREE', 'HASH'):
            raise ValueError('index_type must be "BTREE" or "HASH"')

        self.index_type = index_type.upper() if index_type else None
        self.metadata_only = metadata_only

    def __repr__(self) -> str:
        args = []

        # Handle columns parameter - only add if not empty or if there are keyword args
        if len(self.columns) == 1:
            args.append(repr(self.columns[0]))
        elif len(self.columns) > 1:
            args.append(repr(list(self.columns)))
        elif self.index_type is not None or self.metadata_only:
            # Empty columns but with keyword args, need to be explicit
            args.append('None')

        # Handle keyword-only parameters
        if self.index_type is not None:
            args.append(f'index_type={repr(self.index_type)}')
        if self.metadata_only:
            args.append('metadata_only=True')

        # Special case for empty ShardKey() with no other args
        if not args:
            return 'ShardKey()'

        return f'ShardKey({", ".join(args)})'


@compiles(ShardKey, 'singlestoredb.mysql')
def compile_shard_key(element: Any, compiler: Any, **kw: Any) -> str:
    """Compile ShardKey DDL element to SQL.

    Handles all SingleStore SHARD KEY syntax variants including basic shard keys,
    empty shard keys for keyless sharding, index types, and METADATA_ONLY options.

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
    - SHARD KEY USING BTREE (column1) - with index type
    - SHARD KEY (column1) METADATA_ONLY - prevents index creation
    - SHARD KEY USING HASH (column1, column2) METADATA_ONLY - combined options

    Examples
    --------
    >>> compile_shard_key(ShardKey('user_id'), compiler)
    'SHARD KEY (user_id)'

    >>> compile_shard_key(ShardKey(['user_id', 'tenant_id'], index_type='HASH'), compiler)
    'SHARD KEY USING HASH (user_id, tenant_id)'

    >>> compile_shard_key(ShardKey('user_id', metadata_only=True), compiler)
    'SHARD KEY (user_id) METADATA_ONLY'

    """
    # Start building the SQL parts
    sql_parts = ['SHARD KEY']

    # Add index type if specified
    if element.index_type is not None:
        sql_parts.append(f'USING {element.index_type}')

    # Handle special case for empty shard key
    if not element.columns:
        sql_parts.append('()')

    else:
        # Add column list with proper escaping
        column_list = ', '.join([_escape_column_name(x) for x in element.columns])
        sql_parts.append(f'({column_list})')

    # Add METADATA_ONLY if specified
    if element.metadata_only:
        sql_parts.append('METADATA_ONLY')

    return ' '.join(sql_parts)


class SortKey(DDLElement):
    """SingleStore SORT KEY DDL element.

    Represents a SORT KEY constraint for optimizing query performance by
    pre-sorting data within partitions.

    Parameters
    ----------
    columns : Union[str, List[Union[str, Tuple[str, str]]]]
        Column specifications. Can be:
        - A single string (column name, defaults to ASC)
        - A list of column specifications where each element can be:
          - A string (column name, defaults to ASC)
          - A tuple of (column_name, direction) where direction is 'ASC' or 'DESC'

    Examples
    --------
    Single column sort key (ascending by default):

    >>> SortKey('created_at')
    >>> SortKey(['created_at'])  # Equivalent list syntax

    Multi-column sort key with mixed directions:

    >>> SortKey(['user_id', ('created_at', 'DESC')])

    Using static helper methods:

    >>> SortKey([SortKey.asc('user_id'), SortKey.desc('created_at')])

    """

    def __init__(self, columns: Union[str, List[Union[str, Tuple[str, str]]]]) -> None:
        # Handle single string input by converting to list
        if isinstance(columns, str):
            columns = [columns]

        self.columns = []
        for col in columns:
            if isinstance(col, tuple):
                name, direction = col
                if direction is None:
                    raise TypeError('Direction cannot be None')
                direction = direction.upper()
                if direction not in ('ASC', 'DESC'):
                    raise ValueError(
                        f"Direction must be 'ASC' or 'DESC', got '{direction}'",
                    )
                self.columns.append((name, direction))
            else:
                self.columns.append((col, 'ASC'))  # Default to ASC  # Default to ASC

    @staticmethod
    def asc(column: str) -> Tuple[str, str]:
        """Create an ascending sort key column specification.

        Parameters
        ----------
        column : str
            Column name to sort in ascending order

        Returns
        -------
        Tuple[str, str]
            Tuple of (column_name, 'ASC') for use in SortKey constructor

        Examples
        --------
        >>> SortKey([SortKey.asc('created_at')])
        >>> SortKey([SortKey.asc('user_id'), SortKey.desc('created_at')])

        """
        return (column, 'ASC')

    @staticmethod
    def desc(column: str) -> Tuple[str, str]:
        """Create a descending sort key column specification.

        Parameters
        ----------
        column : str
            Column name to sort in descending order

        Returns
        -------
        Tuple[str, str]
            Tuple of (column_name, 'DESC') for use in SortKey constructor

        Examples
        --------
        >>> SortKey([SortKey.desc('created_at')])
        >>> SortKey([SortKey.asc('user_id'), SortKey.desc('created_at')])

        """
        return (column, 'DESC')

    def __repr__(self) -> str:
        parts = []
        for col_name, direction in self.columns:
            if direction == 'ASC':
                parts.append(repr(col_name))
            else:
                parts.append(f'({col_name!r}, {direction!r})')
        return f'SortKey([{", ".join(parts)}])'


@compiles(SortKey, 'singlestoredb.mysql')
def compile_sort_key(element: Any, compiler: Any, **kw: Any) -> str:
    """Compile SortKey DDL element to SQL.

    Generates the SORT KEY clause for SingleStore table creation statements.
    Supports both ascending (ASC) and descending (DESC) sort directions.

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
    >>> compile_sort_key(SortKey(['created_at']), compiler)
    'SORT KEY (created_at)'

    >>> compile_sort_key(SortKey([SortKey.desc('created_at')]), compiler)
    'SORT KEY (created_at DESC)'

    >>> compile_sort_key(SortKey(['user_id', SortKey.desc('created_at')]), compiler)
    'SORT KEY (user_id, created_at DESC)'

    """
    if not element.columns:
        return 'SORT KEY ()'

    parts = []
    for col_name, direction in element.columns:
        escaped_col = _escape_column_name(col_name)
        if direction == 'ASC':
            # ASC is default in SingleStore, omit for cleaner SQL
            parts.append(escaped_col)
        else:
            # DESC must be explicit
            parts.append(f'{escaped_col} DESC')

    return f'SORT KEY ({", ".join(parts)})'


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
    column_list = ', '.join([_escape_column_name(x) for x in element.columns])

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
    column : str
        Column name to include in the multi-value index. Must be a JSON column.
    index_options : str, optional
        JSON string containing multi-value index options.

    Examples
    --------
    Basic multi-value index:

    >>> MultiValueIndex('tags')

    Multi-value index with options:

    >>> MultiValueIndex('tags', index_options='{"some_option":"value"}')

    Notes
    -----
    Multi-value indexes are used to index JSON arrays in SingleStore.
    They allow efficient queries on individual elements within JSON arrays.

    The column must be of JSON type for multi-value indexing to work properly.
    """

    def __init__(self, column: str, *, index_options: Optional[str] = None) -> None:
        self.column = column
        self.index_options = index_options

    def __repr__(self) -> str:
        args = [repr(self.column)]
        if self.index_options is not None:
            args.append(f'index_options={repr(self.index_options)}')
        return f'MultiValueIndex({", ".join(args)})'


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
    >>> compile_multi_value_index(MultiValueIndex('tags'), compiler)
    'MULTI VALUE INDEX (tags)'

    """
    escaped_column = _escape_column_name(element.column)
    sql = f'MULTI VALUE INDEX ({escaped_column})'

    if element.index_options:
        sql += f" INDEX_OPTIONS='{element.index_options}'"

    return sql


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
    column_list = ', '.join([_escape_column_name(x) for x in element.columns])

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
