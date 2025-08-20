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
    *columns : Union[str, Tuple[str, str]]
        Variable number of column specifications for the shard key.
        Each column can be either:
        - A string column name (defaults to ASC)
        - A tuple of (column_name, direction) where direction is 'ASC' or 'DESC'
        For empty shard key (keyless sharding), pass no arguments.
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

    >>> ShardKey('user_id', 'category_id')

    Empty shard key for keyless sharding:

    >>> ShardKey()

    With explicit ASC/DESC directions:

    >>> ShardKey('user_id', ('category_id', 'DESC'))

    Using static helper methods:

    >>> ShardKey(ShardKey.asc('user_id'), ShardKey.desc('category_id'))

    Mixed directions for performance optimization:

    >>> ShardKey('tenant_id', ShardKey.desc('created_at'))

    With index type (keyword-only):

    >>> ShardKey('user_id', index_type='HASH')

    With METADATA_ONLY to prevent index creation (keyword-only):

    >>> ShardKey('user_id', metadata_only=True)

    Combined options with directions (keyword-only):

    >>> ShardKey(ShardKey.asc('user_id'), ShardKey.desc('tenant_id'),
    ...           index_type='BTREE', metadata_only=True)

    """

    columns: List[Tuple[str, str]]
    index_type: Optional[str]
    metadata_only: bool

    def __init__(
        self,
        *columns: Union[str, Tuple[str, str]],
        index_type: Optional[str] = None,
        metadata_only: bool = False,
    ) -> None:
        self.columns: List[Tuple[str, str]] = []

        # Process each column specification
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
            elif isinstance(col, str):
                self.columns.append((col, 'ASC'))  # Default to ASC
            else:
                raise TypeError(f'Column must be str or tuple, got {type(col).__name__}')

        if index_type is not None and index_type.upper() not in ('BTREE', 'HASH'):
            raise ValueError('index_type must be "BTREE" or "HASH"')

        self.index_type = index_type.upper() if index_type else None
        self.metadata_only = metadata_only

    @staticmethod
    def asc(column: str) -> Tuple[str, str]:
        """Create an ascending shard key column specification.

        Parameters
        ----------
        column : str
            Column name to shard in ascending order

        Returns
        -------
        Tuple[str, str]
            Tuple of (column_name, 'ASC') for use in ShardKey constructor

        Examples
        --------
        >>> ShardKey(ShardKey.asc('user_id'))
        >>> ShardKey(ShardKey.asc('user_id'), ShardKey.desc('category_id'))

        """
        return (column, 'ASC')

    @staticmethod
    def desc(column: str) -> Tuple[str, str]:
        """Create a descending shard key column specification.

        Parameters
        ----------
        column : str
            Column name to shard in descending order

        Returns
        -------
        Tuple[str, str]
            Tuple of (column_name, 'DESC') for use in ShardKey constructor

        Examples
        --------
        >>> ShardKey(ShardKey.desc('user_id'))
        >>> ShardKey(ShardKey.asc('user_id'), ShardKey.desc('category_id'))

        """
        return (column, 'DESC')

    def __repr__(self) -> str:
        args = []

        # Format columns similar to SortKey
        for col_name, direction in self.columns:
            if direction == 'ASC':
                args.append(repr(col_name))
            else:
                args.append(f'({repr(col_name)}, {repr(direction)})')

        # Handle keyword-only parameters
        if self.index_type is not None:
            args.append(f'index_type={repr(self.index_type)}')
        if self.metadata_only:
            args.append('metadata_only=True')

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

    >>> compile_shard_key(ShardKey('user_id', ShardKey.desc('tenant_id')), compiler)
    'SHARD KEY (user_id, tenant_id DESC)'

    >>> compile_shard_key(ShardKey('user_id', 'tenant_id', index_type='HASH'), compiler)
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
        # Add column list with proper escaping and direction support
        column_specs = []
        for col_name, direction in element.columns:
            escaped_col = _escape_column_name(col_name)
            if direction == 'ASC':
                column_specs.append(escaped_col)  # ASC is default, no need to specify
            else:
                column_specs.append(f'{escaped_col} {direction}')

        column_list = ', '.join(column_specs)
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
    *columns : Union[str, Tuple[str, str]]
        Variable number of column specifications. Each can be:
        - A string (column name, defaults to ASC)
        - A tuple of (column_name, direction) where direction is 'ASC' or 'DESC'

    Examples
    --------
    Single column sort key (ascending by default):

    >>> SortKey('created_at')

    Multi-column sort key with mixed directions:

    >>> SortKey('user_id', ('created_at', 'DESC'))

    Using static helper methods:

    >>> SortKey(SortKey.asc('user_id'), SortKey.desc('created_at'))

    """

    def __init__(self, *columns: Union[str, Tuple[str, str]]) -> None:
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
            elif isinstance(col, str):
                self.columns.append((col, 'ASC'))  # Default to ASC
            else:
                # Default to ASC
                raise TypeError(f'Column must be str or tuple, got {type(col).__name__}')

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
        >>> SortKey(SortKey.asc('created_at'))
        >>> SortKey(SortKey.asc('user_id'), SortKey.desc('created_at'))

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
        >>> SortKey(SortKey.desc('created_at'))
        >>> SortKey(SortKey.asc('user_id'), SortKey.desc('created_at'))

        """
        return (column, 'DESC')

    def __repr__(self) -> str:
        parts = []
        for col_name, direction in self.columns:
            if direction == 'ASC':
                parts.append(repr(col_name))
            else:
                parts.append(f'({col_name!r}, {direction!r})')
        return f'SortKey({", ".join(parts)})'


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
    >>> compile_sort_key(SortKey('created_at'), compiler)
    'SORT KEY (created_at)'

    >>> compile_sort_key(SortKey(SortKey.desc('created_at')), compiler)
    'SORT KEY (created_at DESC)'

    >>> compile_sort_key(SortKey('user_id', SortKey.desc('created_at')), compiler)
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
    *columns : str
        Variable number of column names to include in the vector index.
        Usually a single vector column.
    name : str, optional, keyword-only
        Index name for the vector index. If not provided, SingleStore will
        auto-generate a name.
    index_options : str, optional, keyword-only
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

    >>> VectorKey('embedding1', 'embedding2', name='vec_idx')

    """

    name: Optional[str]
    columns: Tuple[str, ...]
    index_options: Optional[str]

    def __init__(
        self,
        *columns: str,
        name: Optional[str] = None,
        index_options: Optional[str] = None,
    ) -> None:
        if not columns:
            raise ValueError(
                'At least one column must be specified for VECTOR index',
            )

        # Validate all columns are strings
        for col in columns:
            if not isinstance(col, str):
                raise TypeError('All column names must be strings')

        self.columns = columns
        self.name = name
        self.index_options = index_options

    def __repr__(self) -> str:
        args = [repr(col) for col in self.columns]

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
    *columns : str
        Variable number of column names to include in the multi-value index.
        Must be JSON columns.
    index_options : str, optional, keyword-only
        JSON string containing multi-value index options.

    Examples
    --------
    Basic multi-value index:

    >>> MultiValueIndex('tags')

    Multiple columns multi-value index:

    >>> MultiValueIndex('tags', 'categories')

    Multi-value index with options:

    >>> MultiValueIndex('tags', index_options='{"some_option":"value"}')

    Notes
    -----
    Multi-value indexes are used to index JSON arrays in SingleStore.
    They allow efficient queries on individual elements within JSON arrays.

    The columns must be of JSON type for multi-value indexing to work properly.
    """

    def __init__(self, *columns: str, index_options: Optional[str] = None) -> None:
        if not columns:
            raise ValueError(
                'At least one column must be specified for MULTI VALUE index',
            )

        # Validate all columns are strings
        for col in columns:
            if not isinstance(col, str):
                raise TypeError('All column names must be strings')

        self.columns = columns
        self.index_options = index_options

    def __repr__(self) -> str:
        args = [repr(col) for col in self.columns]

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
    escaped_columns = [_escape_column_name(col) for col in element.columns]
    column_list = ', '.join(escaped_columns)
    sql = f'MULTI VALUE INDEX ({column_list})'

    if element.index_options:
        sql += f" INDEX_OPTIONS='{element.index_options}'"

    return sql


class FullTextIndex(DDLElement):
    """SingleStore FULLTEXT INDEX DDL element.

    Represents a FULLTEXT INDEX for full-text search on text columns.

    Parameters
    ----------
    *columns : Union[str, Tuple[str, str]]
        Variable number of column specifications. Each can be:
        - A string (column name, defaults to ASC)
        - A tuple of (column_name, direction) where direction is 'ASC' or 'DESC'
        Must be text columns (CHAR, VARCHAR, TEXT, LONGTEXT).
    name : str, optional, keyword-only
        Index name for the fulltext index. If not provided, SingleStore will
        auto-generate a name.
    version : int, optional, keyword-only
        FULLTEXT version to use. Version 1 (default) if not specified.
        Version 2 or higher requires explicit specification.

    Examples
    --------
    Single column, auto-named:

    >>> FullTextIndex('title')

    Single column, named:

    >>> FullTextIndex('title', name='ft_title')

    Multiple columns, auto-named:

    >>> FullTextIndex('title', 'content')

    Multiple columns, named:

    >>> FullTextIndex('title', 'content', name='ft_search')

    With sort directions:

    >>> FullTextIndex('title', ('content', 'DESC'), name='ft_search')

    With version specification:

    >>> FullTextIndex('title', 'content', name='ft_v2', version=2)

    Using static helper methods:

    >>> FullTextIndex(FullTextIndex.asc('title'), FullTextIndex.desc('content'))

    """

    name: Optional[str]
    columns: List[Tuple[str, str]]
    version: Optional[int]

    def __init__(
        self,
        *columns: Union[str, Tuple[str, str]],
        name: Optional[str] = None,
        version: Optional[int] = None,
    ) -> None:
        if not columns:
            raise ValueError(
                'At least one column must be specified for FULLTEXT index',
            )

        self.columns = []
        for col in columns:
            if isinstance(col, tuple):
                column_name, direction = col
                if direction is None:
                    raise TypeError('Direction cannot be None')
                direction = direction.upper()
                if direction not in ('ASC', 'DESC'):
                    raise ValueError(
                        f"Direction must be 'ASC' or 'DESC', got '{direction}'",
                    )
                self.columns.append((column_name, direction))
            elif isinstance(col, str):
                self.columns.append((col, 'ASC'))  # Default to ASC
            else:
                raise TypeError(f'Column must be str or tuple, got {type(col).__name__}')

        self.name = name
        self.version = version

    @staticmethod
    def asc(column: str) -> Tuple[str, str]:
        """Create an ascending fulltext index column specification.

        Parameters
        ----------
        column : str
            Column name to sort in ascending order

        Returns
        -------
        Tuple[str, str]
            Tuple of (column_name, 'ASC') for use in FullTextIndex constructor

        Examples
        --------
        >>> FullTextIndex(FullTextIndex.asc('title'))
        >>> FullTextIndex(FullTextIndex.asc('title'), FullTextIndex.desc('content'))

        """
        return (column, 'ASC')

    @staticmethod
    def desc(column: str) -> Tuple[str, str]:
        """Create a descending fulltext index column specification.

        Parameters
        ----------
        column : str
            Column name to sort in descending order

        Returns
        -------
        Tuple[str, str]
            Tuple of (column_name, 'DESC') for use in FullTextIndex constructor

        Examples
        --------
        >>> FullTextIndex(FullTextIndex.desc('content'))
        >>> FullTextIndex(FullTextIndex.asc('title'), FullTextIndex.desc('content'))

        """
        return (column, 'DESC')

    def __repr__(self) -> str:
        parts = []
        for col_name, direction in self.columns:
            if direction == 'ASC':
                parts.append(repr(col_name))
            else:
                parts.append(f'({col_name!r}, {direction!r})')

        args = [f"{', '.join(parts)}"] if len(parts) > 1 else parts

        if self.name is not None:
            args.append(f'name={repr(self.name)}')
        if self.version is not None:
            args.append(f'version={self.version}')
        return f'FullTextIndex({", ".join(args)})'


class ColumnGroup(DDLElement):
    """SingleStore COLUMN GROUP DDL element.

    Represents a COLUMN GROUP constraint that creates a materialized copy
    of each row as a separate index for columnstore tables, improving
    full-row retrieval/update on wide tables.

    Parameters
    ----------
    name : str, optional, keyword-only
        Optional name for the column group. If not provided, SingleStore
        will auto-generate a name. Must be passed as a keyword argument
        if specified.

    Examples
    --------
    Basic column group with name parameter:

    >>> ColumnGroup(name='cg_all_columns')

    Column group without explicit name (auto-generated):

    >>> ColumnGroup()

    Notes
    -----
    - Only supported on columnstore tables
    - Automatically applies to all columns (uses * syntax)
    - Subset column groups are not supported by SingleStore
    - Improves full-row retrieval/update performance on wide tables
    - Uses less RAM than rowstore for similar use cases
    - If name is not provided, SingleStore will auto-generate one

    """

    name: Optional[str]

    def __init__(self, *, name: Optional[str] = None) -> None:
        if name is not None and not name:
            raise ValueError('Column group name cannot be empty string')
        self.name = name

    def __repr__(self) -> str:
        if self.name is not None:
            return f'ColumnGroup(name={self.name!r})'
        else:
            return 'ColumnGroup()'


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
    - FULLTEXT (column1 ASC, column2 DESC) - with sort directions

    Examples
    --------
    >>> compile_fulltext_index(FullTextIndex('title', 'content'), compiler)
    'FULLTEXT (title, content)'

    >>> compile_fulltext_index(FullTextIndex('title', 'content', name='ft_idx'),
    ...                        compiler)
    'FULLTEXT ft_idx (title, content)'

    >>> compile_fulltext_index(FullTextIndex('title', name='ft_idx', version=2), compiler)
    'FULLTEXT USING VERSION 2 ft_idx (title)'

    >>> compile_fulltext_index(FullTextIndex('title', ('content', 'DESC')), compiler)
    'FULLTEXT (title, content DESC)'

    """
    # Build column list with optional directions
    column_specs = []
    for col_name, direction in element.columns:
        escaped_col = _escape_column_name(col_name)
        if direction == 'ASC':
            column_specs.append(escaped_col)  # ASC is default, no need to specify
        else:
            column_specs.append(f'{escaped_col} {direction}')

    column_list = ', '.join(column_specs)

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


@compiles(ColumnGroup, 'singlestoredb.mysql')
def compile_column_group(element: Any, compiler: Any, **kw: Any) -> str:
    """Compile ColumnGroup DDL element to SQL.

    Parameters
    ----------
    element : ColumnGroup
        The ColumnGroup DDL element to compile
    compiler : DDLCompiler
        SQLAlchemy DDL compiler instance
    **kw : Any
        Additional compiler keyword arguments

    Returns
    -------
    str
        The compiled SQL string for the COLUMN GROUP clause

    Examples
    --------
    >>> compile_column_group(ColumnGroup(name='cg_all'), compiler)
    'COLUMN GROUP cg_all (*)'

    >>> compile_column_group(ColumnGroup(), compiler)
    'COLUMN GROUP (*)'

    """
    if element.name is not None:
        escaped_name = _escape_column_name(element.name)
        return f'COLUMN GROUP {escaped_name} (*)'
    else:
        return 'COLUMN GROUP (*)'
