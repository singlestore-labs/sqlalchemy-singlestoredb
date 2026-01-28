#!/usr/bin/env python
"""Base classes for SingleStoreDB SQLAlchemy objects."""
from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Type
from urllib.parse import quote
from urllib.parse import quote_plus

import sqlalchemy.sql.elements as se
from sqlalchemy import util
from sqlalchemy.dialects.mysql.base import BIT  # noqa: F401
from sqlalchemy.dialects.mysql.base import CHAR  # noqa: F401
from sqlalchemy.dialects.mysql.base import ENUM  # noqa: F401
from sqlalchemy.dialects.mysql.base import MySQLCompiler
from sqlalchemy.dialects.mysql.base import MySQLDDLCompiler
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.dialects.mysql.base import MySQLExecutionContext
from sqlalchemy.dialects.mysql.base import MySQLIdentifierPreparer
from sqlalchemy.dialects.mysql.base import MySQLTypeCompiler
from sqlalchemy.dialects.mysql.base import SET  # noqa: F401
from sqlalchemy.dialects.mysql.base import TEXT  # noqa: F401
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.compiler import BIND_PARAMS
from sqlalchemy.sql.compiler import BIND_PARAMS_ESC
try:
    from sqlalchemy.sql.schema import SchemaConst
except ImportError:
    from sqlalchemy.sql import schema as SchemaConst

from . import reflection
from .column import PersistedColumn
from .compat import supports_statement_cache
from .compat import get_dialect_features
from .compat import has_feature
from .dtypes import _json_deserializer
from .dtypes import JSON
from .dtypes import VECTOR


class CaseInsensitiveDict(Dict[str, Any]):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()
        data = dict(*args, **kwargs)
        self._data = dict((k.lower(), k) for k in data)
        for k in data:
            self[k] = data[k]

    def __contains__(self, k: object) -> bool:
        if isinstance(k, str):
            return k.lower() in self._data
        raise TypeError(k)

    def __delitem__(self, k: str) -> None:
        key = self._data[k.lower()]
        super(CaseInsensitiveDict, self).__delitem__(key)
        del self._data[k.lower()]

    def __getitem__(self, k: str) -> Any:
        key = self._data[k.lower()]
        return super(CaseInsensitiveDict, self).__getitem__(key)

    def get(self, k: str, default: Any = None) -> Any:
        return self[k] if k in self else default

    def __setitem__(self, k: str, v: Any) -> None:
        super(CaseInsensitiveDict, self).__setitem__(k, v)
        self._data[k.lower()] = k


ischema_names = CaseInsensitiveDict(MySQLDialect.ischema_names)
ischema_names['json'] = JSON
ischema_names['vector'] = VECTOR


class SingleStoreDBExecutionContext(MySQLExecutionContext):
    """SingleStoreDB SQLAlchemy execution context."""

    def create_server_side_cursor(self) -> Optional[Any]:
        """Create a server-side cursor for streaming results."""
        if self.dialect.supports_server_side_cursors:
            cursor = self._dbapi_connection.cursor()
            # Configure cursor for server-side operation if supported by driver
            if hasattr(cursor, 'buffered'):
                cursor.buffered = False
            return cursor
        return None

    def handle_dbapi_exception(self, e: Any) -> Any:
        """Handle DBAPI exceptions with proper error context."""
        return super().handle_dbapi_exception(e)


class Array(se.Tuple):
    __visit_name__ = 'array'


array = Array


class SingleStoreDBCompiler(MySQLCompiler):
    """SingleStoreDB SQLAlchemy compiler."""

    def visit_array(self, clauselist: Any, **kw: Any) -> str:
        return '[%s]' % self.visit_clauselist(clauselist, **kw)

    def visit_typeclause(
        self, typeclause: Any, type_: Optional[Any] = None, **kw: Any,
    ) -> Optional[str]:
        if type_ is None:
            type_ = typeclause.type.dialect_impl(self.dialect)
        if isinstance(type_, sqltypes.TypeDecorator):
            return self.visit_typeclause(typeclause, type_.impl, **kw)
        elif isinstance(type_, sqltypes.Integer):
            if getattr(type_, 'unsigned', False):
                return 'UNSIGNED INTEGER'
            else:
                return 'SIGNED INTEGER'
        elif isinstance(type_, sqltypes.TIMESTAMP):
            return 'TIMESTAMP'
        elif isinstance(
            type_,
            (
                sqltypes.DECIMAL,
                sqltypes.DateTime,
                sqltypes.Date,
                sqltypes.Time,
            ),
        ):
            return self.dialect.type_compiler.process(type_)
        elif isinstance(type_, sqltypes.String) and not isinstance(
            type_, (ENUM, SET),
        ):
            adapted = CHAR._adapt_string_for_cast(type_)
            return self.dialect.type_compiler.process(adapted)
        elif isinstance(type_, VECTOR):
            return 'VECTOR'
        elif isinstance(type_, sqltypes._Binary):
            return 'BINARY'
        elif isinstance(type_, sqltypes.JSON):
            return 'JSON'
        elif isinstance(type_, sqltypes.NUMERIC):
            return self.dialect.type_compiler.process(type_).replace(
                'NUMERIC', 'DECIMAL',
            )
        elif isinstance(type_, sqltypes.Float):
            return self.dialect.type_compiler.process(type_)
        elif isinstance(type_, sqltypes.Boolean):
            return 'BOOL'
        else:
            return None

    def visit_cast(self, cast: Any, **kw: Any) -> str:
        type_ = self.process(cast.typeclause)
        if type_ is None:
            util.warn(
                'Datatype %s does not support CAST on SingleStoreDB; '
                'the CAST will be skipped.'
                % self.dialect.type_compiler.process(cast.typeclause.type),
            )
            return self.process(cast.clause.self_group(), **kw)

        # Use the older cast function for strings. The new cast operator
        # will truncate numeric values without a supplied length.
        if 'DOUBLE' in type_ or 'FLOAT' in type_ or 'BOOL' in type_ or \
                'JSON' in type_ or 'TIMESTAMP' in type_ or 'DATETIME' in type_:
            return '%s :> %s' % (self.process(cast.clause, **kw), type_)

        return 'CAST(%s AS %s)' % (self.process(cast.clause, **kw), type_)

    def post_process_text(self, text: str, has_params: bool = False) -> str:
        if has_params and self.preparer._double_percents:
            text = text.replace('%', '%%')
        return text

#   def escape_literal_column(self, text: str, has_params: bool = False) -> str:
#       if has_params and self.preparer._double_percents:
#           text = text.replace("%", "%%")
#       return text

    def visit_textclause(
        self,
        textclause: Any,
        add_to_result_map: Any = None,
        **kw: Any,
    ) -> str:
        def do_bindparam(m: Any) -> str:
            name = m.group(1)
            if name in textclause._bindparams:
                return self.process(textclause._bindparams[name], **kw)
            else:
                return self.bindparam_string(name, **kw)

        if not self.stack:
            self.isplaintext = True

        if add_to_result_map:
            # text() object is present in the columns clause of a
            # select().   Add a no-name entry to the result map so that
            # row[text()] produces a result
            add_to_result_map(None, None, (textclause,), sqltypes.NULLTYPE)

        has_params = len(textclause._bindparams) > 0

        # un-escape any \:params
        return BIND_PARAMS_ESC.sub(
            lambda m: m.group(1),
            BIND_PARAMS.sub(
                do_bindparam, self.post_process_text(
                    textclause.text,
                    has_params=has_params,
                ),
            ),
        )

    def visit_primary_key_constraint(self, constraint: Any, **kw: Any) -> str:
        text = super().visit_primary_key_constraint(constraint)
        using = constraint.dialect_options['mysql']['using']
        if using:
            text += ' USING %s' % (self.preparer.quote(using))
        return text


class SingleStoreDBDDLCompiler(MySQLDDLCompiler):
    """SingleStoreDB SQLAlchemy DDL compiler."""

    def post_create_table(self, table: Any) -> str:
        """Build table-level CREATE options, including SingleStore-specific options."""
        table_opts = []

        # Get opts the same way MySQL does, but filter out our DDL element options
        opts = dict(
            (k[len(self.dialect.name) + 1:].upper(), v)
            for k, v in table.kwargs.items()
            if k.startswith('%s_' % self.dialect.name)
            and k not in {
                'singlestoredb_shard_key',
                'singlestoredb_sort_key',
                'singlestoredb_vector_key',
                'singlestoredb_full_text_index',
                'singlestoredb_multi_value_index',
                'singlestoredb_column_group',
                'singlestoredb_table_type',
            }
        )

        if table.comment is not None:
            opts['COMMENT'] = table.comment

        # Handle SingleStore-specific table options with proper formatting
        singlestore_opts = {
            'AUTOSTATS_ENABLED': ['TRUE', 'FALSE'],
            'AUTOSTATS_CARDINALITY_MODE': ['INCREMENTAL', 'PERIODIC', 'OFF'],
            'AUTOSTATS_HISTOGRAM_MODE': ['CREATE', 'UPDATE', 'OFF'],
            'AUTOSTATS_SAMPLING': ['ON', 'OFF'],
            'COMPRESSION': ['SPARSE'],
        }

        # Boolean conversion mappings for SingleStore options
        # For options that accept OFF, False maps to OFF
        # For AUTOSTATS_ENABLED, False maps to FALSE (specific to that option)
        # For AUTOSTATS_SAMPLING, True maps to ON (specific to that option)
        boolean_mappings = {
            'AUTOSTATS_ENABLED': {True: 'TRUE', False: 'FALSE'},
            'AUTOSTATS_CARDINALITY_MODE': {False: 'OFF'},  # Only False->OFF
            'AUTOSTATS_HISTOGRAM_MODE': {False: 'OFF'},    # Only False->OFF
            'AUTOSTATS_SAMPLING': {True: 'ON', False: 'OFF'},
        }

        # Process remaining options
        for opt, arg in opts.items():
            # Handle boolean values for specific SingleStore options
            if opt in boolean_mappings and isinstance(arg, bool):
                if arg in boolean_mappings[opt]:
                    arg_str = boolean_mappings[opt][arg]
                else:
                    # Boolean value not supported for this option, convert to string
                    arg_str = str(arg)
            else:
                arg_str = str(arg)

            # Handle SingleStore-specific options with validation
            if opt in singlestore_opts:
                if arg_str.upper() in singlestore_opts[opt]:
                    table_opts.append(f'{opt} = {arg_str.upper()}')
                else:
                    valid_values = ', '.join(singlestore_opts[opt])
                    raise ValueError(
                        f'Invalid value "{arg_str}" for {opt}. '
                        f'Valid values are: {valid_values}',
                    )
            else:
                # Standard table options
                table_opts.append(f'{opt}={arg_str}')

        if table_opts:
            return ' ' + ', '.join(table_opts)
        else:
            return ''

    def visit_create_table(self, create: Any, **kw: Any) -> str:
        """Generate CREATE TABLE DDL with SingleStore-specific extensions.

        Handles table type prefixes (ROWSTORE, COLUMNSTORE, REFERENCE, TEMPORARY, etc.)
        and DDL elements (SHARD KEY, SORT KEY, VECTOR INDEX, MULTI VALUE INDEX,
        FULLTEXT INDEX, and COLUMN GROUP constraints) with all syntax variants.
        """
        # Get dialect options for SingleStore table type
        dialect_opts = create.element.dialect_options.get('singlestoredb', {})
        table_type = dialect_opts.get('table_type')

        # Handle table type prefixes before calling super()
        if table_type is not None:
            from sqlalchemy_singlestoredb.ddlelement import (
                TableType,
                RowStore,
            )

            if not isinstance(table_type, TableType):
                raise TypeError(
                    f'singlestoredb_table_type must be a RowStore or '
                    f'ColumnStore instance, got {type(table_type).__name__}',
                )

            # Build the appropriate prefixes based on table type and modifiers
            prefixes = []

            if isinstance(table_type, RowStore):
                # RowStore always gets ROWSTORE prefix
                prefixes.append('ROWSTORE')
            # ColumnStore doesn't get a prefix (it's the default)

            # Add modifier prefixes in the correct order
            if table_type.reference:
                prefixes.append('REFERENCE')
            elif table_type.global_temporary:
                prefixes.extend(['GLOBAL', 'TEMPORARY'])
            elif table_type.temporary:
                prefixes.append('TEMPORARY')

            # Store the original prefixes and add ours
            original_prefixes = (
                list(create.element._prefixes)
                if create.element._prefixes else []
            )
            create.element._prefixes = prefixes + original_prefixes

        create_table_sql = super().visit_create_table(create, **kw)

        # Restore original prefixes if we modified them
        if table_type is not None:
            create.element._prefixes = original_prefixes

        # Collect all DDL elements to append
        ddl_elements = []

        # Handle shard key (single value only)
        shard_key = dialect_opts.get('shard_key')
        if shard_key is not None:
            from sqlalchemy_singlestoredb.ddlelement import compile_shard_key
            shard_key_sql = compile_shard_key(shard_key, self)
            ddl_elements.append(shard_key_sql)

        # Handle sort key (single value only)
        sort_key = dialect_opts.get('sort_key')
        if sort_key is not None:
            from sqlalchemy_singlestoredb.ddlelement import compile_sort_key
            sort_key_sql = compile_sort_key(sort_key, self)
            ddl_elements.append(sort_key_sql)

        # Handle vector keys (single value or list)
        vector_key = dialect_opts.get('vector_key')
        if vector_key is not None:
            from sqlalchemy_singlestoredb.ddlelement import compile_vector_key
            # Ensure it's a list for uniform processing
            vector_keys = vector_key if isinstance(vector_key, list) else [vector_key]
            for vector_index in vector_keys:
                vector_index_sql = compile_vector_key(vector_index, self)
                ddl_elements.append(vector_index_sql)

        # Handle multi-value indexes (single value or list)
        multi_value_index = dialect_opts.get('multi_value_index')
        if multi_value_index is not None:
            from sqlalchemy_singlestoredb.ddlelement import compile_multi_value_index
            # Ensure it's a list for uniform processing
            multi_value_indexes = multi_value_index if isinstance(
                multi_value_index, list,
            ) else [multi_value_index]
            for mv_index in multi_value_indexes:
                mv_index_sql = compile_multi_value_index(mv_index, self)
                ddl_elements.append(mv_index_sql)

        # Handle fulltext index (single value only - SingleStore limitation)
        full_text_index = dialect_opts.get('full_text_index')
        if full_text_index is not None:
            from sqlalchemy_singlestoredb.ddlelement import compile_fulltext_index
            ft_index_sql = compile_fulltext_index(full_text_index, self)
            ddl_elements.append(ft_index_sql)

        # Handle column group (single value only)
        column_group = dialect_opts.get('column_group')
        if column_group is not None:
            from sqlalchemy_singlestoredb.ddlelement import compile_column_group
            column_group_sql = compile_column_group(column_group, self)
            ddl_elements.append(column_group_sql)

        # If we have DDL elements to add, modify the SQL
        if ddl_elements:
            # We need to handle the case where table options might be present
            sql_stripped = create_table_sql.rstrip()

            # Look for table options (they come after the closing parenthesis)
            # Pattern: ") TABLE_OPTION1=value1 TABLE_OPTION2=value2"
            closing_paren_pos = sql_stripped.rfind(')')

            if closing_paren_pos != -1:
                # Split into table definition and table options parts
                table_def_part = sql_stripped[:closing_paren_pos]  # Before ')'
                table_options_part = sql_stripped[closing_paren_pos + 1:]  # After ')'

                # Add DDL elements inside the table definition
                # Remove trailing newline from table definition and add comma
                table_def_clean = table_def_part.rstrip()
                formatted_ddl_elements = ',\n\t'.join(ddl_elements)

                # Reconstruct with proper formatting
                create_table_sql = (
                    f'{table_def_clean},\n\t{formatted_ddl_elements}\n)'
                    f'{table_options_part}'
                )
            else:
                # This shouldn't happen with normal CREATE TABLE, but handle it
                ddl_part = ',\n\t' + ',\n\t'.join(ddl_elements)
                create_table_sql = f'{sql_stripped}{ddl_part}'

        return create_table_sql

    def get_column_specification(self, column: Any, **kw: Any) -> str:
        """Builds column DDL."""
        if not isinstance(column, PersistedColumn):
            return super().get_column_specification(column, **kw)

        if (
            self.dialect.is_mariadb is True
            and column.computed is not None
            and column._user_defined_nullable is SchemaConst.NULL_UNSPECIFIED
        ):
            column.nullable = True
        colspec = [
            self.preparer.format_column(column),
            'AS',
            column.persisted_expression,
            'PERSISTED',
            self.dialect.type_compiler.process(
                column.type, type_expression=column,
            ),
        ]

        if column.computed is not None:
            colspec.append(self.process(column.computed))

        is_timestamp = isinstance(
            column.type._unwrapped_dialect_impl(self.dialect),
            sqltypes.TIMESTAMP,
        )

        if not column.nullable:
            colspec.append('NOT NULL')

        # see: https://docs.sqlalchemy.org/en/latest/dialects/mysql.html#mysql_timestamp_null  # noqa
        elif column.nullable and is_timestamp:
            colspec.append('NULL')

        comment = column.comment
        if comment is not None:
            literal = self.sql_compiler.render_literal_value(
                comment, sqltypes.String(),
            )
            colspec.append('COMMENT ' + literal)

        default = self.get_column_default_string(column)
        if default is not None:
            colspec.append('DEFAULT ' + default)
        return ' '.join(colspec)


class SingleStoreDBTypeCompiler(MySQLTypeCompiler):
    """SingleStoreDB SQLAlchemy type compiler."""

    def visit_DATETIME(self, type_: Any, **kw: Any) -> str:
        if getattr(type_, 'fsp', None):
            return 'DATETIME(%d)' % type_.fsp
        return 'DATETIME'

    def visit_TIMESTAMP(self, type_: Any, **kw: Any) -> str:
        if getattr(type_, 'fsp', None):
            return 'TIMESTAMP(%d)' % type_.fsp
        return 'TIMESTAMP'

    def visit_VECTOR(self, type_: Any, **kw: Any) -> str:
        return 'VECTOR(%d, %s)' % (type_.n_elems, type_.elem_type)


class SingleStoreDBIdentifierPreparer(MySQLIdentifierPreparer):
    """SingleStoreDB SQLAlchemy identifier preparer."""

    @property
    def _double_percents(self) -> bool:
        return self.dialect._double_percents

    @_double_percents.setter
    def _double_percents(self, value: bool) -> None:
        pass

    def _escape_identifier(self, value: str) -> str:
        value = value.replace(self.escape_quote, self.escape_to_quote)
        if self.dialect._double_percents:
            value = value.replace('%', '%%')
        return value


class _myconnpyBIT(BIT):
    def result_processor(self, dialect: Any, coltype: Any) -> Any:
        """MySQL-connector already converts mysql bits, so."""
        return None


def render_as_string(url: URL) -> str:
    s = url.drivername + '://'
    if url.username is not None:
        s += quote(url.username)
        if url.password is not None:
            s += ':' + quote(str(url.password))
        s += '@'
    if url.host is not None:
        if ':' in url.host:
            s += f'[{url.host}]'
        else:
            s += url.host
    if url.port is not None:
        s += ':' + str(url.port)
    if url.database is not None:
        s += '/' + url.database
    if url.query:
        keys = list(url.query)
        keys.sort()
        s += '?' + '&'.join(
            f'{quote_plus(k)}={quote_plus(element)}'
            for k in keys
            for element in util.to_list(url.query[k])
        )
    return s


class SingleStoreDBDialect(MySQLDialect):
    """SingleStoreDB SQLAlchemy dialect."""

    name = 'singlestoredb'

    default_paramstyle = 'pyformat'

    ischema_names = ischema_names

    # Possibly mysql.connector-specific
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_native_decimal = True
    supports_server_side_cursors = True
    colspecs = util.update_copy(MySQLDialect.colspecs, {BIT: _myconnpyBIT})

    statement_compiler = SingleStoreDBCompiler
    ddl_compiler = SingleStoreDBDDLCompiler
    type_compiler = SingleStoreDBTypeCompiler
    preparer = SingleStoreDBIdentifierPreparer
    execution_ctx_cls = SingleStoreDBExecutionContext

    driver = ''

    supports_statement_cache = supports_statement_cache()

    _double_percents = True

    def __init__(
        self,
        isolation_level: Optional[str] = None,
        json_serializer: Optional[Callable[..., Any]] = None,
        json_deserializer: Optional[Callable[..., Any]] = None,
        **kwargs: Any,
    ):
        MySQLDialect.__init__(
            self, isolation_level=isolation_level,
            json_serializer=json_serializer,
            json_deserializer=json_deserializer or _json_deserializer,
            is_mariadb=False, **kwargs,
        )

        # Apply SQLAlchemy version-specific dialect features
        dialect_features = get_dialect_features()
        for feature_name, feature_value in dialect_features.items():
            if hasattr(self, feature_name):
                setattr(self, feature_name, feature_value)

        # Enable server-side cursors if supported
        if has_feature('server_side_cursors'):
            self.supports_server_side_cursors = True

    @classmethod
    def dbapi(cls) -> Any:
        return __import__('singlestoredb')

    @classmethod
    def import_dbapi(cls) -> Any:
        return __import__('singlestoredb')

    def initialize(self, connection: Any) -> Any:
        if hasattr(connection.connection, 'driver'):
            self.driver = connection.connection.driver
        else:
            self.driver = connection.connection._driver.name

        params = connection.connection.connection_params
        if params['host'] == 'singlestore.com':
            self.server_version_info = None
            self.default_schema_name = None
            self.default_isolation_level = 'READ COMMITTED'
            return

        # Initialize connection pool optimization settings
        result = MySQLDialect.initialize(self, connection)

        # Configure pool settings based on SingleStore capabilities
        if hasattr(connection.engine.pool, 'pre_ping'):
            connection.engine.pool.pre_ping = True

        return result

    def create_connect_args(self, url: URL) -> List[Any]:
        from singlestoredb.connection import build_params
        return [
            [],
            build_params(host=render_as_string(url), client_found_rows=True),
        ]

    def _extract_error_code(self, exception: Exception) -> int:
        return getattr(exception, 'errno', -1)

    def _detect_charset(self, connection: Any) -> str:
        return 'utf8mb4'

    @util.memoized_property
    def _tabledef_parser(self) -> reflection.SingleStoreDBTableDefinitionParser:
        """
        Return the SingleStoreDBTableDefinitionParser.

        The deferred creation ensures that the dialect has
        retrieved server version information first.

        Returns
        -------
        reflection.SingleStoreDBTableDefinitionParser

        """
        from . import reflection
        preparer = self.identifier_preparer
        return reflection.SingleStoreDBTableDefinitionParser(self, preparer)

    def do_rollback(self, dbapi_connection: Any) -> None:
        params = dbapi_connection.connection_params
        if params['host'] == 'singlestore.com':
            return
        dbapi_connection.rollback()

    def _execute_context(
        self,
        dialect: Any,
        constructor: Any,
        statement: Any,
        parameters: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Override to handle stream_results execution option."""
        execution_options = getattr(constructor, 'execution_options', {})

        # Check for stream_results option
        stream_results = execution_options.get('stream_results', False)
        if stream_results and self.supports_server_side_cursors:
            # Enable server-side cursor for streaming
            kwargs.setdefault('server_side_cursor', True)

        return super()._execute_context(
            dialect, constructor, statement, parameters, *args, **kwargs,
        )

    def get_default_isolation_level(self, dbapi_conn: Any) -> str:
        """Get the default isolation level."""
        params = getattr(dbapi_conn, 'connection_params', {})
        if params.get('host') == 'singlestore.com':
            return 'READ COMMITTED'
        return super().get_default_isolation_level(dbapi_conn)

    def do_ping(self, dbapi_connection: Any) -> bool:
        """Ping the database connection to test if it's alive."""
        try:
            # Try to execute a simple query
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute('SELECT 1')
                cursor.fetchone()
                return True
            finally:
                cursor.close()
        except Exception:
            return False

    def is_disconnect(
        self, e: Any, connection: Any, cursor: Any,
    ) -> bool:
        """Check if an exception indicates a disconnected state."""
        if super().is_disconnect(e, connection, cursor):
            return True

        # SingleStore-specific disconnect error codes
        error_codes = {
            2006,  # MySQL server has gone away
            2013,  # Lost connection to MySQL server during query
            2055,  # Lost connection to server at '%s', system error: %d
        }

        if hasattr(e, 'errno') and e.errno in error_codes:
            return True

        # Check error message for disconnect indicators
        error_msg = str(e).lower()
        disconnect_indicators = [
            'connection lost',
            'server has gone away',
            'lost connection',
            'broken pipe',
            'connection reset',
        ]

        return any(indicator in error_msg for indicator in disconnect_indicators)

    def get_table_options(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any,
    ) -> Dict[str, Any]:
        """Reflect table options including SingleStore-specific features."""
        options = super().get_table_options(
            connection, table_name, schema=schema, **kw,
        )

        # Parse the CREATE TABLE statement to extract SingleStore features
        parsed_state = self._parsed_state_or_create(
            connection, table_name, schema, **kw,
        )

        # Convert parsed SingleStore features back to dialect options
        if hasattr(parsed_state, 'singlestore_features'):
            from sqlalchemy_singlestoredb.ddlelement import (
                ShardKey, SortKey, VectorKey, RowStore, ColumnStore,
            )

            for feature_type, spec in parsed_state.singlestore_features.items():
                if feature_type == 'shard_key':
                    # Convert parsed spec back to ShardKey object
                    # Handle multiple formats:
                    # 1. New SingleStore format: [(col_name, direction), ...]
                    # 2. MySQL fallback format: [(col_name, direction, extra), ...]
                    # 3. Legacy format: [col_name, ...]
                    columns = spec['columns']
                    column_specs = []

                    if columns and isinstance(columns[0], tuple):
                        # Check if this is our new format or MySQL format
                        first_tuple = columns[0]
                        if (
                            len(first_tuple) == 2 and isinstance(first_tuple[1], str) and
                            first_tuple[1] in ('ASC', 'DESC')
                        ):
                            # New SingleStore format: [(col_name, direction), ...]
                            column_specs = columns
                        else:
                            # MySQL parser format: [(col_name, direction, extra), ...]
                            # Extract just the column names
                            column_specs = [(col[0], 'ASC') for col in columns if col[0]]
                    else:
                        # Legacy SingleStore parser format: [col_name, ...]
                        column_specs = [(col, 'ASC') for col in columns]

                    # Check for metadata_only flag
                    metadata_only = spec.get('metadata_only', False)
                    shard_key = ShardKey(*column_specs, metadata_only=metadata_only)
                    options['singlestoredb_shard_key'] = shard_key

                elif feature_type == 'sort_key':
                    # Convert parsed spec back to SortKey object
                    # Handle multiple formats (same logic as shard_key)
                    columns = spec['columns']
                    column_specs = []

                    if columns and isinstance(columns[0], tuple):
                        # Check if this is our new format or MySQL format
                        first_tuple = columns[0]
                        if (
                            len(first_tuple) == 2 and isinstance(first_tuple[1], str) and
                            first_tuple[1] in ('ASC', 'DESC')
                        ):
                            # New SingleStore format: [(col_name, direction), ...]
                            column_specs = columns
                        else:
                            # MySQL parser format: [(col_name, direction, extra), ...]
                            # Extract just the column names
                            column_specs = [(col[0], 'ASC') for col in columns if col[0]]
                    else:
                        # Legacy SingleStore parser format: [col_name, ...]
                        column_specs = [(col, 'ASC') for col in columns]

                    sort_key = SortKey(*column_specs)
                    options['singlestoredb_sort_key'] = sort_key

                elif feature_type == 'vector_key':
                    # Convert parsed spec back to VectorKey object
                    # Handle both SingleStore format (list of strings) and MySQL
                    # fallback format (list of tuples)
                    columns = spec['columns']
                    if columns and isinstance(columns[0], tuple):
                        # MySQL parser format: [(col_name, direction, extra), ...]
                        # Extract just the column names
                        column_names = [col[0] for col in columns if col[0]]
                    else:
                        # SingleStore parser format: [col_name, ...]
                        column_names = columns

                    vector_key = VectorKey(
                        *column_names,
                        name=spec.get('name'),
                        index_options=spec.get('index_options'),
                    )
                    # For vector keys, store as list if multiple exist
                    existing = options.get('singlestoredb_vector_key')
                    if existing:
                        if isinstance(existing, list):
                            existing.append(vector_key)
                        else:
                            options['singlestoredb_vector_key'] = [existing, vector_key]
                    else:
                        options['singlestoredb_vector_key'] = vector_key

                elif feature_type == 'table_type':
                    # Convert parsed table type spec back to TableType object
                    is_rowstore = spec.get('is_rowstore', False)
                    is_temporary = spec.get('is_temporary', False)
                    is_global_temporary = spec.get('is_global_temporary', False)
                    is_reference = spec.get('is_reference', False)

                    if is_rowstore:
                        # Create RowStore with appropriate modifiers
                        table_type = RowStore(
                            temporary=is_temporary,
                            global_temporary=is_global_temporary,
                            reference=is_reference,
                        )
                    else:
                        # Default to ColumnStore (handles CREATE TABLE without ROWSTORE)
                        # Note: ColumnStore doesn't support global_temporary
                        table_type = ColumnStore(
                            temporary=is_temporary,
                            reference=is_reference,
                        )

                    options['singlestoredb_table_type'] = table_type

        return options

    def on_connect(self) -> Optional[Callable[[Any], None]]:
        """Return a callable that will be executed on new connections."""
        def connect(dbapi_connection: Any) -> None:
            # Set connection charset
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute("SET NAMES 'utf8mb4'")
                # Set session variables if needed
                cursor.execute("SET sql_mode = 'TRADITIONAL'")
            except Exception:
                # Ignore errors for cloud connections or unsupported features
                pass
            finally:
                cursor.close()

        return connect


dialect: Type[SingleStoreDBDialect] = SingleStoreDBDialect
