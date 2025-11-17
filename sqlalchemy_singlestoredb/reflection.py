#!/usr/bin/env python
"""SingleStoreDB SQLAlchemy reflection utilities."""
from __future__ import annotations

import re
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

from sqlalchemy import log
from sqlalchemy import util
from sqlalchemy.dialects.mysql.reflection import MySQLTableDefinitionParser
from sqlalchemy.dialects.mysql.reflection import ReflectedState
from sqlalchemy.sql import sqltypes

from . import DATETIME
from . import ENUM
from . import SET
from . import TIME
from . import TIMESTAMP


_control_char_map = {
    '\\\\': '\\',
    '\\0': '\0',
    '\\a': '\a',
    '\\b': '\b',
    '\\t': '\t',
    '\\n': '\n',
    '\\v': '\v',
    '\\f': '\f',
    '\\r': '\r',
    # '\\e':'\e',
}
_control_char_regexp = re.compile(
    '|'.join(re.escape(k) for k in _control_char_map),
)


def _re_compile(regex: str) -> Any:
    """Compile a string to regex, I and UNICODE."""
    return re.compile(regex, re.I | re.UNICODE)


def _strip_values(values: List[str]) -> List[str]:
    """Strip reflected values quotes"""
    strip_values = []
    for a in values:
        if a[0:1] == '"' or a[0:1] == "'":
            # strip enclosing quotes and unquote interior
            a = a[1:-1].replace(a[0] * 2, a[0])
        strip_values.append(a)
    return strip_values


def cleanup_text(raw_text: str) -> str:
    if '\\' in raw_text:
        raw_text = re.sub(
            _control_char_regexp, lambda s: _control_char_map[s[0]], raw_text,
        )
    return raw_text.replace("''", "'")


@log.class_logger
class SingleStoreDBTableDefinitionParser(MySQLTableDefinitionParser):
    """Parses the results of a SHOW CREATE TABLE statement."""

    def _parse_constraints(self, line: str) -> Tuple[str, Dict[str, Any]]:
        # Check for SingleStore table options that might not be caught by parent parser
        # These typically start with whitespace and contain options like COMPRESSION
        stripped_line = line.strip()
        if (
            stripped_line and
            (
                'COMPRESSION=' in stripped_line or
                'AUTOSTATS_' in stripped_line
            ) and
            not stripped_line.startswith(
                (
                    'KEY ', 'SHARD KEY', 'SORT KEY',
                    'VECTOR ', 'PRIMARY KEY', 'FOREIGN KEY',
                    'UNIQUE ', 'INDEX ', 'FULLTEXT ',
                ),
            )
        ):
            # This looks like a table options line that wasn't caught by MySQL parser
            # Return a special type to avoid the "Unknown schema content" warning
            return 'singlestore_table_option', {'line': line}

        # First try to match COLUMN GROUP pattern
        m = self._re_column_group.match(line)
        if m:
            spec = m.groupdict()

            # Create spec dictionary with parsed information
            parsed_spec = {
                'name': spec.get('name'),
            }

            return 'column_group', parsed_spec

        # Next try to match VECTOR INDEX pattern
        m = self._re_vector_index.match(line)
        if m:
            spec = m.groupdict()

            # Parse columns
            columns_str = spec.get('columns', '').strip()
            if columns_str:
                columns = [
                    col.strip().strip('`') for col in columns_str.split(',')
                    if col.strip()
                ]
            else:
                columns = []

            # Create spec dictionary with parsed information
            parsed_spec = {
                'type': 'VECTOR',
                'name': spec.get('name'),
                'columns': columns,
                'index_options': spec.get('index_options'),
                'only': False,  # VECTOR INDEX doesn't have ONLY modifier
            }

            return 'vector_key', parsed_spec

        # Next try to match SHARD KEY and SORT KEY patterns
        m = self._re_singlestore_key.match(line)
        if m:
            spec = m.groupdict()
            key_type = spec['type'].lower()  # 'shard' or 'sort'

            # Parse columns - handle both empty () and actual column lists with ASC/DESC
            columns_str = spec.get('columns', '').strip()
            columns = []
            if columns_str:
                # Parse column names with ASC/DESC directions
                # Example: "user_id DESC, category_id" ->
                # [("user_id", "DESC"), ("category_id", "ASC")]
                import re

                # Split by comma and process each column specification
                for col_spec in columns_str.split(','):
                    col_spec = col_spec.strip()
                    if not col_spec:
                        continue

                    # Check for explicit DESC or ASC at the end
                    direction_match = re.match(
                        r'^(.+?)\s+(DESC|ASC)\s*$', col_spec, re.IGNORECASE,
                    )
                    if direction_match:
                        col_name = direction_match.group(1).strip().strip('`')
                        direction = direction_match.group(2).upper()
                        columns.append((col_name, direction))
                    else:
                        # No explicit direction, default to ASC
                        col_name = col_spec.strip().strip('`')
                        columns.append((col_name, 'ASC'))

            # Create spec dictionary with parsed information
            parsed_spec = {
                'type': spec['type'],  # 'SHARD' or 'SORT'
                'name': spec.get('name'),  # Optional key name
                'columns': columns,
                'only': spec.get('only') is not None,  # True if ONLY was specified
                'metadata_only': spec.get('metadata_only') is not None,
            }

            return f'{key_type}_key', parsed_spec

        # Check if this line matches our extended KEY pattern with ALL COLUMNS
        # We need to handle this before calling parent to avoid passing
        # None to _parse_keyexprs
        m = self._re_key.match(line)
        if m and m.groupdict().get('all_columns'):
            # This is a column group with ALL COLUMNS syntax
            spec = m.groupdict()

            # Merge name_quoted and name_unquoted into single name field
            if spec.get('name_quoted'):
                spec['name'] = spec['name_quoted']
                del spec['name_quoted']
            elif spec.get('name_unquoted'):
                spec['name'] = spec['name_unquoted']
                del spec['name_unquoted']

            # Set columns to empty list for ALL COLUMNS
            spec['columns'] = []
            del spec['all_columns']

            # Process other fields that parent would normally process
            if spec.get('parser'):
                spec['parser'] = self.preparer.unformat_identifiers(
                    spec['parser'],
                )[0]

            return 'key', spec

        # Fall back to parent class parsing
        type_, spec = super(
            SingleStoreDBTableDefinitionParser,
            self,
        )._parse_constraints(line)

        # Merge name_quoted and name_unquoted into single name field
        if type_ == 'key' and spec:
            if spec.get('name_quoted'):
                spec['name'] = spec['name_quoted']
                del spec['name_quoted']
            elif spec.get('name_unquoted'):
                spec['name'] = spec['name_unquoted']
                del spec['name_unquoted']

        # Check if this is a SHARD KEY, SORT KEY, or VECTOR KEY line
        # that was recognized by parent
        if type_ == 'key' and spec.get('type') in ('SHARD', 'SORT', 'VECTOR'):
            # These are SingleStore-specific KEY types, not regular indexes
            # We'll mark them as recognized by returning a special type
            # that won't trigger the warning in MySQL's get_indexes
            type_ = f"{spec['type'].lower()}_key"

        return type_, spec

    def _parse_table_name(self, line: str, state: Any) -> None:
        """Parse CREATE TABLE line to extract table name and table type information."""
        # Call parent to handle standard parsing
        super()._parse_table_name(line, state)

        # Parse table type information from CREATE statement
        # Patterns we need to detect:
        # CREATE ROWSTORE TABLE -> RowStore()
        # CREATE ROWSTORE TEMPORARY TABLE -> RowStore(temporary=True)
        # CREATE ROWSTORE GLOBAL TEMPORARY TABLE -> RowStore(global_temporary=True)
        # CREATE ROWSTORE REFERENCE TABLE -> RowStore(reference=True)
        # CREATE TEMPORARY TABLE -> ColumnStore(temporary=True)
        # CREATE REFERENCE TABLE -> ColumnStore(reference=True)
        # CREATE TABLE -> ColumnStore() (default)

        line_clean = line.strip()

        # Parse table type specification
        is_rowstore = 'ROWSTORE' in line_clean
        is_global_temporary = 'GLOBAL TEMPORARY' in line_clean
        # Exclude GLOBAL TEMPORARY from regular TEMPORARY
        is_temporary = 'TEMPORARY' in line_clean and not is_global_temporary
        is_reference = 'REFERENCE' in line_clean

        # Store table type info for later conversion to dialect options
        if not hasattr(state, 'singlestore_features'):
            state.singlestore_features = {}

        table_type_spec = {
            'is_rowstore': is_rowstore,
            'is_temporary': is_temporary,
            'is_global_temporary': is_global_temporary,
            'is_reference': is_reference,
        }

        state.singlestore_features['table_type'] = table_type_spec

    def parse(self, show_create: str, charset: str) -> ReflectedState:
        state = ReflectedState()
        state.charset = charset
        for line in re.split(r'\r?\n', show_create):
            if line.startswith('  ' + self.preparer.initial_quote):
                self._parse_column(line, state)
            # a regular table options line
            elif line.startswith(') '):
                self._parse_table_options(line, state)
            # an ANSI-mode table options line
            elif line == ')':
                pass
            elif line.startswith('CREATE '):
                self._parse_table_name(line, state)
            # Not present in real reflection, but may be if
            # loading from a file.
            elif not line:
                pass
            else:
                type_, spec = self._parse_constraints(line)
                if type_ is None:
                    util.warn('Unknown schema content: %r' % line)
                elif type_ == 'key':
                    state.keys.append(spec)
                elif type_ == 'fk_constraint':
                    state.fk_constraints.append(spec)
                elif type_ == 'ck_constraint':
                    state.ck_constraints.append(spec)
                elif type_ in ('shard_key', 'sort_key', 'vector_key', 'column_group'):
                    # Store SingleStore features for later conversion to dialect options
                    if not hasattr(state, 'singlestore_features'):
                        state.singlestore_features = {}
                    state.singlestore_features[type_] = spec
                elif type_ == 'singlestore_table_option':
                    # Silently ignore SingleStore table options to avoid warnings
                    # as they're not regular indexes
                    pass
                else:
                    pass
        return state

    def _prep_regexes(self) -> None:
        """Pre-compile regular expressions."""
        super(SingleStoreDBTableDefinitionParser, self)._prep_regexes()

        quotes = dict(
            zip(
                ('iq', 'fq', 'esc_fq'),
                [
                    re.escape(s)
                    for s in (
                        self.preparer.initial_quote,
                        self.preparer.final_quote,
                        self.preparer._escape_identifier(self.preparer.final_quote),
                    )
                ],
            ),
        )

        # (PRIMARY|UNIQUE|FULLTEXT|SPATIAL) INDEX `name` (USING (BTREE|HASH))?
        # (`col` (ASC|DESC)?, `col` (ASC|DESC)?)
        # KEY_BLOCK_SIZE size | WITH PARSER name  /*!50100 WITH PARSER name */
        # Modified to handle SingleStore SHARD KEY and SORT KEY with comma
        # Handle FULLTEXT with optional USING VERSION clause before KEY
        # Handle VECTOR INDEX_OPTIONS
        self._re_key = _re_compile(
            r'  (?:, *)?'  # Handle optional leading comma for SingleStore
            r'(?:(?P<type>MULTI +VALUE|\S+)(?: +USING +VERSION +\d+)? )?'
            r'(?:KEY|INDEX)'
            r'(?: +(?:%(iq)s(?P<name_quoted>(?:%(esc_fq)s|[^%(fq)s])+)%(fq)s'
            r'|(?P<name_unquoted>\w+)))?'
            r'(?: +USING +(?P<using_pre>\S+))?'
            r'(?:(?: +\((?P<columns>.*?)\)| +(?P<all_columns>ALL +COLUMNS)))'
            r'(?: +INDEX_OPTIONS *= *(?P<index_options>".*?"))?'  # JSON
            r'(?: +USING +(?P<using_post>\S+|CLUSTERED +COLUMNSTORE))?'
            r'(?: +KEY_BLOCK_SIZE *[ =]? *(?P<keyblock>\S+))?'
            r'(?: +WITH PARSER +(?P<parser>\S+))?'
            r'(?: +COMMENT +(?P<comment>(\x27\x27|\x27([^\x27])*?\x27)+))?'
            r'(?: +/\*(?P<version_sql>.+)\*/ *)?'
            r' *,?$' % quotes,  # Handle trailing comma and spaces
        )

        # SingleStore specific SHARD KEY and SORT KEY patterns
        # Handles: SHARD KEY (columns), SHARD KEY ONLY (columns),
        # SHARD KEY (), SORT KEY (columns), SHARD KEY name (columns) METADATA_ONLY, etc.
        self._re_singlestore_key = _re_compile(
            r'  (?:, *)?'  # Handle optional leading comma
            r'(?P<type>SHARD|SORT)'  # Key type
            r' +KEY'  # KEY immediately after type
            r'(?: +(?P<only>ONLY))?'  # Optional ONLY modifier for SHARD KEY
            r'(?:[ `]*(?P<name>[^`\s()]+)[ `]*)?'  # Optional key name (may be quoted)
            r' *\((?P<columns>.*?)\)'  # Column list (can be empty)
            r'(?: +(?P<metadata_only>METADATA_ONLY))?'  # Optional METADATA_ONLY suffix
            r' *,?$',  # Handle trailing comma and spaces
        )

        # SingleStore specific VECTOR INDEX pattern
        # Handles: VECTOR INDEX name (columns) [INDEX_OPTIONS='...']
        self._re_vector_index = _re_compile(
            r'  (?:, *)?'  # Handle optional leading comma
            r'VECTOR +INDEX'  # VECTOR INDEX keywords
            r' +(?P<name>\w+)'  # Index name (required for VECTOR INDEX)
            r' +\((?P<columns>.*?)\)'  # Column list
            r'(?:\s+INDEX_OPTIONS=\'(?P<index_options>.*?)\')?'  # Optional INDEX_OPTIONS
            r' *,?$',  # Handle trailing comma and spaces
        )

        # SingleStore specific COLUMN GROUP pattern
        # Handles: COLUMN GROUP name (*), COLUMN GROUP (*)
        self._re_column_group = _re_compile(
            r'  (?:, *)?'  # Handle optional leading comma
            r'COLUMN +GROUP'  # COLUMN GROUP keywords
            r'(?:[ `]*(?P<name>[^`\s()]+)[ `]*)?'  # Optional group name (may be quoted)
            r' *\(\*\)'  # (*) literal
            r' *,?$',  # Handle trailing comma and spaces
        )

        # `colname` <type> [type opts]
        #  (NOT NULL | NULL)
        #   DEFAULT ('value' | CURRENT_TIMESTAMP...)
        #   COMMENT 'comment'
        #  COLUMN_FORMAT (FIXED|DYNAMIC|DEFAULT)
        #  STORAGE (DISK|MEMORY)
        self._re_column = _re_compile(
            r'  '
            r'%(iq)s(?P<name>(?:%(esc_fq)s|[^%(fq)s])+)%(fq)s +'
            r'(?P<coltype>\w+)'
            r'(?:\((?P<arg>(?:\d+|\d+,\s*(?:F|I)?\d+|'
            r"(?:'(?:''|[^'])*',?)+))\))?"
            r'(?: +(?P<unsigned>UNSIGNED))?'
            r'(?: +(?P<zerofill>ZEROFILL))?'
            r'(?: +CHARACTER SET +(?P<charset>[\w_]+))?'
            r'(?: +COLLATE +(?P<collate>[\w_]+))?'
            r'(?: +(?P<notnull>(?:NOT )?NULL))?'
            r'(?: +DEFAULT +(?P<default>'
            r"(?:NULL|'(?:''|[^'])*'|[\-\w\.\(\)]+"
            r'(?: +ON UPDATE [\-\w\.\(\)]+)?)'
            r'))?'
            r'(?: +(?:GENERATED ALWAYS)? ?AS +(?P<generated>\('
            r'.*\))? ?(?P<persistence>VIRTUAL|STORED)?)?'
            r'(?: +(?P<autoincr>AUTO_INCREMENT))?'
            r"(?: +COMMENT +'(?P<comment>(?:''|[^'])*)')?"
            r'(?: +COLUMN_FORMAT +(?P<colfmt>\w+))?'
            r'(?: +STORAGE +(?P<storage>\w+))?'
            r'(?: +(?P<extra>.*))?'
            r',?$' % quotes,
        )

    def _parse_column(self, line: str, state: Any) -> None:  # noqa: C901
        """
        Extract column details.

        Falls back to a 'minimal support' variant if full parse fails.

        Paraemeters
        -----------
        line :
            Any column-bearing line from SHOW CREATE TABLE

        """
        spec = None
        m = self._re_column.match(line)
        if m:
            spec = m.groupdict()
            spec['full'] = True
        else:
            m = self._re_column_loose.match(line)
            if m:
                spec = m.groupdict()
                spec['full'] = False
        if not spec:
            util.warn('Unknown column definition %r' % line)
            return
        if not spec['full']:
            util.warn('Incomplete reflection of column definition %r' % line)

        name, type_, args = spec['name'], spec['coltype'], spec['arg']

        try:
            col_type = self.dialect.ischema_names[type_]
        except KeyError:
            util.warn(
                "Did not recognize type '%s' of column '%s'" % (type_, name),
            )
            col_type = sqltypes.NullType

        # Column type positional arguments eg. varchar(32)
        if args is None or args == '':
            type_args = []
        elif args[0] == "'" and args[-1] == "'":
            type_args = self._re_csv_str.findall(args)
        else:
            type_args = []
            for v in re.split(r'\s*,\s*', args):
                try:
                    type_args.append(int(v))
                except ValueError:
                    type_args.append(v)

        # Column type keyword options
        type_kw = {}

        if issubclass(col_type, (DATETIME, TIME, TIMESTAMP)):
            if type_args:
                type_kw['fsp'] = type_args.pop(0)

        for kw in ('unsigned', 'zerofill'):
            if spec.get(kw, False):
                type_kw[kw] = True
        for kw in ('charset', 'collate'):
            if spec.get(kw, False):
                type_kw[kw] = spec[kw]
        if issubclass(col_type, (ENUM, SET)):
            type_args = _strip_values(type_args)

            if issubclass(col_type, SET) and '' in type_args:
                type_kw['retrieve_as_bitwise'] = True

        type_instance = col_type(*type_args, **type_kw)

        col_kw: Dict[str, Any] = {}

        # NOT NULL
        col_kw['nullable'] = True
        # this can be "NULL" in the case of TIMESTAMP
        if spec.get('notnull', False) == 'NOT NULL':
            col_kw['nullable'] = False

        # AUTO_INCREMENT
        if spec.get('autoincr', False):
            col_kw['autoincrement'] = True
        elif issubclass(col_type, sqltypes.Integer):
            col_kw['autoincrement'] = False

        # DEFAULT
        default = spec.get('default', None)

        if default == 'NULL':
            # eliminates the need to deal with this later.
            default = None

        comment = spec.get('comment', None)

        if comment is not None:
            comment = cleanup_text(comment)

        sqltext = spec.get('generated')
        if sqltext is not None:
            computed = dict(sqltext=sqltext)
            persisted = spec.get('persistence')
            if persisted is not None:
                computed['persisted'] = persisted == 'STORED'
            col_kw['computed'] = computed

        col_d = dict(
            name=name, type=type_instance, default=default, comment=comment,
        )
        col_d.update(col_kw)
        state.columns.append(col_d)
