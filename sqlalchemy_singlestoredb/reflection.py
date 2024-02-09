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
        type_, spec = super(
            SingleStoreDBTableDefinitionParser,
            self,
        )._parse_constraints(line)
        re_shard = _re_compile(r'\s+,\s+SHARD\s+KEY\s+\(\)\s+')
        m = re_shard.match(line)
        if m:
            type_ = 'shard_key'
            spec = {
                'type': None, 'name': 'SHARD', 'using_pre': None,
                'columns': [], 'using_post': None, 'keyblock': None,
                'parser': None, 'comment': None, 'version_sql': None,
            }
        return type_, spec

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
                elif type_ == 'shard_key':
                    state.keys.append(spec)
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
        self._re_key = _re_compile(
            r'  '
            r'(?:(?P<type>\S+) )?KEY'
            r'(?: +%(iq)s(?P<name>(?:%(esc_fq)s|[^%(fq)s])+)%(fq)s)?'
            r'(?: +USING +(?P<using_pre>\S+))?'
            r' +\((?P<columns>.*?)\)'
            r'(?: +USING +(?P<using_post>\S+|CLUSTERED +COLUMNSTORE))?'
            r'(?: +KEY_BLOCK_SIZE *[ =]? *(?P<keyblock>\S+))?'
            r'(?: +WITH PARSER +(?P<parser>\S+))?'
            r'(?: +COMMENT +(?P<comment>(\x27\x27|\x27([^\x27])*?\x27)+))?'
            r'(?: +/\*(?P<version_sql>.+)\*/ *)?'
            r',?$' % quotes,
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
