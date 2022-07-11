#!/usr/bin/env python
"""SingleStore SQLAlchemy reflection utilities."""
from __future__ import annotations

import re

from sqlalchemy import log
from sqlalchemy import util
from sqlalchemy.dialects.mysql.reflection import _re_compile
from sqlalchemy.dialects.mysql.reflection import MySQLTableDefinitionParser

class ReflectedState(object):
    """Stores raw information about a SHOW CREATE TABLE statement."""

    def __init__(self):
        self.columns = []
        self.table_options = {}
        self.table_name = None
        self.keys = []
        self.fk_constraints = []
        self.ck_constraints = []

@log.class_logger
class SingleStoreTableDefinitionParser(MySQLTableDefinitionParser):
    """Parses the results of a SHOW CREATE TABLE statement."""

    def _parse_constraints(self, line):
        type_, spec = super(SingleStoreTableDefinitionParser, self)._parse_constraints(line)
        re_shard = _re_compile(r'\s+,\s+SHARD\s+KEY\s+\(\)\s+')
        m = re_shard.match(line)
        if m:
            type_ = "shard_key"
            spec = {'type': None, 'name': 'SHARD', 'using_pre': None, 'columns': [], 'using_post': None, 'keyblock': None, 'parser': None, 'comment': None, 'version_sql': None}
        return type_, spec
            
    def parse(self, show_create, charset):
        state = ReflectedState()
        state.charset = charset
        for line in re.split(r"\r?\n", show_create):
            if line.startswith("  " + self.preparer.initial_quote):
                self._parse_column(line, state)
            # a regular table options line
            elif line.startswith(") "):
                self._parse_table_options(line, state)
            # an ANSI-mode table options line
            elif line == ")":
                pass
            elif line.startswith("CREATE "):
                self._parse_table_name(line, state)
            # Not present in real reflection, but may be if
            # loading from a file.
            elif not line:
                pass
            else:
                type_, spec = self._parse_constraints(line)
                if type_ is None:
                    util.warn("Unknown schema content: %r" % line)
                elif type_ == "key":
                    state.keys.append(spec)
                elif type_ == "fk_constraint":
                    state.fk_constraints.append(spec)
                elif type_ == "ck_constraint":
                    state.ck_constraints.append(spec)
                elif type_ == "shard_key":
                    state.keys.append(spec)
                else:
                    pass
        return state

    def _prep_regexes(self) -> None:
        """Pre-compile regular expressions."""
        super(SingleStoreTableDefinitionParser, self)._prep_regexes()

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
