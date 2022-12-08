#!/usr/bin/env python
"""Base classes for SingleStoreDB SQLAlchemy objects."""
from __future__ import annotations

import json
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Type
from typing import Union

import sqlalchemy.dialects.mysql.base as mybase
import sqlalchemy.sql.elements as se
from singlestoredb.connection import build_params
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

from . import reflection


class CaseInsensitiveDict(Dict[str, Any]):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
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


def _json_deserializer(value: Union[str, bytes, Dict[str, Any], List[Any]]) -> Any:
    if value is None:
        return None
    if type(value) is dict or type(value) is list:
        return value
    return json.loads(value)  # type: ignore


class JSON(mybase.JSON):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.pop('collate', None)
        mybase.JSON(*args, **kwargs)

    def result_processor(self, dialect: Any, coltype: Any) -> Any:
        string_process = self._str_impl.result_processor(dialect, coltype)
        json_deserializer = dialect._json_deserializer or json.loads

        def process(value: Union[str, bytes, Dict[str, Any], List[Any]]) -> Any:
            if value is None:
                return None
            if string_process:
                value = string_process(value)
            if type(value) is dict or type(value) is list:
                return value
            return json_deserializer(value)  # type: ignore

        return process


ischema_names['json'] = JSON


class SingleStoreDBExecutionContext(MySQLExecutionContext):
    """SingleStoreDB SQLAlchemy execution context."""


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
            return 'DATETIME'
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
                'Datatype %s does not support CAST on MySQL/MariaDb; '
                'the CAST will be skipped.'
                % self.dialect.type_compiler.process(cast.typeclause.type),
            )
            return self.process(cast.clause.self_group(), **kw)

        # Use the older cast function for strings. The new cast operator
        # will truncate numeric values without a supplied length.
        if 'DOUBLE' in type_ or 'FLOAT' in type_ or 'BOOL' in type_:
            return '%s :> %s' % (self.process(cast.clause, **kw), type_)

        return 'CAST(%s AS %s)' % (self.process(cast.clause, **kw), type_)


class SingleStoreDBDDLCompiler(MySQLDDLCompiler):
    """SingleStoreDB SQLAlchemy DDL compiler."""


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


class SingleStoreDBIdentifierPreparer(MySQLIdentifierPreparer):
    """SingleStoreDB SQLAlchemy identifier preparer."""


class _myconnpyBIT(BIT):
    def result_processor(self, dialect: Any, coltype: Any) -> Any:
        """MySQL-connector already converts mysql bits, so."""
        return None


class SingleStoreDBDialect(MySQLDialect):
    """SingleStoreDB SQLAlchemy dialect."""

    name = 'singlestoredb'

    default_paramstyle = 'named'

    ischema_names = ischema_names

    # Possibly mysql.connector-specific
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_native_decimal = True
    colspecs = util.update_copy(MySQLDialect.colspecs, {BIT: _myconnpyBIT})

    statement_compiler = SingleStoreDBCompiler
    ddl_compiler = SingleStoreDBDDLCompiler
    type_compiler = SingleStoreDBTypeCompiler
    preparer = SingleStoreDBIdentifierPreparer

    driver = ''

    supports_statement_cache = False

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
        return MySQLDialect.initialize(self, connection)

    def create_connect_args(self, url: URL) -> List[Any]:
        return [[], build_params(host=str(url))]

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


dialect: Type[SingleStoreDBDialect] = SingleStoreDBDialect
