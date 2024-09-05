from __future__ import annotations

from typing import Any

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement


class ShardKey(DDLElement):
    def __init__(self, *columns: Any) -> None:
        self.columns = columns

    def __repr__(self) -> str:
        return 'ShardKey(%s)' % ', '.join([repr(x) for x in self.columns])


@compiles(ShardKey, 'singlestoredb.mysql')
def compile_shard_key(element: Any, compiler: Any, **kw: Any) -> str:
    return 'SHARD KEY (%s)' % ', '.join([str(x) for x in element.columns])


class SortKey(DDLElement):
    def __init__(self, *columns: Any) -> None:
        self.columns = columns

    def __repr__(self) -> str:
        return 'SortKey(%s)' % ', '.join([repr(x) for x in self.columns])


@compiles(SortKey, 'singlestoredb.mysql')
def compile_sort_key(element: Any, compiler: Any, **kw: Any) -> str:
    return 'SORT KEY (%s)' % ', '.join([str(x) for x in element.columns])
