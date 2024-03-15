from sqlalchemy import event, Table, DDL
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import DDLElement

Base = declarative_base()


class ShardKey(DDLElement):
    def __init__(self, *columns):
        self.columns = columns


@compiles(ShardKey, 'singlestoredb.mysql')
def compile_shard_key(element, compiler, **kw):
    return "SHARD KEY (%s)" % ', '.join(element.columns)


class SortKey(DDLElement):
    def __init__(self, *columns):
        self.columns = columns


@compiles(SortKey, 'singlestoredb.mysql')
def compile_sort_key(element, compiler, **kw):
    return "SORT KEY (%s)" % ', '.join(element.columns)
