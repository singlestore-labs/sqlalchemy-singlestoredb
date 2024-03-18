"""
Example that uses ShardKey, SortKey and PersistedColumn in one table definition
Utilizes mock engine to run without actual connection to database
"""
from __future__ import annotations

from typing import Any

from singlestoredb.alchemy import create_engine
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import declarative_base

from sqlalchemy_singlestoredb import PersistedColumn
from sqlalchemy_singlestoredb import ShardKey
from sqlalchemy_singlestoredb import SortKey

Base = declarative_base()


# Creating mock engine as suggested by the SQLAlchemy docs
# https://docs.sqlalchemy.org/en/20/faq/metadata_schema.html#how-can-i-get-the-create-table-drop-table-output-as-a-string
def dump(sql: Any, *multiparams: Any, **params: Any) -> None:
    print(sql.compile(dialect=mock_engine.dialect))


mock_engine = create_engine('singlestoredb://host:3306', strategy='mock', executor=dump)


# Example model using PersistedColumn with a computed expression
class MyTable(Base):  # type: ignore
    __tablename__ = 'my_new_table'

    id = Column(Integer, primary_key=True)
    data = Column(String(50))
    data_length = PersistedColumn(Integer, persisted_expression='LENGTH(data)')

    __table_args__ = (
        {
            'info': {
                'singlestoredb_shard_key': ShardKey('id'),
                'singlestoredb_sort_key': SortKey('id', 'data'),
            },
        }
    )


def main() -> None:
    Base.metadata.create_all(mock_engine, checkfirst=False)


if __name__ == '__main__':
    main()
