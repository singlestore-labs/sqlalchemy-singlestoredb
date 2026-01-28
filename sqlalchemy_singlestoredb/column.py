from __future__ import annotations

from typing import Any

from sqlalchemy import Column
from sqlalchemy.sql.expression import TextClause


class PersistedColumn(Column):
    """SingleStore computed column that is automatically calculated and stored.

    PersistedColumn defines computed columns based on an expression. The computed
    value is persisted to disk and updated when dependent columns change.

    Parameters
    ----------
    name : str
        Column name
    type_ : TypeEngine
        SQLAlchemy type for the computed value
    persisted_expression : str or TextClause
        SQL expression to compute the column value
    *args, **kwargs
        Additional arguments passed to Column

    Examples
    --------
    Basic computed column:

    >>> PersistedColumn('total', Integer, persisted_expression='price * quantity')

    Using TextClause for complex expressions:

    >>> from sqlalchemy import text
    >>> PersistedColumn('full_name', String(101),
    ...                 persisted_expression=text("CONCAT(first_name, ' ', last_name)"))

    **Table Usage:**

    .. code-block:: python

        from sqlalchemy import Column, Integer, MetaData, String, Table, text
        from sqlalchemy_singlestoredb import PersistedColumn

        metadata = MetaData()

        products = Table(
            'products', metadata,
            Column('id', Integer, primary_key=True),
            Column('price', Integer),
            Column('quantity', Integer),
            PersistedColumn(
                'total',
                Integer,
                persisted_expression='price * quantity',
            ),
        )

        users = Table(
            'users', metadata,
            Column('id', Integer, primary_key=True),
            Column('first_name', String(50)),
            Column('last_name', String(50)),
            PersistedColumn(
                'full_name',
                String(101),
                persisted_expression=text("CONCAT(first_name, ' ', last_name)"),
            ),
        )

    **ORM Usage:**

    In ORM models, PersistedColumn is used directly as a class attribute,
    just like a regular Column:

    .. code-block:: python

        from sqlalchemy import Column, Integer, String
        from sqlalchemy.orm import declarative_base
        from sqlalchemy_singlestoredb import PersistedColumn

        Base = declarative_base()

        class Product(Base):
            __tablename__ = 'products'

            id = Column(Integer, primary_key=True)
            price = Column(Integer)
            quantity = Column(Integer)
            total = PersistedColumn(
                'total',
                Integer,
                persisted_expression='price * quantity',
            )

        class User(Base):
            __tablename__ = 'users'

            id = Column(Integer, primary_key=True)
            first_name = Column(String(50))
            last_name = Column(String(50))
            full_name = PersistedColumn(
                'full_name',
                String(101),
                persisted_expression="CONCAT(first_name, ' ', last_name)",
            )

    """

    def __init__(
        self,
        *args: Any,
        persisted_expression: Any = None,
        **kwargs: Any,
    ) -> None:
        if persisted_expression is not None:
            # Ensure persisted_expression is either a TextClause or a string
            if not isinstance(persisted_expression, (TextClause, str)):
                raise ValueError(
                    'Persisted expression must be a SQL expression '
                    'as a string or TextClause',
                )
            self.is_persisted = True
            self.persisted_expression = persisted_expression
            kwargs['info'] = {'persisted_expression': persisted_expression}
        super().__init__(*args, **kwargs)
