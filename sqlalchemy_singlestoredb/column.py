from sqlalchemy import Column, text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import TextClause

Base = declarative_base()


class PersistedColumn(Column):
    def __init__(self, *args, persisted_expression=None, **kwargs):
        if persisted_expression is not None:
            # Ensure persisted_expression is either a TextClause or a string
            if not isinstance(persisted_expression, (TextClause, str)):
                raise ValueError("Persisted expression must be a SQL expression as a string or TextClause")
            self.is_persisted = True
            self.persisted_expression = persisted_expression
            kwargs["info"] = {"persisted_expression": persisted_expression}
        super().__init__(*args, **kwargs)
