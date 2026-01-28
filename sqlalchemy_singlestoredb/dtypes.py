#!/usr/bin/env python
"""Base classes for SingleStoreDB SQLAlchemy objects."""
from __future__ import annotations

import json
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import sqlalchemy.dialects.mysql.base as mybase
from sqlalchemy import String

from .compat import HAS_CACHE_KEY
from .compat import make_cache_key


def _json_deserializer(value: Union[str, bytes, Dict[str, Any], List[Any]]) -> Any:
    if value is None:
        return None
    if type(value) is dict or type(value) is list:
        return value
    return json.loads(value)  # type: ignore


class JSON(mybase.JSON):
    """SingleStore JSON data type for storing structured JSON data.

    SingleStore's native JSON data type supporting structured data storage
    and querying.

    Examples
    --------
    **Table Usage:**

    .. code-block:: python

        from sqlalchemy import Column, Integer, MetaData, Table
        from sqlalchemy_singlestoredb import JSON

        metadata = MetaData()

        documents = Table(
            'documents', metadata,
            Column('id', Integer, primary_key=True),
            Column('metadata', JSON),
            Column('tags', JSON),
        )

    **ORM Usage:**

    .. code-block:: python

        from sqlalchemy import Column, Integer, String
        from sqlalchemy.orm import declarative_base
        from sqlalchemy_singlestoredb import JSON

        Base = declarative_base()

        class Document(Base):
            __tablename__ = 'documents'

            id = Column(Integer, primary_key=True)
            title = Column(String(200))
            metadata = Column(JSON)
            tags = Column(JSON)

    **With Multi-Value Indexes:**

    For efficient querying of JSON arrays, combine with MultiValueIndex:

    .. code-block:: python

        from sqlalchemy_singlestoredb import JSON, MultiValueIndex

        class Article(Base):
            __tablename__ = 'articles'

            id = Column(Integer, primary_key=True)
            tags = Column(JSON)

            __table_args__ = {
                'singlestoredb_multi_value_indexes': [MultiValueIndex('tags')],
            }

    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.pop('collate', None)
        super().__init__(*args, **kwargs)

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

    def _cached_literal_processor(self, dialect: Any) -> Any:
        """Return a literal processor for caching support."""
        if HAS_CACHE_KEY:
            return super()._cached_literal_processor(dialect)
        return None

    def _gen_cache_key(self, anon_map: Any, bindparams: Any) -> Any:
        """Generate a cache key for this type."""
        if HAS_CACHE_KEY:
            return make_cache_key((JSON,), anon_map, bindparams)
        return None


class VECTOR(mybase.BLOB):
    """SingleStore VECTOR data type for storing fixed-dimension vectors.

    VECTOR is commonly used in AI/ML applications for embeddings and
    similarity search.

    Parameters
    ----------
    n_elems : int, optional
        Number of elements (dimensions) in the vector. Defaults to 1.
    elem_type : str, optional
        Element type for the vector. Defaults to 'F32'.

        **Floating Point Types:**

        - ``F16`` / ``FLOAT16``: 16-bit float (half precision)
        - ``F32`` / ``FLOAT32``: 32-bit float (single precision, default)
        - ``F64`` / ``FLOAT64``: 64-bit float (double precision)

        **Integer Types:**

        - ``I8`` / ``INT8``: 8-bit integer
        - ``I16`` / ``INT16``: 16-bit integer
        - ``I32`` / ``INT32``: 32-bit integer
        - ``I64`` / ``INT64``: 64-bit integer

    Examples
    --------
    Basic vector column:

    >>> VECTOR(1536)

    With element type:

    >>> VECTOR(768, elem_type=VECTOR.F16)

    **Table Usage:**

    .. code-block:: python

        from sqlalchemy import Column, Integer, MetaData, Table
        from sqlalchemy_singlestoredb import VECTOR

        metadata = MetaData()

        embeddings = Table(
            'embeddings', metadata,
            Column('id', Integer, primary_key=True),
            Column('embedding', VECTOR(1536)),
        )

        # With different element types
        documents = Table(
            'documents', metadata,
            Column('id', Integer, primary_key=True),
            Column('embedding_f16', VECTOR(768, elem_type=VECTOR.F16)),
            Column('embedding_i8', VECTOR(768, elem_type=VECTOR.I8)),
        )

    **ORM Usage:**

    .. code-block:: python

        from sqlalchemy import Column, Integer, String
        from sqlalchemy.orm import declarative_base
        from sqlalchemy_singlestoredb import VECTOR

        Base = declarative_base()

        class Embedding(Base):
            __tablename__ = 'embeddings'

            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            embedding = Column(VECTOR(1536))

        class Document(Base):
            __tablename__ = 'documents'

            id = Column(Integer, primary_key=True)
            content = Column(String(10000))
            embedding = Column(VECTOR(768, elem_type=VECTOR.F16))

    """

    __visit_name__ = 'VECTOR'

    F16 = FLOAT16 = 'F16'
    F32 = FLOAT32 = 'F32'
    F64 = FLOAT64 = 'F64'
    I8 = INT8 = 'I8'
    I16 = INT16 = 'I16'
    I32 = INT32 = 'I32'
    I64 = INT64 = 'I64'

    def __init__(
        self,
        n_elems: Optional[int] = None,
        elem_type: Optional[str] = None,
    ) -> None:
        self.n_elems = n_elems or 1
        self.elem_type = elem_type or 'F32'
        length = int(self.n_elems * int(self.elem_type[1:]) / 8)
        super().__init__(length=length)
        # Set up _str_impl for result_processor (mimics JSON type behavior)
        self._str_impl = String()

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

    def _cached_literal_processor(self, dialect: Any) -> Any:
        """Return a literal processor for caching support."""
        if HAS_CACHE_KEY:
            return super()._cached_literal_processor(dialect)
        return None

    def _gen_cache_key(self, anon_map: Any, bindparams: Any) -> Any:
        """Generate a cache key for this type including dimensions and element type."""
        if HAS_CACHE_KEY:
            return make_cache_key(
                (VECTOR, self.n_elems, self.elem_type),
                anon_map,
                bindparams,
            )
        return None
