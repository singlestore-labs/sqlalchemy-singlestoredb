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

from .compat import HAS_CACHE_KEY
from .compat import make_cache_key


def _json_deserializer(value: Union[str, bytes, Dict[str, Any], List[Any]]) -> Any:
    if value is None:
        return None
    if type(value) is dict or type(value) is list:
        return value
    return json.loads(value)  # type: ignore


class JSON(mybase.JSON):

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
        mybase.BLOB(self.n_elems * int(self.elem_type[1:]) / 8)

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
