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
