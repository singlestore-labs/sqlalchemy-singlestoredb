#!/usr/bin/env python3
from __future__ import annotations

import sys

sys.path.insert(0, '/home/ksmith/src/ibis-singlestore')
sys.path.insert(0, '/home/ksmith/src/sqlalchemy-singlestore')
sys.path.insert(0, '/home/ksmith/src/singlestore-python')

import ibis

conn = ibis.singlestore.connect(host='127.0.0.1', user='root', password='', database='ibis_testing')

import decimal
import pandas as pd
import sqlalchemy as sa

df = pd.DataFrame(
        {
            'plain_int64': list(range(1, 4)),
            'plain_strings': list('abc'),
            'plain_float64': [4.0, 5.0, 6.0],
            'plain_datetimes_naive': pd.Series(
                pd.date_range(
                    start='2017-01-02 01:02:03.234', periods=3,
                ).values,
            ),
            'plain_datetimes_ny': pd.Series(
                pd.date_range(
                    start='2017-01-02 01:02:03.234', periods=3,
                ).values,
            ).dt.tz_localize('UTC'),
            'plain_datetimes_utc': pd.Series(
                pd.date_range(
                    start='2017-01-02 01:02:03.234', periods=3,
                ).values,
            ).dt.tz_localize('UTC'),
            'dup_strings': list('dad'),
            'dup_ints': [1, 2, 1],
            'float64_as_strings': ['100.01', '234.23', '-999.34'],
            'int64_as_strings': list(map(str, range(1, 4))),
            'strings_with_space': [' ', 'abab', 'ddeeffgg'],
            'int64_with_zeros': [0, 1, 0],
            'float64_with_zeros': [1.0, 0.0, 1.0],
            'float64_positive': [1.0, 2.0, 1.0],
            'strings_with_nulls': ['a', None, 'b'],
            'datetime_strings_naive': pd.Series(
                pd.date_range(
                    start='2017-01-02 01:02:03.234', periods=3,
                ).values,
            ).astype(str),
            'datetime_strings_ny': pd.Series(
                pd.date_range(
                    start='2017-01-02 01:02:03.234', periods=3,
                ).values,
            )
            .dt.tz_localize('America/New_York')
            .astype(str),
            'datetime_strings_utc': pd.Series(
                pd.date_range(
                    start='2017-01-02 01:02:03.234', periods=3,
                ).values,
            )
            .dt.tz_localize('UTC')
            .astype(str),
            'decimal': list(map(decimal.Decimal, [1.0, 2, 3.234])),
            # 'array_of_float64': [
            #     np.array([1.0, 2.0]),
            #     np.array([3.0]),
            #     np.array([]),
            # ],
            # 'array_of_int64': [np.array([1, 2]), np.array([]), np.array([3])],
            # 'array_of_strings': [
            #     np.array(['a', 'b']),
            #     np.array([]),
            #     np.array(['c']),
            # ],
            # 'map_of_strings_integers': [{'a': 1, 'b': 2}, None, {}],
            # 'map_of_integers_strings': [{}, None, {1: 'a', 2: 'b'}],
            # 'map_of_complex_values': [None, {'a': [1, 2, 3], 'b': []}, {}],
        },
)

import sqlalchemy.types as st

#conn.drop_table('newdf_cast')

df.to_sql('newdf_cast', conn.con, index=False, schema=None, dtype=dict(decimal=st.DECIMAL))
