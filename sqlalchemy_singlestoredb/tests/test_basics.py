#!/usr/bin/env python
# type: ignore
"""Basic SingleStoreDB connection testing."""
from __future__ import annotations

import os
import re
import unittest

import singlestoredb.tests.utils as utils
import sqlalchemy as sa


class TestBasics(unittest.TestCase):

    dbname: str = ''
    dbexisted: bool = False

    @classmethod
    def setUpClass(cls):
        sql_file = os.path.join(os.path.dirname(__file__), 'test.sql')
        cls.dbname, cls.dbexisted = utils.load_sql(sql_file)

    @classmethod
    def tearDownClass(cls):
        if not cls.dbexisted:
            utils.drop_database(cls.dbname)

    def setUp(self):
        url = os.environ['SINGLESTOREDB_URL']
        if re.match(r'^[\w\-\+]+://', url):
            if not url.startswith('singlestoredb'):
                url = 'singlestoredb+' + url
        else:
            url = 'singlestoredb://' + url
        if url.endswith('/'):
            url = url[:-1]
        url = url + '/' + self.__class__.dbname
        self.engine = sa.create_engine(url)
        self.conn = self.engine.connect()

    def tearDown(self):
        try:
            if self.cur is not None:
                self.cur.close()
        except Exception:
            # traceback.print_exc()
            pass

        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            # traceback.print_exc()
            pass

    def test_connection(self):
        dbs = [x[0] for x in list(self.conn.exec_driver_sql('show databases'))]
        assert type(self).dbname in dbs, dbs

    def test_deferred_connection(self):
        url = os.environ['SINGLESTOREDB_URL']
        if '://' in url:
            scheme = url.split('://', 1)[0]
            if 'singlestoredb' not in scheme:
                scheme = f'singlestoredb+{scheme}'
        else:
            scheme = 'singlestoredb'
        try:
            del os.environ['SINGLESTOREDB_URL']
            eng = sa.create_engine(f'{scheme}://:@singlestore.com')
            conn = eng.connect()
            with self.assertRaises(sa.exc.InterfaceError):
                conn.exec_driver_sql('show databases')
            os.environ['SINGLESTOREDB_URL'] = url
            out = conn.exec_driver_sql('show databases')
            assert len(out.fetchall()) > 0
            conn.close()
        finally:
            os.environ['SINGLESTOREDB_URL'] = url

    def test_alltypes(self):
        meta = sa.MetaData()
        tbl = sa.Table('alltypes', meta)
        insp = sa.inspect(self.engine)
        insp.reflect_table(tbl, None)

        cols = {col.name: col for col in tbl.columns}

        def dtype(col):
            return col.type.__class__.__name__.lower()

        assert dtype(cols['id']) == 'integer', dtype(cols['id'])
        assert cols['id'].nullable is True
        assert cols['id'].type.unsigned is False

        assert dtype(cols['tinyint']) == 'tinyint', dtype(cols['tinyint'])
        assert cols['tinyint'].nullable is True
        assert cols['tinyint'].type.unsigned is False

        assert dtype(cols['unsigned_tinyint']) == 'tinyint', dtype(
            cols['unsigned_tinyint'],
        )
        assert cols['unsigned_tinyint'].nullable is True
        assert cols['unsigned_tinyint'].type.unsigned is True

        assert dtype(cols['bool']) == 'tinyint', dtype(cols['bool'])
        assert cols['bool'].nullable is True
        assert cols['bool'].type.unsigned is False
        assert cols['bool'].type.display_width == 1

        assert dtype(cols['boolean']) == 'tinyint', dtype(cols['boolean'])
        assert cols['boolean'].nullable is True
        assert cols['boolean'].type.unsigned is False
        assert cols['boolean'].type.display_width == 1

        assert dtype(cols['smallint']) == 'smallint', dtype(cols['smallint'])
        assert cols['smallint'].nullable is True
        assert cols['smallint'].type.unsigned is False

        assert dtype(cols['unsigned_smallint']) == 'smallint', dtype(
            cols['unsigned_smallint'],
        )
        assert cols['unsigned_smallint'].nullable is True
        assert cols['unsigned_smallint'].type.unsigned is True

        assert dtype(cols['mediumint']) == 'mediumint', dtype(cols['mediumint'])
        assert cols['mediumint'].nullable is True
        assert cols['mediumint'].type.unsigned is False

        assert dtype(cols['unsigned_mediumint']) == 'mediumint', dtype(
            cols['unsigned_mediumint'],
        )
        assert cols['unsigned_mediumint'].nullable is True
        assert cols['unsigned_mediumint'].type.unsigned is True

        assert dtype(cols['int24']) == 'mediumint', dtype(cols['int24'])
        assert cols['int24'].nullable is True
        assert cols['int24'].type.unsigned is False

        assert dtype(cols['unsigned_int24']) == 'mediumint', dtype(cols['unsigned_int24'])
        assert cols['unsigned_int24'].nullable is True
        assert cols['unsigned_int24'].type.unsigned is True

        assert dtype(cols['int']) == 'integer', dtype(cols['int'])
        assert cols['int'].nullable is True
        assert cols['int'].type.unsigned is False

        assert dtype(cols['unsigned_int']) == 'integer', dtype(cols['unsigned_int'])
        assert cols['unsigned_int'].nullable is True
        assert cols['unsigned_int'].type.unsigned is True

        assert dtype(cols['integer']) == 'integer', dtype(cols['integer'])
        assert cols['integer'].nullable is True
        assert cols['integer'].type.unsigned is False

        assert dtype(cols['unsigned_integer']) == 'integer', dtype(
            cols['unsigned_integer'],
        )
        assert cols['unsigned_integer'].nullable is True
        assert cols['unsigned_integer'].type.unsigned is True

        assert dtype(cols['bigint']) == 'bigint', dtype(cols['bigint'])
        assert cols['bigint'].nullable is True
        assert cols['bigint'].type.unsigned is False

        assert dtype(cols['unsigned_bigint']) == 'bigint', dtype(cols['unsigned_bigint'])
        assert cols['unsigned_bigint'].nullable is True
        assert cols['unsigned_bigint'].type.unsigned is True

        assert dtype(cols['float']) == 'float', dtype(cols['float'])
        assert cols['float'].nullable is True

        assert dtype(cols['double']) == 'double', dtype(cols['double'])
        assert cols['double'].nullable is True

        assert dtype(cols['real']) == 'double', dtype(cols['real'])
        assert cols['real'].nullable is True

        assert dtype(cols['decimal']) == 'decimal', dtype(cols['decimal'])
        assert cols['decimal'].nullable is True

        assert dtype(cols['dec']) == 'decimal', dtype(cols['dec'])
        assert cols['dec'].nullable is True

        assert dtype(cols['fixed']) == 'decimal', dtype(cols['fixed'])
        assert cols['fixed'].nullable is True

        assert dtype(cols['numeric']) == 'decimal', dtype(cols['numeric'])
        assert cols['numeric'].nullable is True

        assert dtype(cols['date']) == 'date', dtype(cols['date'])
        assert cols['date'].nullable is True

        assert dtype(cols['time']) == 'time', dtype(cols['time'])
        assert cols['time'].nullable is True
        assert cols['time'].type.fsp is None, cols['time'].type.fsp

        assert dtype(cols['time_6']) == 'time', dtype(cols['time_6'])
        assert cols['time_6'].nullable is True
        assert cols['time_6'].type.fsp == 6, cols['time_6'].type.fsp

        assert dtype(cols['datetime']) == 'datetime', dtype(cols['datetime'])
        assert cols['datetime'].nullable is True
        assert cols['datetime'].type.fsp is None, cols['datetime'].type.fsp

        assert dtype(cols['datetime_6']) == 'datetime', dtype(cols['datetime_6'])
        assert cols['datetime_6'].nullable is True
        assert cols['datetime_6'].type.fsp == 6, cols['datetime_6'].type.fsp

        assert dtype(cols['timestamp']) == 'timestamp', dtype(cols['timestamp'])
        assert cols['timestamp'].nullable is True
        assert cols['timestamp'].type.fsp is None, cols['timestamp'].type.fsp

        assert dtype(cols['timestamp_6']) == 'timestamp', dtype(cols['timestamp_6'])
        assert cols['timestamp_6'].nullable is True
        assert cols['timestamp_6'].type.fsp == 6, cols['timestamp_6'].type.fsp

        assert dtype(cols['year']) == 'year', dtype(cols['year'])
        assert cols['year'].nullable is True

        assert dtype(cols['char_100']) == 'char', dtype(cols['char_100'])
        assert cols['char_100'].nullable is True
        assert cols['char_100'].type.length == 100, cols['char_100'].type.length

        assert dtype(cols['binary_100']) == 'binary', dtype(cols['binary_100'])
        assert cols['binary_100'].nullable is True
        assert cols['binary_100'].type.length == 100, cols['binary_100'].type.length

        assert dtype(cols['varchar_200']) == 'varchar', dtype(cols['varchar_200'])
        assert cols['varchar_200'].nullable is True
        assert cols['varchar_200'].type.length == 200, cols['varchar_200'].type.length

        assert dtype(cols['varbinary_200']) == 'varbinary', dtype(cols['varbinary_200'])
        assert cols['varbinary_200'].nullable is True
        assert cols['varbinary_200'].type.length == 200, cols['varbinary_200'].type.length

        assert dtype(cols['longtext']) == 'longtext', dtype(cols['longtext'])
        assert cols['longtext'].nullable is True

        assert dtype(cols['mediumtext']) == 'mediumtext', dtype(cols['mediumtext'])
        assert cols['mediumtext'].nullable is True

        assert dtype(cols['text']) == 'text', dtype(cols['text'])
        assert cols['text'].nullable is True

        assert dtype(cols['tinytext']) == 'tinytext', dtype(cols['tinytext'])
        assert cols['tinytext'].nullable is True

        assert dtype(cols['longblob']) == 'longblob', dtype(cols['longblob'])
        assert cols['longblob'].nullable is True

        assert dtype(cols['mediumblob']) == 'mediumblob', dtype(cols['mediumblob'])
        assert cols['mediumblob'].nullable is True

        assert dtype(cols['blob']) == 'blob', dtype(cols['blob'])
        assert cols['blob'].nullable is True

        assert dtype(cols['tinyblob']) == 'tinyblob', dtype(cols['tinyblob'])
        assert cols['tinyblob'].nullable is True

        assert dtype(cols['json']) == 'json', dtype(cols['json'])
        assert cols['json'].nullable is True

        assert dtype(cols['enum']) == 'enum', dtype(cols['enum'])
        assert cols['enum'].nullable is True
        assert cols['enum'].type.enums == ['one', 'two', 'three'], cols['enum'].type.enums

        assert dtype(cols['set']) == 'set', dtype(cols['set'])
        assert cols['set'].nullable is True
        assert cols['set'].type.values == ('one', 'two', 'three'), cols['set'].type.values

    def test_double_percents(self):
        # Direct to driver, no params
        out = list(self.conn.exec_driver_sql('select 21 % 2, 101 % 2'))
        assert out == [(1, 1)]

        # Direct to driver, positional params
        out = list(self.conn.exec_driver_sql('select 21 %% 2, %s %% 2', (101,)))
        assert out == [(1, 1)]

        # Direct to driver, dict params
        out = list(
            self.conn.exec_driver_sql(
                'select 21 %% 2, %(num)s %% 2', dict(num=101),
            ),
        )
        assert out == [(1, 1)]

        with self.assertRaises(ValueError):
            self.conn.exec_driver_sql('select 21 % 2, %(num)s % 2', dict(foo=101))

        # Direct to driver, no params (with dummy param)
        out = list(self.conn.exec_driver_sql('select 21 %% 2, 101 %% 2', dict(foo=100)))
        assert out == [(1, 1)]

        with self.assertRaises(ValueError):
            self.conn.exec_driver_sql('select 21 % 2, 101 % 2', dict(foo=100))

        # Text clause, no params
        out = list(self.conn.execute(sa.text('select 21 % 2, 101 % 2')))
        assert out == [(1, 1)]

        # Texx clause, dict params
        out = list(self.conn.execute(sa.text('select 21 % 2, :num % 2'), dict(num=101)))
        assert out == [(1, 1)]

        # Text clause, dict params (with dummy param)
        out = list(self.conn.execute(sa.text('select 21 % 2, 101 % 2'), dict(foo=100)))
        assert out == [(1, 1)]
