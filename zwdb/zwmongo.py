import pymongo
from pymongo import UpdateOne,DeleteOne
from pymongo.errors import ConnectionFailure

from . import utils
from .zwdb import Connection
from .zwdb import Record
from .zwdb import RecordCollection
from .zwdb import ZwdbError

class ZWMongo(object):
    """Class defining a Mongo driver"""
    def __init__(self, db_url, **kwargs):
        # http://dochub.mongodb.org/core/connections
        o = utils.db_url_parser(db_url)
        p = o['props']
        self.dbcfg = {
            'host'      : o['host'],
            'port'      : o['port'] or 27017,
            'username'  : o['usr'],
            'password'  : o['pwd'],
        }
        self.dbname = o['db']
        self.dbcfg.update(kwargs)
        cfg = {
            'maxPoolSize' : 100
        }
        for p in cfg:
            self.dbcfg[p] = self.dbcfg.get(p, cfg[p])

        client = pymongo.MongoClient(**self.dbcfg)
        try:
            # The ismaster command is cheap and does not require auth.
            client.admin.command('ismaster')
            self.client = client
        except ConnectionFailure:
            raise ZwdbError('Server not available, {}:{}'.format(self.dbcfg['host'], self.dbcfg['port']))

    @property
    def pool_size(self):
        return self.client.max_pool_size

    def connect(self):
        conn = self.client[self.dbname]
        return ZWMongoConnection(conn)

    def dispose(self):
        self.client.close()

    def get_table_names(self):
        with self.connect() as conn:
            rs = conn._conn.list_collection_names()
        return rs

class ZWMongoConnection(Connection):
    def __init__(self, conn):
        self._conn = conn
        self._cursor = None
        self.open = True
    
    def close(self):
        self._close_cursor()
        self._conn.close()
        self.open = False
    
    def _close_cursor(self):
        if self._cursor:
            self._cursor.close()
            self._cursor = None

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def __next__(self):
        rec = None
        if self._cursor:
            rec = self._cursor.fetchone()
        if rec:
            return rec
        else:
            self._close_cursor()
            raise StopIteration('Cursor contains no more rows.')

    def execute(self, stmt, fetchall=True, commit=False, **params):
        '''use execute to run raw sql and we don't want multi stmt in operation(multi=False)
        '''
        params = params or {}
        # Execute the given query
        self._cursor = self._conn.cursor(buffered=False)
        self._cursor.execute(stmt, params=params)
        keys = self._cursor.column_names
        if commit:
            self._conn.commit()
        row_gen = (keys, self)
        results = RecordCollection(*row_gen)
        if fetchall:
            results.all()
        return results

    def executemany(self, stmt, fetchall=True, commit=False, paramslist=None):
        paramslist = paramslist or []
        # Execute the given query
        self._cursor = self._conn.cursor(buffered=False)
        self._cursor.executemany(stmt, paramslist)
        keys = self._cursor.column_names
        if commit:
            self._conn.commit()
        row_gen = (keys, self)
        results = RecordCollection(*row_gen)
        if fetchall:
            results.all()
        return results

    def find(self, dst, fetchall=False, **params):
        """select query
        """
        stmt = 'SELECT * FROM {}'.format(dst)
        ks = params.keys()
        if params:
            vs = ','.join(['{0}=%({0})s'.format(s) for s in ks])
            stmt += ' WHERE {}'.format(vs)
        results = self.execute(stmt, fetchall=fetchall, **params)
        return results

    def insert(self, dst, recs):
        if len(recs) == 0:
            return 0
        ks = recs[0].keys()
        fs = ','.join(ks)
        vs = ','.join(['%({})s'.format(s) for s in ks])
        stmt = 'INSERT INTO {} ({}) VALUES({})'.format(dst, fs, vs)
        rc = self.executemany(stmt, fetchall=False, commit=True, paramslist=recs)
        return rc._rows._cursor.rowcount
    
    def update(self, dst, recs, keyflds):
        if len(recs) == 0:
            return 0
        rec = recs[0]
        ks = rec.keys()
        vs = ','.join(['{0}=%({0})s'.format(s) for s in ks])
        ws = ['{0}=%({0})s'.format(k) for k in keyflds]
        ws.append('1=1')
        ws = ' AND '.join(ws)
        stmt = 'UPDATE {} SET {} WHERE {}'.format(dst, vs, ws)
        rc = self.executemany(stmt, fetchall=False, commit=True, paramslist=recs)
        return rc._rows._cursor.rowcount
    
    def upsert(self, dst, recs, keyflds):
        if len(recs) == 0:
            return 0
        recs_update = []
        recs_insert = []
        ws = ['{0}=%({0})s'.format(k) for k in keyflds]
        ws.append('1=1')
        ws = ' AND '.join(ws)
        stmt = 'SELECT count(*) AS count FROM {} WHERE {}'.format(dst, ws)
        for rec in recs:
            r = self.execute(stmt, fetchall=True, **rec)
            if r[0].count == 0:
                recs_insert.append(rec)
            else:
                recs_update.append(rec)
        c = self.update(dst, recs_update, keyflds) + self.insert(dst, recs_insert)
        return c
    
    def delete(self, dst, recs, keyflds):
        if len(recs) == 0:
            return 0
        ws = ['{0}=%({0})s'.format(k) for k in keyflds]
        ws.append('1=1')
        ws = ' AND '.join(ws)
        stmt = 'DELETE FROM {} WHERE {}'.format(dst, ws)
        rc = self.executemany(stmt, fetchall=False, commit=True, paramslist=recs)
        return rc._rows._cursor.rowcount

Connection.register(ZWMongoConnection)