import mysql.connector
import mysql.connector.pooling

from . import utils
from .zwdb import Connection
from .zwdb import Record
from .zwdb import RecordCollection
from .zwdb import ZwdbError

class ZWMysql(object):
    """Class defining a MySQL driver"""
    def __init__(self, db_url, **kwargs):
        o = utils.db_url_parser(db_url)
        p = o['props']
        self.dbcfg = {
            'host'      : o['host'],
            'port'      : o['port'] or 3306,
            'user'      : o['usr'],
            'password'  : o['pwd'],
            'database'  : o['db'],
            'charset'           : p.get('characterEncoding', 'utf8mb4'),
            'use_unicode'       : p.get('useUnicode', True),
            'connect_timeout'   : p.get('connectTimeout', 10),
        }
        self.dbcfg.update(kwargs)
        cfg = {
            'collation' : 'utf8mb4_general_ci',
            'use_pure'  : False,
            'pool_size' : 5
        }
        for p in cfg:
            self.dbcfg[p] = self.dbcfg.get(p, cfg[p])
        self._pool = None

    @property
    def pool_size(self):
        """Return number of connections managed by the pool"""
        return self._pool.pool_size

    def connect(self):
        if not self._pool:
            self._pool = mysql.connector.pooling.MySQLConnectionPool(**self.dbcfg)
        conn = self._pool.get_connection()
        return ZWMysqlConnection(conn)

    def dispose(self):
        self._pool._remove_connections()

    def get_table_names(self):
        with self.connect() as conn:
            rs = conn.execute('SHOW TABLES')
            recs = rs.all()
        return recs

class ZWMysqlConnection(Connection):
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

Connection.register(ZWMysqlConnection)
