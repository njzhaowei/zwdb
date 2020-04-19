import os
from contextlib import contextmanager

import mysql.connector
import mysql.connector.pooling

from . import utils
from .records import Record, RecordCollection

class ZWMysql(object):
    """Class defining a MySQL driver"""
    def __init__(self, db_url, **kwargs):
        self.db_url = db_url or os.environ.get('DATABASE_URL')
        if not self.db_url:
            raise ValueError('You must provide a db_url.')

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

    @property
    def version(self):
        return mysql.connector.__version__

    def get_connection(self):
        if not self._pool:
            self._pool = mysql.connector.pooling.MySQLConnectionPool(**self.dbcfg)
        conn = self._pool.get_connection()
        return ZWMysqlConnection(conn)

    def close(self):
        self._pool._remove_connections()

    def lists(self):
        with self.get_connection() as conn:
            rs = conn.execute('SHOW TABLES')
            recs = rs.all()
        return recs
    
    def find(self, tbl, clause=None, fetchall=False, **params):
        conn = self.get_connection()
        recs = conn.find(tbl, clause, fetchall, **params)
        if fetchall:
            conn.close()
        return recs
    
    def insert(self, tbl, recs):
        with self.get_connection() as conn:
            rtn = conn.insert(tbl, recs)
        return rtn
    
    def update(self, tbl, recs, keyflds):
        with self.get_connection() as conn:
            rtn = conn.update(tbl, recs, keyflds)
        return rtn
    
    def upsert(self, tbl, recs, keyflds):
        with self.get_connection() as conn:
            rtn = conn.upsert(tbl, recs, keyflds)
        return rtn

    def delete(self, tbl, recs, keyflds):
        with self.get_connection() as conn:
            rtn = conn.delete(tbl, recs, keyflds)
        return rtn
    
    def exec_script(self, fp):
        with self.get_connection() as conn:
            cursor = conn._conn.cursor()
            statement = ''
            for line in open(fp):
                if line.strip().startswith('--'):  # ignore sql comment lines
                    continue
                if not line.strip().endswith(';'):  # keep appending lines that don't end in ';'
                    statement = statement + line
                else:  # when you get a line ending in ';' then exec statement and reset for next statement
                    statement = statement + line
                    cursor.execute(statement)
                    statement = ''
        return True

    @contextmanager
    def transaction(self):
        """A context manager for executing a transaction on this Database."""
        conn = self.get_connection()
        _conn = conn._conn
        tx = _conn.transaction()
        try:
            yield conn
            tx.commit()
        except:
            tx.rollback()
        finally:
            _conn.close()

    def __repr__(self):
        return '<Database host={}:{}>'.format(self.dbcfg['host'], self.dbcfg['port'])

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

class ZWMysqlConnection(object):
    def __init__(self, conn):
        self._conn = conn
        self._cursor = None
        self.open = True

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

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

    def execute(self, stmt, commit=False, fetchall=True, **params):
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

    def executemany(self, stmt, paramslist=None, commit=False, fetchall=True):
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

    def find(self, tbl, clause=None, fetchall=False, **params):
        """select query
        """
        stmt = 'SELECT * FROM {}'.format(tbl)
        ks = params.keys()
        if params:
            vs = ' AND '.join(['{0}=%({0})s'.format(s) if params[s] is not None else 'isnull({0})'.format(s) for s in ks])
            stmt += ' WHERE {}'.format(vs)
        if clause:
            for k,v in clause.items():
                stmt += ' {0} {1}'.format(k, v)
        results = self.execute(stmt, commit=False, fetchall=fetchall, **params)
        return results
    
    def exist(self, tbl, rec, keyflds):
        ws = ['{0}=%({0})s'.format(k) for k in keyflds]
        ws.append('1=1')
        ws = ' AND '.join(ws)
        stmt = 'SELECT count(*) AS count FROM {} WHERE {}'.format(tbl, ws)
        r = self.execute(stmt, commit=False, fetchall=True, **rec)
        return r[0].count != 0

    def insert(self, tbl, recs):
        if recs is None or len(recs) == 0:
            return 0
        ks = recs[0].keys()
        fs = ','.join(ks)
        vs = ','.join(['%({})s'.format(s) for s in ks])
        stmt = 'INSERT INTO {} ({}) VALUES({})'.format(tbl, fs, vs)
        rc = self.executemany(stmt, paramslist=recs, commit=True, fetchall=False)
        return rc._rows._cursor.rowcount

    def update(self, tbl, recs, keyflds):
        if recs is None or len(recs) == 0:
            return 0
        rec = recs[0]
        ks = rec.keys()
        vs = ','.join(['{0}=%({0})s'.format(s) for s in ks])
        ws = ['{0}=%({0})s'.format(k) for k in keyflds]
        ws.append('1=1')
        ws = ' AND '.join(ws)
        stmt = 'UPDATE {} SET {} WHERE {}'.format(tbl, vs, ws)
        rc = self.executemany(stmt, paramslist=recs, commit=True, fetchall=False)
        return rc._rows._cursor.rowcount

    def upsert(self, tbl, recs, keyflds):
        if recs is None or len(recs) == 0:
            return 0
        recs_update = []
        recs_insert = []
        for idx,rec in enumerate(recs):
            if not self.exist(tbl, rec, keyflds):
                if self._exist_in_recs(idx, recs, keyflds):
                    recs_update.append(rec)
                else:
                    recs_insert.append(rec)
            else:
                recs_update.append(rec)
        ic = self.insert(tbl, recs_insert)
        uc = self.update(tbl, recs_update, keyflds)
        return ic, uc

    def delete(self, tbl, recs, keyflds):
        if recs is None or len(recs) == 0:
            return 0
        ws = ['{0}=%({0})s'.format(k) for k in keyflds]
        ws.append('1=1')
        ws = ' AND '.join(ws)
        stmt = 'DELETE FROM {} WHERE {}'.format(tbl, ws)
        rc = self.executemany(stmt, paramslist=recs, commit=True, fetchall=False)
        return rc._rows._cursor.rowcount
    
    def _exist_in_recs(self, idx, recs, keyflds):
        rec = recs[idx]
        for i in range(idx):
            r = recs[i]
            is_equal = True
            for k in keyflds:
                if rec[k] != r[k]:
                    is_equal = False
                    break
            if is_equal:
                return True
        return False
        

