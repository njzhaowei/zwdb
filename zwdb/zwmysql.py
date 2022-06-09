import os
from contextlib import contextmanager

import mysql.connector
import mysql.connector.pooling

from . import utils
from .records import RecordCollection

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
            'use_pure'  : True,
            'pool_size' : 5
        }
        for k,v in cfg.items():
            self.dbcfg[k] = self.dbcfg.get(k, v)
        self._debug = False
        self._pool = None

    pool_size = property(lambda self: self._pool.pool_size)
    version = property(lambda _: mysql.connector.__version__)

    def get_connection(self):
        if not self._pool:
            self._pool = mysql.connector.pooling.MySQLConnectionPool(**self.dbcfg)
        conn = self._pool.get_connection()
        return ZWMysqlConnection(conn, debug=self._debug)

    def close(self):
        if self._pool:
            # pylint: disable=protected-access
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

    def findone(self, tbl, clause=None, **params):
        clause = clause or {}
        clause['limit'] = 1
        recs = self.find(tbl, clause=clause, fetchall=True, **params)
        return recs[0] if len(recs)>0 else None

    def exists(self, tbl, rec=None, keyflds=None, **params):
        with self.get_connection() as conn:
            rtn = conn.exists(tbl, rec, keyflds, **params)
        return rtn

    def count(self, tbl, **params):
        with self.get_connection() as conn:
            rtn = conn.count(tbl, **params)
        return rtn

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

    def delete(self, tbl, recs=None, keyflds=None, **params):
        with self.get_connection() as conn:
            rtn = conn.delete(tbl, recs, keyflds, **params)
        return rtn

    def select(self, stmt, fetchall=True, **params):
        conn = self.get_connection()
        rtn =  conn.execute(stmt, fetchall=fetchall, **params)
        if fetchall:
            conn.close()
        return rtn

    def exec_script(self, fp):
        with self.get_connection() as conn:
            # pylint: disable=protected-access
            cursor = conn._conn.cursor()
            statement = ''
            with open(fp, encoding='utf-8') as fs:
                for line in fs:
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
        conn.transaction = True
        _conn = conn.conn
        _conn.autocommit = False
        _conn.start_transaction()
        try:
            yield conn
            _conn.commit()
        except Exception:
            _conn.rollback()
        finally:
            conn.transaction = False
            conn.close()

    def __repr__(self):
        return '<Database host={}:{}>'.format(self.dbcfg['host'], self.dbcfg['port'])

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

class ZWMysqlConnection(object):
    conn = property(lambda self: self._conn)

    def __init__(self, conn, debug=False):
        self._conn = conn
        self._cursor = None
        self.open = True
        self.transaction = False
        self._debug = debug

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
        if self._debug:
            print('%s <= %s'%(stmt, params))
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
        if self._debug:
            print('%s <= %s'%(stmt, paramslist))
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
        if params:
            vs = self._get_wheres(**params)
            stmt += ' WHERE {}'.format(vs)
        if clause:
            for k,v in clause.items():
                stmt += ' {0} {1}'.format(k, v)
        params = {k:v for k,v in params.items() if not isinstance(v, dict)}
        results = self.execute(stmt, commit=False, fetchall=fetchall, **params)
        return results

    def exists(self, tbl, rec, keyflds, **params):
        if rec and keyflds:
            ws = self._get_keyflds(keyflds)
        elif not rec and len(params) > 0:
            rec = {k: v for k, v in params.items()}
            ws = self._get_wheres(**params)
        else:
            return False
        stmt = 'SELECT count(1) AS count FROM {} WHERE {}'.format(tbl, ws)
        r = self.execute(stmt, commit=False, fetchall=True, **rec)
        return r[0].count != 0

    def count(self, tbl, **params):
        ws = self._get_wheres(**params)
        stmt = 'SELECT count(1) AS count FROM {} WHERE {}'.format(tbl, ws)
        params = {k:v for k,v in params.items() if not isinstance(v, dict)}
        r = self.execute(stmt, commit=False, fetchall=True, **params)
        return r[0].count

    def insert(self, tbl, recs):
        if recs is None or len(recs) == 0:
            return 0
        ks = recs[0].keys()
        fs = ','.join(ks)
        vs = ','.join(['%({})s'.format(s) for s in ks])
        stmt = 'INSERT INTO {} ({}) VALUES({})'.format(tbl, fs, vs)
        commit = not self.transaction
        rc = self.executemany(stmt, paramslist=recs, commit=commit, fetchall=False)
        return rc._rows._cursor.rowcount

    def update(self, tbl, recs, keyflds):
        if recs is None or len(recs) == 0:
            return 0
        rec = recs[0]
        ks = rec.keys()
        vs = ','.join(['{0}=%({0})s'.format(s) for s in ks])
        ws = self._get_keyflds(keyflds)
        stmt = 'UPDATE {} SET {} WHERE {}'.format(tbl, vs, ws)
        commit = not self.transaction
        rc = self.executemany(stmt, paramslist=recs, commit=commit, fetchall=False)
        return rc._rows._cursor.rowcount

    def upsert(self, tbl, recs, keyflds):
        if recs is None or len(recs) == 0:
            return 0
        recs_update = []
        recs_insert = []
        for idx,rec in enumerate(recs):
            if not self.exists(tbl, rec, keyflds):
                if self._exist_in_recs(idx, recs, keyflds):
                    recs_update.append(rec)
                else:
                    recs_insert.append(rec)
            else:
                recs_update.append(rec)
        ic = self.insert(tbl, recs_insert)
        uc = self.update(tbl, recs_update, keyflds)
        return ic, uc

    def delete(self, tbl, recs, keyflds, **params):
        if recs and keyflds:
            ws = self._get_keyflds(keyflds)
        elif not recs and len(params) > 0:
            recs = [{k: v for k, v in params.items()}]
            ws = self._get_wheres(**params)
        else:
            return 0
        stmt = 'DELETE FROM {} WHERE {}'.format(tbl, ws)
        commit = not self.transaction
        rc = self.executemany(stmt, paramslist=recs, commit=commit, fetchall=False)
        return rc._rows._cursor.rowcount

    def _cond_map(self, o):
        hm = {
            None: 'ISNULL(%s)',
            'like': '%s LIKE %s',
            '<>': '%s <> %s'
        }
        fld, val = list(o.items())[0]
        k, v = list(val.items())[0] if isinstance(val, dict) else (None, None)
        k = k if k is None else k.lower()
        if k is None:
            s = hm[k] % fld
        elif k == 'or':
            s = ' OR '.join([f'{fld}="{t}"' if isinstance(t, str) else f'{fld}={t}' for t in v])
        elif k == 'range':
            vs = v[0]
            ve = v[1]
            vs = f'{fld}>="{vs}"' if isinstance(vs, str) else f'{fld}>={vs}'
            ve = f'{fld}<"{ve}"' if isinstance(ve, str) else f'{fld}<{ve}'
            s = ' AND '.join([vs, ve])
        elif k in hm:
            v = f'"{v}"' if isinstance(v, str) else v
            s = hm[k] % (fld, v)
        else:
            s = '%s %s'%(fld, v)
        if s:
            s = f'({s})'
        return s

    def _get_keyflds(self, keyflds):
        ws = ['{0}=%({0})s'.format(k) if isinstance(k, str) else self._cond_map(k) for k in keyflds]
        ws.append('1=1')
        ws = ' AND '.join(ws)
        return ws

    def _get_wheres(self, **params):
        ks = params.keys()
        ws = ['{0}=%({0})s'.format(k) if not any([isinstance(params[k], dict), params[k] is None]) else self._cond_map({k:params[k]}) for k in ks]
        ws.append('1=1')
        ws = ' AND '.join(ws)
        return ws

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
