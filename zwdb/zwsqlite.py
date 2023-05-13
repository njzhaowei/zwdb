import sqlite3
import traceback
import logging
from .records import RecordCollection

class ZWSqlite(object):
    """Class defining a sqlite driver"""
    def __init__(self, db_url, **kwargs):
        # file:C://test.db or test.db
        self.isuri = True if str(db_url).startswith('file:') else False
        self.dbcfg = {
        }
        self.dburl = db_url
        self.dbcfg.update(kwargs)
        cfgdef = {
        }
        for k, v in cfgdef.items():
            self.dbcfg[k] = self.dbcfg.get(k, v)
        self.debug = self.dbcfg.get('debug', False)

    @property
    def version(self):
        '''Return sqlite_version'''
        return sqlite3.sqlite_version

    @property
    def info(self):
        '''Return version and sqlite_version'''
        return {
            'sqlite_version': sqlite3.sqlite_version,
            'version': sqlite3.version,
        }

    def get_connection(self):
        conn = sqlite3.connect(self.dburl, uri=self.isuri)
        return ZWSqliteConnection(conn, debug=self.debug)

    def close(self):
        pass

    def lists(self):
        '''List all tables in database'''
        with self.get_connection() as conn:
            recs = conn.execute("SELECT name FROM sqlite_master WHERE type='table' order by name", fetchall=True)
        return recs

    def find(self, tbl, clause=None, fetchall=False, **params):
        '''Select records from table

        :param str tbl: table name
        :param dict clause: clause setting such as LIMIT, ORDER BY
        :param bool fetchall: fetch all or not
        :param dict params: select where condition
        :return: result set
        :rtype: :py:class:`zwtk.sqlite.RecordCollection`

        .. code-block:: Python
            :linenos:

            db.find('tbl', clause={'ORDER BY num': 'DESC'})
            db.find('tbl', none=None, txt={'like': 'txt%'}, num={'<>': 2})
            db.find('tbl', num={'range': (1, 3)}) # 1<=num<3
            db.find('tbl', num={'or': (2, 3)})
        '''
        conn = self.get_connection()
        recs = conn.find(tbl, clause, fetchall, **params)
        if fetchall:
            conn.close()
        return recs

    def findone(self, tbl, clause=None, **params):
        '''Select one record from table

        :param str tbl: table name
        :param dict clause: clause setting such as LIMIT, ORDER BY
        :param dict params: select where condition
        :return: result record or None
        :rtype: :py:class:`zwtk.sqlite.Record`

        .. code-block:: Python
            :linenos:

            db.findone('tbl', clause={'ORDER BY num': 'DESC'} num={'>': 1})
        '''
        clause = clause or {}
        clause['limit'] = 1
        recs = self.find(tbl, clause=clause, fetchall=True, **params)
        return recs[0] if len(recs)>0 else None

    def exists(self, tbl, rec=None, keyflds=None, **params):
        '''Check existence of record

        :param str tbl: table name
        :param `zwtk.sqlite.Record`/dict rec: record object to check
        :param list(str) keyflds: key field list
        :param dict params: select where condition
        :return: exist or not
        :rtype: bool

        .. code-block:: Python
            :linenos:

            recs = {'id':1, 'txt': 'txt1', 'num': 1 }
            db.exists('tbl', recs, keyflds=['id'])
        '''
        with self.get_connection() as conn:
            rtn = conn.exists(tbl, rec, keyflds, **params)
        return rtn

    def count(self, tbl, **params):
        '''Get table count by where condition in params

        :param str tbl: table name
        :param dict params: select where condition
        :return: count
        :rtype: int
        '''
        with self.get_connection() as conn:
            rtn = conn.count(tbl, **params)
        return rtn

    def insert(self, tbl, recs):
        '''Insert recs into table, return insert count

        :param str tbl: table name
        :param list(dict{str, object}) recs: record dict list
        :return: insert count
        :rtype: int

        .. code-block:: Python
            :linenos:

            recs = [
                {'txt': 'txt1', 'num': 1, 'dt': datetime.now()},
                {'txt': 'txt2', 'num': 2, 'dt': datetime.now()},
            ]
            db.insert('tbl', recs)
        '''
        with self.get_connection() as conn:
            rtn = conn.insert(tbl, recs)
        return rtn

    def update(self, tbl, recs, keyflds):
        '''Update recs in table, return update count

        :param str tbl: table name
        :param list(dict{str, object}) recs: record dict list
        :param list(str) keyflds: key field(s) list used to locate records
        :return: update count
        :rtype: int

        .. code-block:: Python
            :linenos:

            recs = [
                {'id':1, 'txt': '1txt', 'num': 1.5 },
            ]
            db.update('tbl', recs, keyflds=['id'])
        '''
        with self.get_connection() as conn:
            rtn = conn.update(tbl, recs, keyflds)
        return rtn

    def upsert(self, tbl, recs, keyflds):
        '''Update(if exists)/Insert(if not) recs in table, return update/insert count

        :param str tbl: table name
        :param list(dict{str, object}) recs: record dict list
        :param list(str) keyflds: key field(s) list used to locate records
        :return: (insert_count, update_count)
        :rtype: tuple

        .. code-block:: Python
            :linenos:

            recs = [
                {'id':1, 'txt': 'txt1', 'num': 1 },
                {'id':4, 'txt': 'txt4', 'num': 4 },
                {'id':5, 'txt': 'txt5', 'num': 5 },
            ]
            db.upsert('tbl', recs, keyflds=['id'])
        '''
        with self.get_connection() as conn:
            rtn = conn.upsert(tbl, recs, keyflds)
        return rtn

    def delete(self, tbl, recs=None, keyflds=None, **params):
        '''Delete recs by record list or where condition, return delete count

        :param str tbl: table name
        :param list(dict{str, object}) recs: record dict list
        :param list(str) keyflds: key field(s) list used to locate records
        :param dict params: select where condition
        :return: delete
        :rtype: int

        .. code-block:: Python
            :linenos:

            recs = [
                {'id':4, 'txt': 'txt4', 'num': 4 },
            ]
            db.delete('tbl', recs, keyflds=['id'])
            db.delete('tbl', dt=None)
        '''
        with self.get_connection() as conn:
            rtn = conn.delete(tbl, recs, keyflds, **params)
        return rtn

    def select(self, stmt, fetchall=True, **params):
        '''Run raw select sql

        :param str stmt: sql statement
        :param bool fetchall: fetch all records
        :param dict params: select where condition
        :return: result set
        :rtype: :py:class:`zwtk.sqlite.RecordCollection`

        .. code-block:: Python
            :linenos:

            db.select('SELECT txt FROM tbl WHERE id=1')
        '''
        conn = self.get_connection()
        rtn =  conn.execute(stmt, fetchall=fetchall, **params)
        if fetchall:
            conn.close()
        return rtn

    def exec_script(self, fp):
        '''Run sql script file

        :param str fp: sql file path
        :return: success or not
        :rtype: bool

        .. code-block:: Python
            :linenos:

            db.exec_script('C:/data.sql')
        '''
        try:
            with self.get_connection() as conn:
                statement = ''
                with open(fp, encoding='utf-8') as fs:
                    for line in fs:
                        line = line.strip()
                        if line.startswith('--'):  # ignore sql comment lines
                            continue
                        if not line.endswith(';'):  # keep appending lines that don't end in ';'
                            statement = statement + line
                        else:  # when you get a line ending in ';' then exec statement and reset for next statement
                            statement = statement + line
                            conn.execute(statement)
                            statement = ''
        except Exception:
            logging.error(traceback.format_exc())
            return False
        return True

    def __repr__(self):
        return '<Database dburl={}>'.format(self.dburl)

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

class ZWSqliteConnection(object):
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
        self._close_conn()
        self.open = False

    def _close_conn(self):
        if self._conn:
            self._conn.close()
            self._conn = None

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
        self._cursor = self._conn.execute(stmt, params)
        keys = [o[0] for o in self._cursor.description] if self._cursor.description else None
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
        self._cursor = self._conn.executemany(stmt, paramslist)
        keys = [o[0] for o in self._cursor.description] if self._cursor.description else None
        if commit:
            self._conn.commit()
        row_gen = (keys, self)
        results = RecordCollection(*row_gen)
        if fetchall:
            results.all()
        return results

    def find(self, tbl, clause=None, fetchall=False, **params):
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
        vs = ','.join([':{}'.format(s) for s in ks])
        stmt = 'INSERT INTO {} ({}) VALUES({})'.format(tbl, fs, vs)
        commit = not self.transaction
        rc = self.executemany(stmt, paramslist=recs, commit=commit, fetchall=False)
        return rc._rows._cursor.rowcount

    def update(self, tbl, recs, keyflds):
        if recs is None or len(recs) == 0:
            return 0
        rec = recs[0]
        ks = rec.keys()
        vs = ','.join(['{0}=:{0}'.format(s) for s in ks])
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

    def _get_wheres(self, **params):
        ks = params.keys()
        ws = [self._cond_map({k:params[k]}) if any([isinstance(params[k], dict), params[k] is None]) else '{0}=:{0}'.format(k) for k in ks]
        # ws = ['{0}=%({0})s'.format(k) if not any([isinstance(params[k], dict), params[k] is None]) else self._cond_map({k:params[k]}) for k in ks]
        ws.append('1=1')
        ws = ' AND '.join(ws)
        return ws

    def _cond_map(self, o):
        hm = {
            None: '%s IS NULL',
            'like': '%s LIKE %s',
            '<>': '%s <> %s',
            '>': '%s > %s',
            '>=': '%s >= %s',
            '<': '%s < %s',
            '<=': '%s <= %s',
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
        ws = ['{0}=:{0}'.format(k) if isinstance(k, str) else self._cond_map(k) for k in keyflds]
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
