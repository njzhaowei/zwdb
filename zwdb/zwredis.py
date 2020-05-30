import os
import warnings
import traceback
from redis import Redis
from redis.connection import BlockingConnectionPool

from . import utils

class ZWRedis():
    """Class defining a Redis driver"""
    def __init__(self, db_url, **kwargs):
        self.db_url = db_url or os.environ.get('DATABASE_URL')
        if not self.db_url:
            raise ValueError('You must provide a db_url.')

        o = utils.db_url_parser(db_url)
        self.dbcfg = {
            'host'      : o['host'],
            'port'      : o['port'] or 6379,
            'username'  : o['usr'],
            'password'  : o['pwd'],
            'db'        : o['db'] if 'db' in o and o['db'] and o['db'] != '' else 0,
            'decode_responses' : True,
        }
        try:
            self.dbcfg['db'] = int(o['db'])
        except (AttributeError, ValueError):
            self.dbcfg['db'] = 0

        self.dbcfg.update(kwargs)
        self._conn = Redis(connection_pool=BlockingConnectionPool(**self.dbcfg))
    
    def close(self):
        self._conn.connection_pool.disconnect()

    def set(self, key, value):
        rtn = None
        if isinstance(value, str):
            rtn = self._conn.set(key, value)
        elif isinstance(value, dict):
            rtn = self._conn.hmset(key, value)
        elif isinstance(value, list):
            self._conn.delete(key)
            rtn = self._conn.rpush(key, *value)
        elif isinstance(value, set):
            self._conn.delete(key)
            rtn = self._conn.sadd(key, *value)
        else:
            rtn = self._conn.set(key, value)
        return rtn

    def get(self, key, data_type=None):
        rtn = None
        t = data_type if data_type is not None else self._conn.type(key)
        if t == 'string':
            rtn = self._conn.get(key)
        elif t == 'hash':
            rtn = self._conn.hgetall(key)
        elif t == 'list':
            rtn = self._conn.lrange(key, 0, -1)
        elif t == 'set':
            rtn = self._conn.smembers(key)
        else:
            rtn = self._conn.get(key)
        return rtn

    def setby(self, key, field, value, data_type=None):
        '''key: key(hash) or index(list)
        return None if not support
        '''
        rtn = None
        t = data_type if data_type is not None else self._conn.type(key)
        if t == 'hash':
            rtn = self._conn.hset(key, field, value)
        elif t == 'list':
            rtn = self._conn.lset(key, field, value)
        return rtn
    
    def getby(self, key, field, data_type=None):
        '''key: key(hash) or index(list)
        return None if not support
        '''
        rtn = None
        t = data_type if data_type is not None else self._conn.type(key)
        if t == 'hash':
            rtn = self._conn.hget(key, field)
        elif t == 'list':
            rtn = self._conn.lindex(key, field)
        return rtn
    
    def delby(self, key, fields, data_type=None):
        '''fields: fields(hash) or indexes(list) or values(set)
        return None if not support
        '''
        rtn = None
        t = data_type if data_type is not None else self._conn.type(key)
        if t == 'hash':
            rtn = self._conn.hdel(key, *fields)
        elif t == 'list':
            with self._conn.pipeline() as p:
                p.multi()
                for idx in fields:
                    p.lset(key, idx, '__ZWREDIS_DELETED__')
                rtn = p.lrem(key, 0, '__ZWREDIS_DELETED__')
                p.execute()
        elif t == 'set':
            self._conn.srem(key, *fields)
        return rtn
    
    def append(self, key, value, data_type=None):
        rtn = None
        t = data_type if data_type is not None else self._conn.type(key)
        if t == 'string':
            rtn = self._conn.append(key, value)
        elif t == 'hash':
            rtn = self._conn.hmset(key, value)
        elif t == 'list':
            rtn = self._conn.rpush(key, *value)
        elif t == 'set':
            rtn = self._conn.sadd(key, *value)
        else:
            rtn = self._conn.append(key, value)
        return rtn
    
    def contains(self, key, field, data_type=None):
        '''key: key(hash) or value(list/set) or substring(string)'''
        rtn = None
        t = data_type if data_type is not None else self._conn.type(key)
        if t == 'string':
            rtn = field in self._conn.get(field)
        elif t == 'hash':
            rtn = self._conn.hexists(key, field)
        elif t == 'list':
            with self._conn.pipeline() as p:
                p.multi()
                set_name = '_%s_tmp_set' % key
                p.delete(set_name)
                arr = self._conn.lrange(key, 0, -1)
                p.sadd(set_name, *arr)
                p.sismember(set_name, field)
                p.delete(set_name)
                rtn = p.execute()
                rtn = rtn[2]
        elif t == 'set':
            rtn = self._conn.sismember(key, field)
        else:
            rtn = field in self._conn.get(field)
        return rtn
    
    def len(self, key, data_type=None):
        rtn = None
        t = data_type if data_type is not None else self._conn.type(key)
        if t == 'string':
            rtn = self._conn.strlen(key)
        elif t == 'hash':
            rtn = self._conn.hlen(key)
        elif t == 'list':
            rtn = self._conn.llen(key)
        elif t == 'set':
            rtn = self._conn.scard(key)
        else:
            rtn = self._conn.strlen(key)
        return rtn
    
    def all(self):
        rtn = []
        keys = self._conn.keys('*')
        for key in keys:
            rtn.append({
                'key': key,
                'value': self.get(key)
            })
        return rtn
    
    def all_iter(self, cbfunc):
        for key in self._conn.scan_iter():
            cbfunc(key)

    def delete(self, name):
        return self._conn.delete(name)

    def exists(self, key):
        return self._conn.exists(key) == 1

    def dbsize(self):
        return self._conn.dbsize()

    @property
    def conn(self):
        return self._conn

    def __repr__(self):
        return '<Database host={}:{}>'.format(self.dbcfg['host'], self.dbcfg['port'])

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()