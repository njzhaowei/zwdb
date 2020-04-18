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

    def set(self, name, value):
        rtn = None
        if isinstance(value, str):
            rtn = self._conn.set(name, value)
        elif isinstance(value, dict):
            rtn = self._conn.hmset(name, value)
        elif isinstance(value, list):
            self._conn.delete(name)
            rtn = self._conn.rpush(name, *value)
        elif isinstance(value, set):
            self._conn.delete(name)
            rtn = self._conn.sadd(name, *value)
        else:
            rtn = self._conn.set(name, value)
        return rtn

    def get(self, name, data_type=None):
        rtn = None
        t = data_type if data_type is not None else self._conn.type(name)
        if t == 'string':
            rtn = self._conn.get(name)
        elif t == 'hash':
            rtn = self._conn.hgetall(name)
        elif t == 'list':
            rtn = self._conn.lrange(name, 0, -1)
        elif t == 'set':
            rtn = self._conn.smembers(name)
        else:
            rtn = self._conn.get(name)
        return rtn

    def setby(self, name, key, value, data_type=None):
        '''key: key(hash) or index(list)
        return None if not support
        '''
        rtn = None
        t = data_type if data_type is not None else self._conn.type(name)
        if t == 'hash':
            rtn = self._conn.hset(name, key, value)
        elif t == 'list':
            rtn = self._conn.lset(name, key, value)
        return rtn
    
    def getby(self, name, key, data_type=None):
        '''key: key(hash) or index(list)
        return None if not support
        '''
        rtn = None
        t = data_type if data_type is not None else self._conn.type(name)
        if t == 'hash':
            rtn = self._conn.hget(name, key)
        elif t == 'list':
            rtn = self._conn.lindex(name, key)
        return rtn
    
    def delby(self, name, keys, data_type=None):
        '''keys: keys(hash) or indexes(list) or values(set)
        return None if not support
        '''
        rtn = None
        t = data_type if data_type is not None else self._conn.type(name)
        if t == 'hash':
            rtn = self._conn.hdel(name, *keys)
        elif t == 'list':
            with self._conn.pipeline() as p:
                p.multi()
                for idx in keys:
                    p.lset(name, idx, '__ZWREDIS_DELETED__')
                rtn = p.lrem(name, 0, '__ZWREDIS_DELETED__')
                p.execute()
        elif t == 'set':
            self._conn.srem(name, *keys)
        return rtn
    
    def append(self, name, value, data_type=None):
        rtn = None
        t = data_type if data_type is not None else self._conn.type(name)
        if t == 'string':
            rtn = self._conn.append(name, value)
        elif t == 'hash':
            rtn = self._conn.hmset(name, value)
        elif t == 'list':
            rtn = self._conn.rpush(name, *value)
        elif t == 'set':
            rtn = self._conn.sadd(name, *value)
        else:
            rtn = self._conn.append(name, value)
        return rtn
    
    def contains(self, name, key, data_type=None):
        '''key: key(hash) or value(list/set) or substring(string)'''
        rtn = None
        t = data_type if data_type is not None else self._conn.type(name)
        if t == 'string':
            rtn = key in self._conn.get(name)
        elif t == 'hash':
            rtn = self._conn.hexists(name, key)
        elif t == 'list':
            with self._conn.pipeline() as p:
                p.multi()
                set_name = '_%s_tmp_set' % name
                p.delete(set_name)
                arr = self._conn.lrange(name, 0, -1)
                p.sadd(set_name, *arr)
                p.sismember(set_name, key)
                p.delete(set_name)
                rtn = p.execute()
                rtn = rtn[2]
        elif t == 'set':
            rtn = self._conn.sismember(name, key)
        else:
            rtn = key in self._conn.get(name)
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