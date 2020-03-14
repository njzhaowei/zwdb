import traceback
import redis

from .logger import getLogger
LOG = getLogger(__name__)

class MyRedis():
    def __init__(self, cfg=None):
        cfg = cfg or {}
        self.dbcfg = {
            'host'    : cfg['host'] if 'host' in cfg else 'localhost',
            'port'    : cfg['port'] if 'port' in cfg else 6379
        }
        self._pool = redis.ConnectionPool(
            host=self.dbcfg['host'],
            port=self.dbcfg['port'],
            decode_responses=True)
        self._conn = redis.Redis(connection_pool=self._pool)

        self.dry_run = cfg['dry_run'] if 'dry_run' in cfg else False
        if self.dry_run:
            LOG.warning('Redis in dry run mod, nothing will store to db!!')

    def exists(self, key):
        return self._conn.exists(key) == 1

    def dbsize(self):
        return self._conn.dbsize()

    def conn(self):
        return self._conn
