import time
import pymongo
from pymongo import UpdateOne, DeleteOne
from pymongo.errors import ConnectionFailure

from . import utils
from .records import Record, DocumentCollection, ZwdbError

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
            'password'  : o['pwd']
        }
        self.dbname = o['db']
        self.dbcfg.update(kwargs)
        cfg = {
            'maxPoolSize' : 100
        }
        for p in cfg:
            self.dbcfg[p] = self.dbcfg.get(p, cfg[p])

        self.client = None
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

    @property
    def version(self):
        return pymongo.version
    
    @property
    def server_info(self):
        return self.client.server_info()

    def get_connection(self):
        db = self.client[self.dbname]
        return ZWMongoConnection(db)

    def close(self):
        if self.client:
            self.client.close()

    def lists(self):
        return self.client[self.dbname].list_collection_names()
    
    def find(self, coll, conds=None, projection=None, sort=None, limit=0, fetchall=False, **params):
        conn = self.get_connection()
        docs = conn.find(coll, conds, projection, sort, limit, fetchall, **params)
        if fetchall:
            conn.close()
        return docs

    def insert(self, coll, recs, ordered=False):
        with self.get_connection() as conn:
            rtn = conn.insert(coll, recs, ordered)
        return rtn

    def update(self, coll, recs, keyflds=None):
        with self.get_connection() as conn:
            rtn = conn.update(coll, recs, keyflds)
        return rtn

    def upsert(self, coll, recs, keyflds=None):
        with self.get_connection() as conn:
            rtn = conn.upsert(coll, recs, keyflds)
        return rtn
    
    def delete(self, coll, recs, keyflds=None):
        with self.get_connection() as conn:
            rtn = conn.delete(coll, recs, keyflds)
        return rtn

    def count(self, coll, conds=None):
        with self.get_connection() as conn:
            rtn = conn.count(coll, conds)
        return rtn        

    def drop_collection(self, coll):
        return self.client[self.dbname].drop_collection(coll)

    def __repr__(self):
        return '<Database host={}:{}>'.format(self.dbcfg['host'], self.dbcfg['port'])

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    @classmethod
    def id2time(cls, objid):
        objid = str(objid)
        timestamp = int(objid[:8], 16)
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp))

class ZWMongoConnection(object):
    def __init__(self, db):
        self._db = db
        self._cursor = None
        self.open = True
    
    def close(self):
        self._close_cursor()
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
            rec = self._cursor.next()
        if rec:
            return rec
        else:
            self._close_cursor()
            raise StopIteration('Cursor contains no more docs.')

    def find(self, coll, conds=None, projection=None, sort=None, limit=0, fetchall=False, **params):
        sort = sort or [('_id', -1)]
        self._cursor = self._db[coll].find(filter=conds, projection=projection, sort=sort, limit=limit, **params)
        results = DocumentCollection(self)
        if fetchall:
            results.all()
        return results

    def insert(self, coll, recs, ordered=False):
        if recs is None or len(recs) == 0:
            return 0
        result = self._db[coll].insert_many(recs, ordered=ordered)
        return len(result.inserted_ids)
    
    def update(self, coll, recs, keyflds=None):
        if recs is None or len(recs) == 0:
            return 0
        filter_arr = []
        keyflds = keyflds or []
        if '_id' in keyflds:
            reqs = [ UpdateOne({'_id': o['_id']}, {'$set': o}, upsert=False) for i,o in enumerate(recs)]
        else:
            for rec in recs:
                conds = []
                for fld in keyflds:
                    conds.append({fld: rec[fld]})
                filter_arr.append(conds)
            reqs = [ UpdateOne({'$and': filter_arr[i]}, {'$set': o}, upsert=False) for i,o in enumerate(recs)]
        result = self._db[coll].bulk_write(reqs)
        return result.modified_count
    
    def upsert(self, coll, recs, keyflds=None):
        if recs is None or len(recs) == 0:
            return 0
        filter_arr = []
        keyflds = keyflds or []
        if '_id' in keyflds:
            reqs = [ UpdateOne({'_id': o['_id']}, {'$set': o}, upsert=True) for i,o in enumerate(recs)]
        else:
            for rec in recs:
                conds = []
                for fld in keyflds:
                    conds.append({fld: rec[fld]})
                filter_arr.append(conds)
            reqs = [ UpdateOne({'$and': filter_arr[i]}, {'$set': o}, upsert=True) for i,o in enumerate(recs)]
        result = self._db[coll].bulk_write(reqs)
        return result.modified_count, result.upserted_count
    
    def delete(self, coll, recs, keyflds=None):
        if recs is None or len(recs) == 0:
            return 0
        filter_arr = []
        keyflds = keyflds or []
        if '_id' in keyflds:
            reqs = [ DeleteOne({'_id': o['_id']}) for i,o in enumerate(recs)]
        else:
            for rec in recs:
                conds = []
                for fld in keyflds:
                    conds.append({fld: rec[fld]})
                filter_arr.append(conds)
            reqs = [ DeleteOne({'$and': filter_arr[i]}) for i,o in enumerate(recs)]
        result = self._db[coll].bulk_write(reqs)
        return result.deleted_count        

    def count(self, coll, conds=None):
        conds = conds or {}
        # return self._db[coll].count_documents(conds)
        return self._db[coll].count(conds)