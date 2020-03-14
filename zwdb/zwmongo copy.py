import time
import traceback
import pymongo
from pymongo import UpdateOne,DeleteOne
from bson import ObjectId
from .logger import getLogger
LOG = getLogger(__name__)

class MyMongo():
    def __init__(self, cfg, db_name):
        self.dbcfg = {
            'usr'     : cfg['usr'] if 'usr' in cfg else '',
            'pwd'     : cfg['pwd'] if 'pwd' in cfg else '',
            'host'    : cfg['host'] if 'host' in cfg else 'localhost',
            'port'    : cfg['port'] if 'port' in cfg else 27017
        }
        self.db_name = db_name
        self.dry_run = cfg['dry_run'] if 'dry_run' in cfg else False
        if self.dry_run:
            LOG.warning('Mongo in dry run mod, nothing will store to db!!')

    def get_client(self):
        client = pymongo.MongoClient('mongodb://%s:%s@%s:%s' % (
            self.dbcfg['usr'],
            self.dbcfg['pwd'],
            self.dbcfg['host'],
            self.dbcfg['port']
        ))
        return client

    def insert(self, coll_name, o):
        if self.dry_run:
            return True
        if o is None:
            LOG.warn('object is empty, do nothing for %s', coll_name)
            return True
        return self.insert_many(coll_name, [o])

    def insert_many(self, coll_name, recs):
        if self.dry_run:
            return True
        if len(recs)<1:
            LOG.debug('recs is empty, do nothing for %s', coll_name)
            return
        try:
            client = self.get_client()
            coll = client[self.db_name][coll_name]
            result = coll.insert_many(recs)
            LOG.debug('INSERT %s.%s(%s)', self.db_name, coll_name, len(result.inserted_ids))
        except Exception:
            LOG.error(traceback.format_exc())
            return False
        finally:
            client.close()
        return True
    
    def upsert(self, coll_name, o, keyflds=None):
        if self.dry_run:
            return True
        if o is None:
            return False
        return self.upsert_many(coll_name, [o], keyflds)

    def upsert_many(self, coll_name, recs, keyflds=None):
        if self.dry_run:
            return True
        if len(recs)<1:
            LOG.debug('recs is empty, do nothing for %s', coll_name)
            return False
        arr = []
        flds = [] if keyflds is None else keyflds
        for r in recs:
            conds = []
            for fld in flds:
                conds.append({fld: r[fld]})
            arr.append(conds)
        try:
            client = self.get_client()
            coll = client[self.db_name][coll_name]
            if '_id' in flds:
                upserts=[ UpdateOne({'_id': x['_id']}, {'$set':x}, upsert=True) for i,x in enumerate(recs)]
            else:
                upserts=[ UpdateOne({'$and':arr[i]}, {'$set':x}, upsert=True) for i,x in enumerate(recs)]
            result = coll.bulk_write(upserts)
            LOG.debug('UPSERT %s.%s(%s)', self.db_name, coll_name, result.upserted_count+result.modified_count)
        except Exception:
            LOG.error(traceback.format_exc())
            return False
        finally:
            client.close()
        return True

    def find(self, coll_name, conds=None, flds=None, sort=None):
        rtn = None
        conds = conds or {}
        sort = sort or ('_id', -1)
        try:
            client = self.get_client()
            coll = client[self.db_name][coll_name]
            if flds is None:
                rtn = list(coll.find(conds).sort(*sort))
            else:
                rtn = list(coll.find(conds, flds).sort(*sort).limit(1))
            rtn = rtn[0] if len(rtn) == 1 else None
        except Exception:
            LOG.error(traceback.format_exc())
        finally:
            client.close()
        return rtn

    def find_many_limit(self, coll_name, conds=None, flds=None, sort=None, limit=None):
        rtn = []
        conds = conds or {}
        sort = sort or ('_id', -1)
        limit = limit or 100
        try:
            client = self.get_client()
            coll = client[self.db_name][coll_name]
            if flds is None:
                rtn = list(coll.find(conds).sort(*sort).limit(limit))
            else:
                rtn = list(coll.find(conds, flds).sort(*sort).limit(limit))
            LOG.debug('FIND %s.%s(%s)', self.db_name, coll_name, len(rtn))
        except Exception:
            LOG.error(traceback.format_exc())
            return None
        finally:
            client.close()
        return rtn

    def find_many(self, coll_name, conds=None, flds=None, sort=None):
        rtn = []
        conds = conds or {}
        sort = sort or ('_id', -1)
        try:
            client = self.get_client()
            coll = client[self.db_name][coll_name]
            if flds is None:
                rtn = list(coll.find(conds).sort(*sort))
            else:
                rtn = list(coll.find(conds, flds).sort(*sort))
            LOG.debug('FIND %s.%s(%s)', self.db_name, coll_name, len(rtn))
        except Exception:
            LOG.error(traceback.format_exc())
            return None
        finally:
            client.close()
        return rtn

    def delete(self, coll_name, o, keyflds=None):
        if self.dry_run:
            return True
        if o is None:
            return False
        return self.delete_many(coll_name, [o], keyflds)        

    def delete_many(self, coll_name, recs, keyflds=None):
        if self.dry_run:
            return True
        if len(recs)<1:
            LOG.debug('recs is empty, do nothing for %s', coll_name)
            return False
        arr = []
        flds = [] if keyflds is None else keyflds
        for r in recs:
            conds = []
            for fld in flds:
                conds.append({fld: r[fld]})
            arr.append(conds)
        try:
            client = self.get_client()
            coll = client[self.db_name][coll_name]
            ops = [DeleteOne({'$and':arr[i]}) for i in range(len(recs))]
            result = coll.bulk_write(ops)
            LOG.debug('DELETE %s.%s(%s)', self.db_name, coll_name, result.deleted_count)
        except Exception:
            LOG.error(traceback.format_exc())
            return False
        finally:
            client.close()
        return True

    def count(self, coll_name, conds=None):
        rtn = 0
        conds = conds or {}
        try:
            client = self.get_client()
            coll = client[self.db_name][coll_name]
            rtn = coll.count(conds)
        except Exception:
            LOG.error(traceback.format_exc())
            return False
        finally:
            client.close()
        return rtn

    @classmethod
    def id2time(cls, objid):
        objid = str(objid)
        timestamp = int(objid[:8], 16)
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp))