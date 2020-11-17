from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError

from . import utils
from .records import Record, DocumentCollection, ZwdbError

class ZWElastic(object):
    '''Class defining a Mongo driver'''
    def __init__(self, dburl, **kwargs):
        o = utils.db_url_parser(dburl)
        p = o['props']
        self.dbcfg = {
            'hosts'     : [{'host': o['host'], 'port': o['port'] or 9200}],
            'http_auth' : (o['usr'], o['pwd'])
        }
        self.dbcfg.update(kwargs)
        cfg = {
            'maxsize' : 25
        }
        for p in cfg:
            self.dbcfg[p] = self.dbcfg.get(p, cfg[p])
            
        self.es = Elasticsearch(**self.dbcfg)
    
    def find(self, index, body=None, pgnum=0, pgsize=10, **params):
        body = body or {}
        pgfrom = pgnum*pgsize
        body['from'] = pgfrom
        body['size'] = pgsize
        return self.es.search(index=index, body=body, **params)
    
    def create_index(self, index, body=None):
        rtn = None
        if not self.es.indices.exists(index=index):
            body = body or {}
            try:
                rtn = self.es.indices.create(index=index, body=body)
            except Exception as ex:
                raise ZwdbError('Create index error, {}, {}:{}'.format(ex, self.dbcfg['host'], self.dbcfg['port']))
        return rtn is not None
    
    def create(self, index, docid=None, body=None, **params):
        try:
            self.es.create(index=index, id=docid, body=body, **params)
        except ConflictError:
            return False
        return True

    def delete(self, index, docid=None, body=None, **params):
        is_idx = index and not docid
        is_doc = index and docid
        is_qry = index and not docid and body
        rtn = None
        try:
            if is_idx and self.es.indices.exists(index=index):
                rtn = self.es.indices.delete(index=index)
            elif is_doc:
                rtn = self.es.delete(index=index, id=docid, **params)
            elif is_qry:
                rtn = self.es.delete_by_query(index=index, body=body, **params)
            else:
                raise ZwdbError('not support!')
        except Exception as ex:
            raise ZwdbError('Delete error, {}'.format(ex))
        return rtn is not None
    
    def exists(self, index, docid=None):
        is_idx = index and not docid
        is_doc = index and docid
        rtn = False
        if is_idx:
            rtn = self.es.indices.exists(index=index)
        elif is_doc:
            rtn = self.es.exists(index=index, id=docid)
        return rtn
    
    def count(self, index, body=None):
        body = body or {
            'query': {
                'match_all': {}
            }
        }
        o = self.es.count(index=index, body=body)
        return o['count']

    def version(self):
        return self.es.info()
    
    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()