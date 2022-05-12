# pylint: disable=arguments-differ
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError, NotFoundError

from .zwdbase import ZWDbase
from . import utils
from .records import Record, DocumentCollection, ZwdbError

class ZWElastic(ZWDbase):
    '''Class defining a Mongo driver'''
    def __init__(self, dburl, https=True, **kwargs):
        o = utils.db_url_parser(dburl)
        self.dbcfg = {
            'hosts'     : [{'host': o['host'], 'port': o['port'] or 9200, 'scheme': 'https' if https else 'http'}],
            'basic_auth': (o['usr'], o['pwd'])
        }
        self.dbcfg.update(kwargs)
        cfgdef = {
            # 'maxsize' : 25
        }
        for k, v in cfgdef.items():
            self.dbcfg[k] = self.dbcfg.get(k, v)
        self.es = Elasticsearch(**self.dbcfg)

    @property
    def version(self):
        return self.es.info()['version']['number']

    @property
    def info(self):
        es = self.es
        o = es.info().body
        o['plugins'] = es.cat.plugins(format='json').body
        return o

    def create_index(self, index, settings=None, mappings=None, **params):
        if self.es.indices.exists(index=index):
            return False
        try:
            rtn = self.es.indices.create(index=index, settings=settings, mappings=mappings, **params)
        except Exception as ex:
            raise ZwdbError(f"Create index error, {ex}") from ex
        return rtn is not None

    def insert(self, index, docs, idfld='id', **params):
        docs = docs if isinstance(docs, list) else [docs]
        rs = []
        for doc in docs:
            docid = doc[idfld] if idfld in doc else None
            r = self.create(index, docid, doc, **params)
            rs.append(r)
        return all(rs)

    def create(self, index, docid, doc, **params):
        es = self.es
        if doc is None:
            return False
        try:
            es.create(index=index, id=docid, document=doc, **params)
        except ConflictError:
            return False
        except Exception as ex:
            raise ZwdbError(f"Create document error, {ex}, index: {index}, id: {docid}") from ex
        return True

    def delete(self, index, docid=None, query=None, **params):
        del_idx = index and not docid and not query
        del_doc = index and docid
        del_qry = index and not docid and query
        rtn = False
        try:
            if del_idx:
                rtn = self.es.indices.delete(index=index, **params)
            elif del_doc:
                rtn = self.es.delete(index=index, id=docid, **params)
            elif del_qry:
                rtn = self.es.delete_by_query(index=index, query=query, **params)
            else:
                raise ZwdbError(f'Delete oper not support! index: {index}, docid: {docid}, query: {query}')
            rtn = True
        except NotFoundError:
            rtn = True
        except Exception as ex:
            raise ZwdbError(f'Delete error, {ex}') from ex
        return rtn

    def findone(self, index, docid=None, **params):
        es = self.es
        is_idx = index and not docid
        is_doc = index and docid
        try:
            if is_idx:
                rtn = es.indices.get(index=index, **params)
            elif is_doc:
                rtn = es.get(index=index, id=docid, **params)
        except NotFoundError:
            return None
        return rtn.body

    def find(self, index, query=None, pgnum=0, pgsize=10, **params):
        query = query or {}
        pgfrom = pgnum*pgsize
        query['from'] = pgfrom
        query['size'] = pgsize
        return self.es.search(index=index, query=query, **params)

    def exists(self, index, docid=None, **params):
        is_idx = index and not docid
        is_doc = index and docid
        rtn = False
        if is_idx:
            rtn = self.es.indices.exists(index=index, **params)
        elif is_doc:
            rtn = self.es.exists(index=index, id=docid, **params)
        return bool(rtn)

    def count(self, index, query=None):
        query = query or {
            'match_all': {}
        }
        o = self.es.count(index=index, query=query)
        return o['count']

    def close(self):
        self.es.close()
