# pylint: disable=arguments-differ
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError, NotFoundError

from .zwdbase import ZWDbase
from . import utils
from .records import ZwdbError

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

    def insert(self, index, docs, idfld='id', **params):
        docs = docs if isinstance(docs, list) else [docs]
        rs = []
        for doc in docs:
            docid = doc[idfld] if idfld in doc else None
            r = self.create(index, docid, doc, **params)
            rs.append(r)
        return all(rs)

    def update(self, index, docs=None, idfld='id', docids=None, script=None, upsert=False, **params):
        """
            https://www.elastic.co/guide/en/elasticsearch/reference/8.2/docs-update.html
        """
        es = self.es
        docs = docs if isinstance(docs, list) or docs is None else [docs]
        docids = docids if isinstance(docids, list) or docids is None else [docids]
        is_doc = index and docs is not None
        is_scr = index and not docs and docids and script
        rtn = False
        try:
            rs = []
            if is_doc:
                for doc in docs:
                    docid = doc[idfld] if idfld in doc else None
                    if docid:
                        r = es.update(index=index, id=docid, doc=doc, doc_as_upsert=upsert, **params)
                        rs.append(199<r.meta.status<300)
                    else:
                        rs.append(False)
            elif is_scr:
                for docid in docids:
                    r = es.update(index=index, id=docid, script=script, scripted_upsert=upsert, **params)
                    rs.append(199<r.meta.status<300)
            else:
                raise ZwdbError(f'Update oper not support! index: {index}, docs: {docs}, script: {script}')
            rtn = all(rs)
        except NotFoundError:
            rtn = False
        except Exception as ex:
            raise ZwdbError(f'Update error, {ex}') from ex
        return rtn

    def upsert(self, index, docs=None, idfld='id', docids=None, script=None, **params):
        return self.update(index, docs, idfld, docids, script, True, **params)

    def delete(self, index, docid=None, query=None, **params):
        is_idx = index and not docid and not query
        is_doc = index and docid
        is_qry = index and not docid and query
        rtn = False
        try:
            if is_idx:
                rtn = self.es.indices.delete(index=index, **params)
            elif is_doc:
                rtn = self.es.delete(index=index, id=docid, **params)
            elif is_qry:
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
        rtn = None
        try:
            if is_idx:
                r = es.indices.get(index=index, **params)
                rtn = r[index]
            elif is_doc:
                r = es.get(index=index, id=docid, **params)
                rtn = r['_source']
        except NotFoundError:
            pass
        return rtn

    def find(self, index, query=None, **params):
        query = query or {'match_all': {}}
        r = self.es.search(index=index, query=query, **params)
        hits = r['hits']
        rtn = {
            'total': hits['total']['value'],
            'docs': [o['_source'] for o in hits['hits']],
            'last': hits['hits'][-1]['sort'] if len(hits['hits'])>0 and 'sort' in hits['hits'][-1] else None,
        }
        return rtn

    def exists(self, index, docid=None, **params):
        is_idx = index and not docid
        is_doc = index and docid
        rtn = False
        if is_idx:
            rtn = self.es.indices.exists(index=index, **params)
        elif is_doc:
            rtn = self.es.exists(index=index, id=docid, **params)
        return bool(rtn)

    def count(self, index, query=None, **params):
        query = query or {
            'match_all': {}
        }
        o = self.es.count(index=index, query=query, **params)
        return o['count']

    def close(self):
        self.es.close()
