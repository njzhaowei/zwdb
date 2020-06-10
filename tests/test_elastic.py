# -*- coding: utf-8 -*-
import pytest
import os
import sys
from datetime import datetime

from zwdb.zwelastic import ZWElastic
from zwdb.zwmongo import ZWMongo

@pytest.fixture(scope='module')
def db():
    dburl = 'es://elastic:testtest@localhost:9200/'
    with ZWElastic(dburl) as mydb:
        yield mydb

# def test_sync(db):
#     mongourl = 'mongo://tester:test@localhost/gnews'
#     with ZWMongo(mongourl, maxPoolSize=50) as mongo:
#         srcs = mongo.find('sources')
#         srcs = {o['id']:o['name'] for o in srcs}
#         recs = mongo.find('articles')
#         for rec in recs:
#             body = {
#                 'title': rec['title'],
#                 'article_text': rec['article_text'],
#                 'source_mark': rec['source_mark'],
#                 'source_name': srcs[rec['source_mark']],
#                 'keywords': rec['meta_keywords'],
#                 'receive_time': rec['receive_time']
#             }
#             db.create(index='article', docid=rec['uid'], body=body)

def test_info(db):
    print(db.version())

def test_create_index(db):
    nm = 'testindex'
    db.create_index(index=nm, body={
        
    })
    assert db.exists(index=nm)

def test_create_doc(db):
    nm = 'testindex'
    db.create(index=nm, docid=1, body={
        'aa':'aa',
        'bb':'bb',
        'cc': 22
    })
    c = db.count(index=nm)
    assert c == 1

def test_delete_index(db):
    nm = 'testindex'
    db.delete(index=nm)
    assert not db.exists(index=nm)

def test_find(db):
    recs = db.find(index='article', pgnum=0, pgsize=12)
    assert 1