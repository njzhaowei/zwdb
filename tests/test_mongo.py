# -*- coding: utf-8 -*-
import pytest
import os
import sys
from datetime import datetime

from zwdb.zwmongo import ZWMongo

@pytest.fixture(scope='module')
def db():
    db_url = 'mongo://tester:test@localhost/testdb'
    with ZWMongo(db_url, maxPoolSize=50) as mydb:
        recs = [{'txt':'a', 'num':1.0, 'none':None},{'txt':'b', 'num':2.0, 'none':None}]
        mydb.insert('col', recs)
        yield mydb
        colls = ['col', 'col_insert']
        for coll in colls:
            mydb.drop_collection(coll)

class TestMongo:
    def test_list(self, db):
        nms = db.lists()
        assert len(nms)>0

    @pytest.mark.parametrize(
        'recs', (
            [{'id': i, 'txt':'a', 'num':i, 'none':None} for i in range(10)],
        )
    )    
    def test_insert(self, db, recs):
        coll = 'col_insert'
        c = db.insert(coll, recs)
        assert c == len(recs)
    
    def test_find(self, db):
        coll = 'col_insert'
        rs = db.find(coll)
        assert rs.pending == True
        recs = list(rs)
        assert rs.pending == False and recs[0].num == 9 and len(recs) == 10

        rs = db.find(coll)
        assert rs.pending == True
        recs = rs.all()
        assert rs.pending == False and recs[0].num == 9 and len(recs) == 10
  
        rs = db.find(coll)
        assert rs.pending == True
        recs = [r for r in rs]
        assert rs.pending == False and recs[0].num == 9 and len(recs) == 10
        
        rs = db.find(coll, fetchall=True)
        assert rs.pending == False and len(rs) == 10

        rs = db.find(coll, conds={'id':0, 'txt':'a'}, fetchall=True)
        assert len(rs) == 1

    @pytest.mark.parametrize(
        'recs, keyflds', (
            ([{'id':0, 'txt':'aa'},{'id':1, 'txt':'bb'}],['id']),
        )
    )
    def test_update(self, db, recs, keyflds):
        coll = 'col_insert'
        c = db.update(coll, recs, keyflds=keyflds)
        for rec in recs:
            r = db.find(coll, conds={'id': rec['id']}, fetchall=True)
            assert r[0].txt == rec['txt']
        assert c == len(recs)

    @pytest.mark.parametrize(
        'recs, keyflds', (
            ([{'id':0, 'txt':'aaa'},{'id':11, 'txt':'a', 'num':11}],['id']),
        )
    )    
    def test_upsert(self, db, recs, keyflds):
        coll = 'col_insert'
        c = db.upsert(coll, recs, keyflds=keyflds)
        total_recs = db.find(coll, fetchall=True)
        for rec in recs:
            r = db.find(coll, conds={'id': rec['id']}, fetchall=True)
            assert r[0].txt == rec['txt']
        assert c[0]+c[1] == len(recs) and 11 == len(total_recs)

    @pytest.mark.parametrize(
        'recs, keyflds', (
            ([{'id':1},{'id':11}], ['id']),
        )
    )
    def test_delete(self, db, recs, keyflds):
        coll = 'col_insert'
        c = db.delete(coll, recs, keyflds=keyflds)
        total_recs = db.find(coll, fetchall=True)
        assert c == 2 and 9 == len(total_recs)
    
    def test_count(self, db):
        coll = 'col_insert'
        c = db.count(coll)
        assert c == 9