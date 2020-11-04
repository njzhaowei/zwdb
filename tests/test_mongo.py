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
        colls = ['col', 'col_insert']
        for coll in colls:
            mydb.drop_collection(coll)
        recs = [{'txt':'a', 'num':1.0, 'none':None},{'txt':'b', 'num':2.0, 'none':None}]
        mydb.insert('col', recs)
        yield mydb

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

        r = db.findone(coll, conds={'id': recs[0]['id']})
        r = r.as_dict()
        old = r['txt']
        r['txt'] = 'bbb'
        db.update(coll, recs=[r], keyflds=['_id'])
        r = db.findone(coll, conds={'id': recs[0]['id']})
        assert r.txt == 'bbb'
        r = r.as_dict()
        r['_id'] = str(r['_id'])
        r['txt'] = old
        db.update(coll, recs=[r], keyflds=['_id'])
        assert r.txt == old

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

    def test_groupby(self, db):
        coll = 'col_insert'
        recs = db.groupby(coll, 'txt', sort={'count': -1})
        assert recs[0]['_id'] == 'a'
    
    def test_exists(self, db):
        assert db.exists('col', {'num': 1})
        assert not db.exists('col', {'num': 3})
    
    def test_leftjoin(self, db):
        coll = 'col'
        coll_right = 'col_insert'
        recs = [{'id': 'lj01', 'txt': 'lj01'},{'id': 'lj02', 'txt': 'lj02'}]
        recs_rignt = [{'id': 'lj01', 'num': 999},{'id': 'lj02', 'num': 666}]
        db.insert(coll, recs)
        db.insert(coll_right, recs_rignt)

        r = db.leftjoin(coll, coll_right, fld='id', fld_right='id', nameas='rec', match={'id': 'lj01'}, fetchall=True, project={'_id':0})
        assert len(r)==1 and r[0].rec[0]['num'] == 999

        db.delete(coll, recs, keyflds=['id'])
        db.delete(coll_right, recs_rignt, keyflds=['id'])

