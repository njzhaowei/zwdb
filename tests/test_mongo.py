# -*- coding: utf-8 -*-
import pytest
import os
import sys
from datetime import datetime

from zwdb.zwdb import Database

@pytest.fixture(scope='module')
def db():
    db_url = 'mongo://tester:test@localhost/testdb'
    with Database(db_url, maxPoolSize=200) as dbobj:
        yield dbobj

class TestDatabase:
    def test_get_table_names(self, db):
        nms = db.get_table_names()
        assert len(nms)>0
  
    # def test_findmany(self, db):
    #     rs = db.findmany('tbl_0')
    #     assert rs.pending == True
    #     recs = list(rs)
    #     assert rs.pending == False
    #     assert recs[0].txt == 'a'
    #     assert len(recs) == 3

    #     rs = db.findmany('tbl_0')
    #     assert rs.pending == True
    #     recs = rs.all()
    #     assert rs.pending == False
    #     assert recs[0].txt == 'a'
    #     assert len(recs) == 3

    #     rs = db.findmany('tbl_0')
    #     assert rs.pending == True
    #     recs = [r for r in rs]
    #     assert rs.pending == False
    #     assert recs[0].txt == 'a'
    #     assert len(recs) == 3

    #     rs = db.findmany('tbl_0', fetchall=True)
    #     assert rs.pending == False

    #     rs = db.findmany('tbl_0', fetchall=True, id=1)
    #     assert len(rs) == 1

    # @pytest.mark.parametrize(
    #     'rec', (
    #         {'id':1, 'txt':'a', 'num':1, 'none':None, 'dt':datetime.now()},
    #     )
    # ) 
    # def test_insertmany(self, db, rec):
    #     tbl = 'tbl_insert'
    #     db.execute('TRUNCATE %s'%tbl, commit=True)
    #     c = db.insertmany(tbl, [rec])
    #     assert c == 1
    
    # @pytest.mark.parametrize(
    #     'rec, keyflds', (
    #         ({'id':1, 'txt':'a', 'num':333, 'none':None, 'dt':datetime.now()}, ['id']),
    #     )
    # ) 
    # def test_updatemany(self, db, rec, keyflds):
    #     c = db.updatemany('tbl_insert', [rec], keyflds)
    #     assert c == 1
    
    # @pytest.mark.parametrize(
    #     'recs, keyflds', (
    #         (
    #             [
    #                 {'id':1, 'txt':'a', 'num':555, 'none':None, 'dt':datetime.now()},
    #                 {'id':999, 'txt':'a', 'num':222, 'none':None, 'dt':datetime.now()}
    #             ], 
    #             ['id']
    #         ),
    #     )
    # ) 
    # def test_updsertmany(self, db, recs, keyflds):
    #     c = db.upsertmany('tbl_insert', recs, keyflds)
    #     assert c == 2

    # @pytest.mark.parametrize(
    #     'rec, keyflds', (
    #         ({'id':1}, ['id']),
    #     )
    # ) 
    # def test_deletemany(self, db, rec, keyflds):
    #     c = db.deletemany('tbl_insert', [rec], keyflds)
    #     assert c == 1