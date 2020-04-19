# -*- coding: utf-8 -*-
import pytest
import os
import sys
from datetime import datetime

from zwdb.zwmysql import ZWMysql

@pytest.fixture(scope='module')
def db():
    db_url = 'mysql://tester:test@localhost/testdb'
    with ZWMysql(db_url) as dbobj:
        with dbobj.get_connection() as conn:
            tbl = 'CREATE TABLE `tbl` ( \
                `id` int(11) NOT NULL AUTO_INCREMENT, \
                `txt` varchar(45) DEFAULT NULL, \
                PRIMARY KEY (`id`) \
                ) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4;'
            tbl_insert = 'CREATE TABLE `testdb`.`tbl_insert` (\
                `id` INT NOT NULL,\
                `txt` VARCHAR(45) NULL,\
                `num` FLOAT NULL,\
                `none` VARCHAR(45) NULL,\
                `dt` DATETIME NULL,\
                PRIMARY KEY (`id`));'
            tbls = [tbl, tbl_insert]
            for t in tbls:
                conn.execute(t, commit=True)

            recs_insert = [{'id':1, 'txt':'abc'}, {'id':2, 'txt':None}, {'id':3, 'txt':'xxx'}]
            ks = recs_insert[0].keys()
            fs = ','.join(ks)
            vs = ','.join(['%({})s'.format(s) for s in ks])
            stmt = 'INSERT INTO tbl ({}) VALUES({})'.format(fs, vs)
            conn.executemany(stmt, fetchall=False, commit=True, paramslist=recs_insert)
        yield dbobj
        # clean
        with dbobj.get_connection() as conn:
            tbls = ['tbl', 'tbl_insert', 'tbl_create']
            for t in tbls:
                conn.execute('DROP TABLE IF EXISTS %s'%t, commit=True)

class TestMysql:
    def test_list(self, db):
        assert len(db.lists())>0
    
    def test_find(self, db):
        rs = db.find('tbl')
        assert rs.pending == True
        recs = list(rs)
        assert rs.pending == False and recs[0].txt == 'abc' and len(recs) == 3

        rs = db.find('tbl')
        assert rs.pending == True
        recs = rs.all()
        assert rs.pending == False and recs[0].txt == 'abc' and len(recs) == 3
  
        rs = db.find('tbl')
        assert rs.pending == True
        recs = [r for r in rs]
        assert rs.pending == False and recs[0].txt == 'abc' and len(recs) == 3
        
        rs = db.find('tbl', fetchall=True)
        assert rs.pending == False

        rs = db.find('tbl', fetchall=True, id=1, txt='abc')
        assert len(rs) == 1

        rs = db.find('tbl', clause={'order by':'txt desc', 'limit':1}, txt=None, fetchall=True)
        assert len(rs) == 1 and rs[0].id == 2

    @pytest.mark.parametrize(
        'recs', (
            [
                {'id':1, 'txt':'a', 'num':1.0, 'none':None, 'dt':datetime.now()},
                {'id':2, 'txt':'b', 'num':2.0, 'none':None, 'dt':datetime.now()}
            ],
        )
    )
    def test_insert(self, db, recs):
        c = db.insert('tbl_insert', recs)
        assert c == len(recs)

    @pytest.mark.parametrize(
        'recs, keyflds', (
            ([{'id':1, 'txt':'aa'},{'id':2, 'txt':'bb'}],['id']),
        )
    )
    def test_update(self, db, recs, keyflds):
        tbl = 'tbl_insert'
        c = db.update(tbl, recs, keyflds=keyflds)
        for rec in recs:
            r = db.find(tbl, id=rec['id'], fetchall=True)
            assert r[0].txt == rec['txt']
        assert c == len(recs)

    @pytest.mark.parametrize(
        'recs, keyflds', (
            ([{'id':1, 'txt':'a'},{'id':3, 'txt':'c', 'num':3.0, 'dt':datetime.now()}, {'id':3, 'txt':'haha'}],['id']),
        )
    )    
    def test_upsert(self, db, recs, keyflds):
        tbl = 'tbl_insert'
        c = db.upsert(tbl, recs, keyflds=keyflds)
        total_recs = db.find(tbl, fetchall=True)
        for i,rec in enumerate(recs):
            r = db.find(tbl, id=rec['id'], fetchall=True)
            if i == 1:
                assert r[0].txt == 'haha'
            else:
                assert r[0].txt == rec['txt']
        assert c[0]+c[1] == len(recs) and 3 == len(total_recs)

    @pytest.mark.parametrize(
        'recs, keyflds', (
            ([{'id':1},{'id':3}], ['id']),
        )
    )
    def test_delete(self, db, recs, keyflds):
        tbl = 'tbl_insert'
        c = db.delete('tbl_insert', recs, keyflds=keyflds)
        total_recs = db.find(tbl, fetchall=True)
        assert c == 2 and 1 == len(total_recs)
    
    def test_exec_script(self, db):
        r = db.exec_script('data/tbl_create.sql')
        with db.get_connection() as conn:
            rs = conn.execute("show tables like 'tbl_create';", fetchall=True)
        assert len(rs) == 1