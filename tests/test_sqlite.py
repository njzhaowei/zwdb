# -*- coding: utf-8 -*-
# pylint: disable=redefined-outer-name

import pytest
from datetime import datetime
from zwdb.zwsqlite import ZWSqlite

DB_URL = 'file:./data/test.db'
SQL_PTH = './data/tbl_create_sqlite.sql'
TBLS = ['tbl', 'tbl_create']
RECS_INIT = [
    {'txt': 'txt1', 'num': 1, 'dt': datetime.now()},
    {'txt': 'txt2', 'num': 2, 'dt': datetime.now()},
    {'txt': '3txt', 'num': 3, 'dt': datetime.now()},
]

@pytest.fixture(scope='module')
def db():
    with ZWSqlite(DB_URL, debug=True) as dbobj:
        # clean
        with dbobj.get_connection() as conn:
            for t in TBLS:
                conn.execute('DROP TABLE IF EXISTS %s'%t, commit=True)

        with dbobj.get_connection() as conn:
            # When create a table that has an INTEGER PRIMARY KEY column,
            # this column is the alias of the rowid column
            # which is 64-bit signed integer and auto increment 
            sql = 'CREATE TABLE `%s` ( \
                `id` INTEGER PRIMARY KEY, \
                `txt` varchar(45) DEFAULT NULL, \
                `num` FLOAT NULL,\
                `none` VARCHAR(45) NULL,\
                `dt` DATETIME NULL\
                );' % TBLS[0]
            sqls = [sql]
            for t in sqls:
                conn.execute(t, commit=True)
        yield dbobj

        # clean
        with dbobj.get_connection() as conn:
            for t in TBLS:
                conn.execute('DROP TABLE IF EXISTS %s'%t, commit=True)

def test_info(db):
    dbver = db.version
    dbinfo = db.info
    assert dbver ==  dbinfo['sqlite_version']

def test_lists(db):
    assert len(db.lists())>0

def test_insert(db):
    tbl = TBLS[0]
    r = db.insert(tbl, recs=RECS_INIT)
    assert r == len(RECS_INIT)

def test_count(db):
    tbl = TBLS[0]
    c = db.count(tbl, none=None)
    assert c == len(RECS_INIT)

def test_find(db):
    tbl = TBLS[0]
    rs = db.find(tbl, none=None)
    assert rs.pending
    recs = rs.all()
    assert len(recs) == db.count(tbl, none=None)

    recs = db.find(tbl, clause={'ORDER BY num': 'DESC'}, fetchall=True)
    assert len(recs) == db.count(tbl) and recs[0].num == 3.0

    recs = db.find(tbl, none=None, txt={'like': 'txt%'}, num={'<>': 2}, fetchall=True)
    assert len(recs) == 1 and recs[0].txt == 'txt1'

    recs = db.find(tbl, num={'range': (1, 3)}, fetchall=True)
    assert len(recs) == 2

    recs = db.find(tbl, num={'or': (2, 3)}, fetchall=True)
    assert len(recs) == 2

def test_findone(db):
    tbl = TBLS[0]
    r = db.findone(tbl, clause={'ORDER BY num': 'DESC'}, num={'>': 1})
    assert r.txt == '3txt'

def test_exists(db):
    tbl = TBLS[0]
    recs = {'id':1, 'txt': 'txt1', 'num': 1 }
    r = db.exists(tbl, recs, keyflds=['id'])
    assert r
    recs = {'id':99, 'txt': 'txt1', 'num': 1 }
    r = db.exists(tbl, recs, keyflds=['id'])
    assert not r

def test_update(db):
    tbl = TBLS[0]
    recs = [
        {'id':1, 'txt': '1txt', 'num': 1.5 },
    ]
    c = db.update(tbl, recs, keyflds=['id'])
    r = db.findone(tbl, id=1)
    assert c == 1 and r.txt == '1txt' and r.num == 1.5

def test_upsert(db):
    tbl = TBLS[0]
    recs = [
        {'id':1, 'txt': 'txt1', 'num': 1 },
        {'id':4, 'txt': 'txt4', 'num': 4 },
        {'id':5, 'txt': 'txt5', 'num': 5 },
    ]
    ic, uc = db.upsert(tbl, recs, keyflds=['id'])
    r = db.findone(tbl, id=1)
    assert ic == 2 and uc == 1 and r.txt == 'txt1' and r.num == 1

def test_delete(db):
    tbl = TBLS[0]
    recs = [
        {'id':4, 'txt': 'txt4', 'num': 4 },
    ]
    c0 = db.count(tbl)
    r1 = db.delete(tbl, recs, keyflds=['id'])
    c1 = db.count(tbl)
    r2 = db.delete(tbl, dt=None)
    c2 = db.count(tbl)
    assert c1 == c0-1 and c2 == c0-2 and r1 == 1 and r1 == r2

def test_select(db):
    rs = db.select('SELECT txt FROM tbl WHERE id=1')
    assert len(rs) == 1 and rs[0].txt == 'txt1' and not hasattr(rs[0], 'num')

def test_exec_script(db):
    db.exec_script(SQL_PTH)
    assert len(db.lists()) == len(TBLS)