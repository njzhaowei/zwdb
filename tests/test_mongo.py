# -*- coding: utf-8 -*-
import pytest
import datetime
from zwdb.zwmongo import ZWMongo

DB_URL = 'mongo://tester:test@localhost/testdb'
COLLS = ['col', 'col_leftjoin']
RECS_INIT = [
    {'id': 1, 'txt': 'abc', 'num': 1, 'none': None, 'dt': datetime.datetime.now()},
    {'id': 2, 'txt': 'def', 'num': 2, 'none': None, 'dt': datetime.datetime.now()-datetime.timedelta(days=1)},
    {'id': 3, 'txt': 'ghi', 'num': 3, 'none': None, 'dt': datetime.datetime.now()-datetime.timedelta(days=2)},
]
RECS_INSERT = [
    {'txt': 'aaa', 'num': 10, 'none': None},
    {'txt': 'aaa', 'num': 11, 'none': None},
]
RECS_UPDATE = [
    {'txt': 'XXX', 'num': 10, 'none': None},
]
RECS_UPSERT = [
    {'txt': 'aaa', 'num': 10, 'none': None},
    {'txt': 'ccc', 'num': 12, 'none': None},
]

@pytest.fixture(scope='module')
def db():
    with ZWMongo(DB_URL, maxPoolSize=50) as mydb:
        # clean
        for coll in COLLS:
            mydb.drop_collection(coll)
        mydb.insert('col', RECS_INIT)
        yield mydb
        # clean
        for coll in COLLS:
            mydb.drop_collection(coll)

def test_info(db):
    colls = db.lists()
    dbver = db.version
    dbsiz = db.pool_size
    dbinf = db.server_info
    assert len(colls) == 1 and len(dbver.split('.')) == 3 and dbsiz == db.dbcfg['maxPoolSize']

def test_find(db):
    coll = COLLS[0]
    rs = db.find(coll)
    assert rs.pending is True
    recs = list(rs)
    assert rs.pending is False and recs[0].txt == RECS_INIT[2]['txt'] and len(recs) == len(RECS_INIT)

    recs = db.find(coll).all()
    assert recs[0].txt == RECS_INIT[2]['txt'] and len(recs) == len(RECS_INIT)

    recs = [r for r in db.find(coll)]
    assert recs[0].txt == RECS_INIT[2]['txt'] and len(recs) == len(RECS_INIT)

    rs = db.find(coll, fetchall=True)
    assert rs.pending is False and len(rs) == len(RECS_INIT)

    rs = db.find(coll, conds={'id':1, 'txt':'abc'}, fetchall=True)
    assert len(rs) == 1

    rs = db.find(coll, conds={'none': None}, sort=[('num', -1)], limit=2, fetchall=True)
    assert len(rs) == 2 and rs[0].id == 3

    r = db.findone(coll, conds={'id': 1})
    r1 = db.findone(coll, conds={'_id': r['_id']})
    r2 = db.findone(coll, conds={'_id': str(r['_id'])})
    assert r and r1 and r2 and r.id == r1.id and r.id == r2.id

def test_insert(db):
    coll = COLLS[0]
    c = db.insert(coll, RECS_INSERT)
    assert c == len(RECS_INSERT)

def test_update(db):
    coll = COLLS[0]
    c = db.update(coll, RECS_UPDATE, keyflds=['num', 'none'])
    r = db.findone(coll, conds={'txt': 'XXX'})
    assert c == 1 and r.num == 10

def test_upsert(db):
    coll = COLLS[0]
    insert_count, update_count = db.upsert(coll, RECS_UPSERT, keyflds=['num', 'none'])
    total_count = db.count(coll, conds={'none': None})
    r = db.findone(coll, conds={'txt': 'XXX'})
    assert insert_count == 1 and update_count == 1 and total_count == 6 and r is None

def test_delete(db):
    coll = COLLS[0]
    a = db.delete(coll, recs=None, keyflds=None)
    b = db.delete(coll, recs=[], keyflds=[])
    c = db.delete(coll, recs=None, keyflds=[])
    d = db.delete(coll, recs=[], keyflds=None)
    r1 = db.delete(coll, conds={'num': 12})
    r2 = db.delete(coll, recs=RECS_INSERT, keyflds=['txt', 'num'])
    assert all(o == 0 for o in [a, b, c, d]) and r1 == 1 and r2 == 2 and db.count(coll) == len(RECS_INIT)

def test_exists(db):
    coll = COLLS[0]
    a = db.exists(coll, rec=None, keyflds=None)
    b = db.exists(coll, rec=[], keyflds=[])
    c = db.exists(coll, rec=None, keyflds=[])
    d = db.exists(coll, rec=[], keyflds=None)
    r1 = db.exists(coll, conds={'num': 1})
    r2 = db.exists(coll, rec=RECS_INIT[0], keyflds=['txt', 'num'])
    assert all(o is False for o in [a, b, c, d]) and r1 is True and r2 is True

def test_groupby(db):
    coll = COLLS[0]
    recs = db.groupby(coll, 'none', sort={'count': -1})
    assert len(recs) == 1 and recs[0]['count'] == len(RECS_INIT)

def test_leftjoin(db):
    coll = COLLS[0]
    coll_right = COLLS[1]
    recs = [{'id': 'lj01', 'txt': 'lj01'},{'id': 'lj02', 'txt': 'lj02'}]
    recs_rignt = [{'id': 'lj01', 'num': 999},{'id': 'lj02', 'num': 666}]
    db.insert(coll, recs)
    db.insert(coll_right, recs_rignt)

    r = db.leftjoin(coll, coll_right, fld='id', fld_right='id', nameas='rec', match={'id': 'lj01'}, fetchall=True, project={'_id':0})
    assert len(r)==1 and r[0].rec[0]['num'] == 999

    db.delete(coll, recs, keyflds=['id'])
    db.delete(coll_right, recs_rignt, keyflds=['id'])
