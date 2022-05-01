# -*- coding: utf-8 -*-
import pytest
import datetime
from zwdb.zwmysql import ZWMysql

DB_URL = 'mysql://tester:test@localhost/testdb'
TBLS = ['tbl', 'tbl_create']
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
    with ZWMysql(DB_URL) as dbobj:
        # clean
        with dbobj.get_connection() as conn:
            for t in TBLS:
                conn.execute('DROP TABLE IF EXISTS %s'%t, commit=True)
        with dbobj.get_connection() as conn:
            sql = 'CREATE TABLE `%s` ( \
                `id` int(11) NOT NULL AUTO_INCREMENT, \
                `txt` varchar(45) DEFAULT NULL, \
                `num` FLOAT NULL,\
                `none` VARCHAR(45) NULL,\
                `dt` DATETIME NULL,\
                PRIMARY KEY (`id`) \
                ) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4;' % TBLS[0]
            sqls = [sql]
            for t in sqls:
                conn.execute(t, commit=True)

            ks = RECS_INIT[0].keys()
            fs = ','.join(ks)
            vs = ','.join(['%({})s'.format(s) for s in ks])
            stmt = 'INSERT INTO {} ({}) VALUES({})'.format(TBLS[0], fs, vs)
            conn.executemany(stmt, fetchall=False, commit=True, paramslist=RECS_INIT)
        yield dbobj
        # clean
        with dbobj.get_connection() as conn:
            for t in TBLS:
                conn.execute('DROP TABLE IF EXISTS %s'%t, commit=True)

def test_info(db):
    tbls = db.lists()
    dbver = db.version
    dbsiz = db.pool_size
    assert len(tbls) == 1 and len(dbver.split('.')) == 3 and dbsiz == db.dbcfg['pool_size']

def test_find(db):
    tbl = TBLS[0]
    rs = db.find(tbl)
    assert rs.pending is True
    recs = list(rs)
    assert rs.pending is False and recs[0].txt == RECS_INIT[0]['txt'] and len(recs) == len(RECS_INIT)

    recs = db.find(tbl).all()
    assert recs[0].txt == RECS_INIT[0]['txt'] and len(recs) == len(RECS_INIT)

    recs = [r for r in db.find(tbl)]
    assert recs[0].txt == RECS_INIT[0]['txt'] and len(recs) == len(RECS_INIT)

    rs = db.find(tbl, fetchall=True)
    assert rs.pending is False and len(recs) == len(RECS_INIT)

    rs = db.find(tbl, fetchall=True, id=1, txt='abc')
    assert len(rs) == 1

    rs = db.find(tbl, clause={'order by':'num desc', 'limit':2}, none=None, fetchall=True)
    assert len(rs) == 2 and rs[0].id == 3

def test_insert(db):
    tbl = TBLS[0]
    c = db.insert(tbl, RECS_INSERT)
    assert c == len(RECS_INSERT)

def test_update(db):
    tbl = TBLS[0]
    c = db.update(tbl, RECS_UPDATE, keyflds=['num', {'none': None}])
    r = db.findone(tbl, txt='XXX')
    assert c == 1 and r is not None

def test_upsert(db):
    tbl = TBLS[0]
    insert_count, update_count = db.upsert(tbl, RECS_UPSERT, keyflds=['num', {'none': None}])
    total_count = db.count(tbl, none=None)
    r = db.findone(tbl, txt='XXX')
    assert insert_count == 1 and update_count == 1 and total_count == 6 and r is None

def test_delete(db):
    tbl = TBLS[0]
    a = db.delete(tbl, recs=None, keyflds=None)
    b = db.delete(tbl, recs=[], keyflds=[])
    c = db.delete(tbl, recs=None, keyflds=[])
    d = db.delete(tbl, recs=[], keyflds=None)
    r1 = db.delete(tbl, num=12)
    r2 = db.delete(tbl, recs=RECS_INSERT, keyflds=['txt', 'num'])
    assert all(o == 0 for o in [a, b, c, d]) and r1 == 1 and r2 == 2 and db.count(tbl) == len(RECS_INIT)

def test_exists(db):
    tbl = TBLS[0]
    a = db.exists(tbl, rec=None, keyflds=None)
    b = db.exists(tbl, rec=[], keyflds=[])
    c = db.exists(tbl, rec=None, keyflds=[])
    d = db.exists(tbl, rec=[], keyflds=None)
    r1 = db.exists(tbl, num=1)
    r2 = db.exists(tbl, rec=RECS_INIT[0], keyflds=['txt', 'num'])
    assert all(o is False for o in [a, b, c, d]) and r1 is True and r2 is True

@pytest.mark.parametrize(
    'stmt, params', (
        ('select * from '+TBLS[0]+' where txt=%(txt)s', {'txt': 'abc'}),
    )
)
def test_select(db, stmt, params):
    r = db.select(stmt, **params)
    assert r and len(r) == 1

def test_execscript(db):
    db.exec_script('data/tbl_create.sql')
    with db.get_connection() as conn:
        rs = conn.execute("show tables like 'tbl_create';", fetchall=True)
    assert len(rs) == 1

def test_transaction(db):
    tbl = TBLS[0]
    with db.transaction() as conn:
        conn.insert(tbl, RECS_INSERT[:1])
        conn.insert(tbl, RECS_INSERT[1:])
        with ZWMysql(DB_URL) as o:
            c = o.count(tbl)
            assert c == len(RECS_INIT)
    c = db.count(tbl)
    assert c == len(RECS_INIT)+len(RECS_INSERT)
