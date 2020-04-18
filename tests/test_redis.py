# -*- coding: utf-8 -*-
import pytest
import os
import sys
from datetime import datetime

from zwdb.zwredis import ZWRedis

@pytest.fixture(scope='module')
def db():
    db_url = 'redis://:111111@localhost:6379/0'
    with ZWRedis(db_url) as db:
        yield db

class TestRedis:
    def test_set(self, db):
        db.set('a', 1)
        db.set('b', 'b')
        db.set('hm', {'a':1, 'b':'b'})
        db.set('lt', [1, 'b', 1.1])
        db.set('st', {1,2,3})
        assert db.dbsize() == 5
    
    def test_get(self, db):
        a = db.get('a')
        b = db.get('b')
        c = db.get('hm')
        d = db.get('lt')
        e = db.get('st')
        t = db.get('a', data_type='string')
        assert a == '1' and b == 'b' and c['a'] == '1' and d[0] == '1' and len(e) == 3 and t == '1'
    
    def test_append(self, db):
        db.append('a', 'aaa')
        db.append('b', 'bbb')
        db.append('hm', {'c':'c'})
        db.append('lt', ['c', 'd'])
        db.append('st', {6,7})
        assert db.get('a')=='1aaa' and db.get('b')== 'bbbb' and \
            db.get('hm')['c']=='c' and db.get('lt')[4]=='d' and len(db.get('st'))==5
    
    def test_contains(self, db):
        r0 = db.contains('a', 'a')
        r1 = db.contains('b', 'b')
        r2 = db.contains('hm', 'c')
        r3 = db.contains('lt', 'd')
        r4 = db.contains('st', 7)
        assert r0 and r1 and r2 and r3 and r4
    
    def test_setby(self, db):
        db.setby('hm', 'c', 'cc')
        db.setby('lt', 0, 'A')
        assert db.get('hm')['c']=='cc' and db.get('lt')[0]=='A'
    
    def test_getby(self, db):
        r0 = db.getby('hm', 'c')
        r1 = db.getby('lt', 0)
        assert r0=='cc' and r1=='A'
    
    def test_delby(self, db):
        db.delby('hm', ['c', 'b'])
        db.delby('lt', [3, 4])
        db.delby('st', ['6', '7'])
        assert db.getby('hm', 'c') is None and db.getby('lt', 3) is None and len(db.get('st'))==3

    def test_all(self, db):
        arr = db.all()
        trr = []
        def cbf(k):
            trr.append(k)
        db.all_iter(cbfunc=cbf)
        assert len(arr) == len(trr)
