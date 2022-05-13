# -*- coding: utf-8 -*-
# pylint: disable=redefined-outer-name

import pytest
import datetime

from zwdb.zwelastic import ZWElastic

DB_URL = 'es://elastic:111111@yew.com:9200/'
INDEX_NM = 'test-idxa'
INDICES = {
    'test-idxa': {
        'settings':{
            'refresh_interval': '1s',
            'number_of_shards': 1,
            'number_of_replicas': 1,
            'analysis': {
                'analyzer':{
                    'ik_analyzer': {
                        'tokenizer':'ik_smart'
                    },
                    'pinyin_analyzer': {
                        'tokenizer':'my_pinyin'
                    }
                },
                'tokenizer': {
                    'my_pinyin': {
                        'type' : 'pinyin',
                        'keep_first_letter': True,              # 刘德华>ldh, default: true
                        'keep_separate_first_letter': False,    # 刘德华>l,d,h, default: false
                        'limit_first_letter_length': 16,        # max length of the first_letter result, default: 16
                        'keep_full_pinyin': True,               # 刘德华> [liu,de,hua], default: true
                        'keep_joined_full_pinyin': False,       # 刘德华> [liudehua], default: false
                        'keep_none_chinese': True,              # keep non chinese letter or number in result, default: true
                        'keep_none_chinese_together': True,     # True: DJ音乐家 -> DJ,yin,yue,jia False: DJ音乐家 -> D,J,yin,yue,jia
                        'keep_none_chinese_in_first_letter': True, # 刘德华AT2016->ldhat2016, default: true
                        'keep_none_chinese_in_joined_full_pinyin': False, # 刘德华2016->liudehua2016, default: false
                        'none_chinese_pinyin_tokenize': True,   # liudehuaalibaba13zhuanghan -> liu,de,hua,a,li,ba,ba,13,zhuang,han, default: true
                        'keep_original': False,                 # keep original input as well, default: false
                        'lowercase': True,                      # lowercase non Chinese letters, default: true
                        'trim_whitespace': True,                # default: true
                        'remove_duplicated_term': False,        # de的>de, default: false
                        'ignore_pinyin_offset': True,           # if you need offset, please set it to false. default: true
                    }
                }
            },
        },

        'mappings': {
            'properties': {
                'id': {
                    'type': 'integer',
                    'index': False,
                },
                'title': {
                    'type': 'text',
                    'analyzer': 'ik_analyzer',
                },
                'tags': {
                    'type': 'text',
                    'analyzer': 'keyword',
                    'fields': {
                        'pinyin': {
                            'type': 'text',
                            'analyzer': 'pinyin_analyzer',
                        }
                    }
                },
                'num': {
                    'type': 'integer',
                    'index': False,
                },
                'none': {
                    'type': 'object',
                },
                'receive_time': {
                    'type': 'date',
                    'format': "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
                }
            }
        }
    }
}

RECS_INIT = [
    {'id': 1, 'title': '测试照地球南博万。。。', 'tags':['南京', '北京', '上海', '深圳'], 'num': 1, 'none': None, 'receive_time': datetime.datetime.now()},
]

@pytest.fixture(scope='module')
def db():
    with ZWElastic(DB_URL) as mydb:
        for k, v in INDICES.items():
            mydb.delete(index=k)
            mydb.create_index(k, v['settings'], v['mappings'])
        mydb.insert(INDEX_NM, RECS_INIT, refresh=True)
        yield mydb
        for k, v in INDICES.items():
            mydb.delete(index=k)

def test_version(db):
    s = db.version
    print(s)
    assert len(s.split('.')) == 3

def test_info(db):
    o = db.info
    print(o)
    assert len(o['cluster_name'])>0

def test_count(db):
    assert db.count(INDEX_NM) == len(RECS_INIT)

def test_exists(db):
    assert db.exists(INDEX_NM)
    assert not db.exists('NOT_EXIST_INDEX')
    assert db.exists(INDEX_NM, docid=1)
    assert not db.exists(INDEX_NM, docid=2)

def test_findone(db):
    ri = db.findone(INDEX_NM)
    rd = db.findone(INDEX_NM, docid=1)
    assert ri['settings'] and rd['id'] == 1
    ri = db.findone('NOT_EXIST_INDEX')
    rd = db.findone(INDEX_NM, docid=2)
    assert (not ri) and (not rd)

def test_insert(db):
    r = db.insert(INDEX_NM, {'id': 2, 'title': '测试照地球南博万。。。', 'tags':['南京', '北京'], 'num': 1, 'none': None, 'receive_time': datetime.datetime.now()-datetime.timedelta(days=1)})
    assert r
    r = db.insert(INDEX_NM, {'id': 2})
    assert not r

def test_update(db):
    title = '测试照地球南博万。。刘德华'
    r = db.update(INDEX_NM, docs={'id': 2, 'title': title}, refresh=True)
    o = db.findone(INDEX_NM, docid=2)
    assert r and o['title'] == title
    r = db.update(INDEX_NM, docs={'id': 999, 'title': title})
    assert not r

    script = {
        'source': 'ctx._source.num += params.num',
        'lang': 'painless',
        'params' : {
            'num' : 2
        }
    }
    r = db.update(INDEX_NM, docids=2, script=script, refresh=True)
    o = db.findone(INDEX_NM, docid=2)
    assert r and o['num'] == 1+2

def test_upsert(db):
    title = '测试照地球南博万。。刘德华。。林志玲'
    r = db.upsert(INDEX_NM, docs={'id': 2, 'title': title}, refresh=True)
    o = db.findone(INDEX_NM, docid=2)
    assert r and o['title'] == title
    r = db.upsert(INDEX_NM, docs={'id': 3, 'title': title, 'tags':['南京'], 'num': 1}, refresh=True)
    o = db.findone(INDEX_NM, docid=3)
    assert r and o['title'] == title

def test_find(db):
    # 分页
    r = db.find(INDEX_NM, query={
        'match': {
            'num': 1
        }
    }, sort=[{
        'receive_time': {
            'order': 'desc'
        },
        'id': {
            'order': 'desc'
        },
        '_score': {
            'order': 'desc'
        }
    }], from_=0, size=1)
    rec = r['docs'][0]
    assert len(r['docs']) == 1 and rec['id'] == 1

    r = db.find(INDEX_NM, query={
        'match': {
            'num': 1
        }
    }, sort=[{
        'receive_time': {
            'order': 'desc'
        },
        'id': {
            'order': 'desc'
        },
        '_score': {
            'order': 'desc'
        }
    }], from_=0, size=1, search_after=r['last'])
    rec = r['docs'][0]
    assert len(r['docs']) == 1 and rec['id'] == 3

    # 分词
    r = db.find(INDEX_NM, query={
        'match': {
            'title': '照地球'
        }
    })
    assert r['total'] == 0

    r = db.find(INDEX_NM, query={
        'match': {
            'title': '照地球南博万'
        }
    })
    assert r['total'] == 3

    # 拼音
    r = db.find(INDEX_NM, query={
        'match': {
            'tags.pinyin': 'shanghai'
        }
    })
    assert r['total'] == 1
