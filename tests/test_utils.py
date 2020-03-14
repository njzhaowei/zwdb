# -*- coding: utf-8 -*-
import pytest
import os
import sys
TEST_DIR = os.path.abspath(os.path.dirname(__file__))
PARENT_DIR = os.path.join(TEST_DIR, '..')
sys.path.insert(0, PARENT_DIR)

from zwdb import utils

@pytest.mark.parametrize(
    'db_url, result', (
        ('mysql://tester:test@localhost', True),
        ('mysql://tester:test@localhost/testdb', True),
        ('mysql://tester:test@localhost:3306/testdb?useUnicode=true&characterEncoding=UTF-8', True),
    )
)
def test_db_url_parser(db_url, result):
    rtn = utils.db_url_parser(db_url)
    assert (rtn is not None) == result