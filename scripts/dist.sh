#!/bin/bash
cd .. 
rm -rf ./dist
#.venv/bin/python setup.py sdist bdist_wheel
.venv/bin/python setup.py bdist_wheel
.venv/bin/python -m twine upload dist/*