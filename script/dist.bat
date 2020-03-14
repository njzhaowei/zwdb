cd .. 
.venv\Scripts\python setup.py sdist bdist_wheel 
.venv\Scripts\python -m twine upload dist/*
@echo off
pause