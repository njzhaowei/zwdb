cd .. && \
.venv/bin/python setup.py sdist bdist_wheel && \
.venv/bin/python -m twine upload dist/*