#!/usr/bin/env python
import os
from setuptools import setup, find_packages
from codecs import open

pkg_name = 'zwdb'

here = os.path.abspath(os.path.dirname(__file__))
packages = find_packages()
requires = [s.strip() for s in open('requirements.txt').readlines()]
test_requirements = [s.strip() for s in open('requirements_dev.txt').readlines()][4:]

about = {}
lines = []
with open(os.path.join(here, pkg_name, '__version__.py'), 'r', 'utf-8') as f:
    exec(f.read(), about)
    # auto update min version number for every dist upload
    verarr = about['__version__'].split('.')
    verarr[2] = str(int(verarr[2])+1)
    about['__version__'] = '.'.join(verarr)
    f.seek(0)
    lines = f.readlines()
    lines[0] = "__version__ = '%s'\n"%about['__version__']

with open(os.path.join(here, pkg_name, '__version__.py'), 'w', 'utf-8') as f:
    f.writelines(lines)

with open('README.md', 'r') as f:
    readme = f.read()

setup(
    name=about['__title__'],
    version=about['__version__'],
    description=about['__description__'],
    long_description=readme,
    long_description_content_type='text/markdown',
    author=about['__author__'],
    author_email=about['__author_email__'],
    url=about['__url__'],
    license=about['__license__'],
    packages=packages,
    package_data={'': ['LICENSE', 'NOTICE']},
    package_dir={pkg_name:pkg_name},
    include_package_data=True,
    install_requires=requires,
    tests_require=test_requirements,
    python_requires='>=3.6',
    platforms=["all"],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
)