#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import codecs

extra = {}
PY3 = sys.version_info[0] == 3

if sys.version_info < (2, 6):
    raise Exception('Kombu requires Python 2.6 or higher.')

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup  # noqa

from distutils.command.install import INSTALL_SCHEMES

# -- Parse meta
import re
re_meta = re.compile(r'__(\w+?)__\s*=\s*(.*)')
re_vers = re.compile(r'VERSION\s*=.*?\((.*?)\)')
re_doc = re.compile(r'^"""(.+?)"""')
rq = lambda s: s.strip("\"'")


def add_default(m):
    attr_name, attr_value = m.groups()
    return ((attr_name, rq(attr_value)), )


def add_version(m):
    v = list(map(rq, m.groups()[0].split(', ')))
    return (('VERSION', '.'.join(v[0:3]) + ''.join(v[3:])), )


def add_doc(m):
    return (('doc', m.groups()[0]), )

pats = {re_meta: add_default,
        re_vers: add_version,
        re_doc: add_doc}
here = os.path.abspath(os.path.dirname(__file__))
meta_fh = open(os.path.join(here, 'kombu/__init__.py'))
try:
    meta = {}
    for line in meta_fh:
        if line.strip() == '# -eof meta-':
            break
        for pattern, handler in pats.items():
            m = pattern.match(line.strip())
            if m:
                meta.update(handler(m))
finally:
    meta_fh.close()
# --

packages, data_files = [], []
root_dir = os.path.dirname(__file__)
if root_dir != '':
    os.chdir(root_dir)
src_dir = 'kombu'


def fullsplit(path, result=None):
    if result is None:
        result = []
    head, tail = os.path.split(path)
    if head == '':
        return [tail] + result
    if head == path:
        return result
    return fullsplit(head, [tail] + result)


for scheme in list(INSTALL_SCHEMES.values()):
    scheme['data'] = scheme['purelib']

for dirpath, dirnames, filenames in os.walk(src_dir):
    # Ignore dirnames that start with '.'
    for i, dirname in enumerate(dirnames):
        if dirname.startswith('.'):
            del dirnames[i]
    for filename in filenames:
        if filename.endswith('.py'):
            packages.append('.'.join(fullsplit(dirpath)))
        else:
            data_files.append(
                [dirpath, [os.path.join(dirpath, f) for f in filenames]],
            )

if os.path.exists('README.rst'):
    long_description = codecs.open('README.rst', 'r', 'utf-8').read()
else:
    long_description = 'See http://pypi.python.org/pypi/kombu'

# -*- Installation Requires -*-
py_version = sys.version_info
is_jython = sys.platform.startswith('java')
is_pypy = hasattr(sys, 'pypy_version_info')


def strip_comments(l):
    return l.split('#', 1)[0].strip()


def reqs(*f):
    return [
        r for r in (
            strip_comments(l) for l in open(
                os.path.join(os.getcwd(), 'requirements', *f)).readlines()
        ) if r]

install_requires = reqs('default.txt')
if py_version[0:2] == (2, 6):
    install_requires.extend(reqs('py26.txt'))

# -*- Tests Requires -*-

tests_require = reqs('test3.txt' if PY3 else 'test.txt')

extras = lambda *p: reqs('extras', *p)
extras_require = extra['extras_require'] = {
    'msgpack': extras('msgpack.txt'),
    'yaml': extras('yaml.txt'),
    'redis': extras('redis.txt'),
    'mongodb': extras('mongodb.txt'),
    'sqs': extras('sqs.txt'),
    'couchdb': extras('couchdb.txt'),
    'beanstalk': extras('beanstalk.txt'),
    'zookeeper': extras('zookeeper.txt'),
    'zeromq': extras('zeromq.txt'),
    'sqlalchemy': extras('sqlalchemy.txt'),
    'librabbitmq': extras('librabbitmq.txt'),
    'pyro': extras('pyro.txt'),
    'slmq': extras('slmq.txt'),
}

setup(
    name='kombu',
    version=meta['VERSION'],
    description=meta['doc'],
    author=meta['author'],
    author_email=meta['contact'],
    url=meta['homepage'],
    platforms=['any'],
    packages=packages,
    data_files=data_files,
    zip_safe=False,
    test_suite='nose.collector',
    install_requires=install_requires,
    tests_require=tests_require,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: Implementation :: Jython',
        'Intended Audience :: Developers',
        'Topic :: Communications',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Networking',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    long_description=long_description,
    **extra)
