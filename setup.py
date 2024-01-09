#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import sys

import setuptools
from setuptools import setup

# -- Parse meta
re_meta = re.compile(r'__(\w+?)__\s*=\s*(.*)')
re_doc = re.compile(r'^"""(.+?)"""')


def add_default(m):
    attr_name, attr_value = m.groups()
    return ((attr_name, attr_value.strip("\"'")),)


def add_doc(m):
    return (('doc', m.groups()[0]),)


pats = {re_meta: add_default, re_doc: add_doc}
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


def fullsplit(path, result=None):
    if result is None:
        result = []
    head, tail = os.path.split(path)
    if head == '':
        return [tail] + result
    if head == path:
        return result
    return fullsplit(head, [tail] + result)


# if os.path.exists('README.rst'):
#    long_description = codecs.open('README.rst', 'r', 'utf-8').read()
# else:
#    long_description = 'See https://pypi.org/project/kombu/'

# -*- Installation Requires -*-
py_version = sys.version_info
is_pypy = hasattr(sys, 'pypy_version_info')


def strip_comments(line):
    return line.split('#', 1)[0].strip()


def reqs(*f):
    with open(os.path.join(os.getcwd(), "requirements", *f)) as reqs_file:
        return [r for r in (strip_comments(line) for line in reqs_file) if r]


def extras(*p):
    return reqs('extras', *p)


def readme():
    with open('README.rst') as f:
        return f.read()


setup(
    name='kombu',
    packages=setuptools.find_packages(exclude=['t', 't.*']),
    version=meta['version'],
    description=meta['doc'],
    keywords='messaging message amqp rabbitmq redis actor producer consumer',
    author=meta['author'],
    author_email=meta['contact'],
    url=meta['homepage'],
    project_urls={
        'Source': 'https://github.com/celery/kombu'
    },
    platforms=['any'],
    license='BSD-3-Clause',
    python_requires=">=3.8",
    install_requires=reqs('default.txt'),
    tests_require=reqs('test.txt'),
    extras_require={
        'msgpack': extras('msgpack.txt'),
        'yaml': extras('yaml.txt'),
        'redis': extras('redis.txt'),
        'mongodb': extras('mongodb.txt'),
        'sqs': extras('sqs.txt'),
        'gcpubsub': extras('gcpubsub.txt'),
        'zookeeper': extras('zookeeper.txt'),
        'sqlalchemy': extras('sqlalchemy.txt'),
        'librabbitmq': extras('librabbitmq.txt'),
        'pyro': extras('pyro.txt'),
        'slmq': extras('slmq.txt'),
        'azurestoragequeues': extras('azurestoragequeues.txt'),
        'azureservicebus': extras('azureservicebus.txt'),
        'qpid': extras('qpid.txt'),
        'consul': extras('consul.txt'),
        'confluentkafka': extras('confluentkafka.txt'),
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Intended Audience :: Developers',
        'Topic :: Communications',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Networking',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
