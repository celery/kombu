#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import codecs

extra = {}
tests_require = {"nose", "nose-cover3"}
if sys.version_info >= (3, 0):
    extra.update(use_2to3=True)
elif sys.version_info <= (2, 6):
    tests_require.append("unittest2")
elif sys.version_info <= (2, 5):
    tests_require.append("simplejson")


if sys.version_info < (2, 4):
    raise Exception("Kombu requires Python 2.4 or higher.")

try:
    from setuptools import setup, Extension, Feature, find_packages
except ImportError:
    from distutils.core import setup, Extension, find_packages
    Feature = None

from distutils.command.install_data import install_data
from distutils.command.install import INSTALL_SCHEMES

os.environ["KOMBU_NO_EVAL"] = "yes"
import kombu
os.environ.pop("KOMBU_NO_EVAL", None)
sys.modules.pop("kombu", None)

packages, data_files = [], []
root_dir = os.path.dirname(__file__)
if root_dir != '':
    os.chdir(root_dir)
src_dir = "kombu"


def osx_install_data(install_data):

    def finalize_options(self):
        self.set_undefined_options("install", ("install_lib", "install_dir"))
        install_data.finalize_options(self)


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
        if dirname.startswith("."):
            del dirnames[i]
    for filename in filenames:
        if filename.endswith(".py"):
            packages.append('.'.join(fullsplit(dirpath)))
        else:
            data_files.append([dirpath, [os.path.join(dirpath, f) for f in
                filenames]])

if os.path.exists("README.rst"):
    long_description = codecs.open('README.rst', "r", "utf-8").read()
else:
    long_description = "See http://pypi.python.org/pypi/kombu"

setup(
    name='kombu',
    version=kombu.__version__,
    description=kombu.__doc__,
    author=kombu.__author__,
    author_email=kombu.__contact__,
    url=kombu.__homepage__,
    platforms=["any"],
    packages=packages,
    data_files=data_files,
    zip_safe=False,
    test_suite="nose.collector",
    install_requires=[
        'anyjson',
        'amqplib>=0.6',
    ],
    tests_require=tests_require,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Framework :: Django",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.5",
        "Programming Language :: Python :: 2.4",
        "Programming Language :: Python :: 2",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
        "Topic :: Communications",
        "Topic :: System :: Distributed Computing",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    long_description=long_description,
    **extra
)
