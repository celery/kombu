from __future__ import absolute_import

import pickle
import sys

from functools import wraps

if sys.version_info >= (3, 0):
    from io import StringIO, BytesIO
else:
    from StringIO import StringIO, StringIO as BytesIO  # noqa

from .. import utils

from .utils import redirect_stdouts, mask_modules, skip_if_module
from .utils import unittest


class OldString(object):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

    def split(self, *args, **kwargs):
        return self.value.split(*args, **kwargs)

    def rsplit(self, *args, **kwargs):
        return self.value.rsplit(*args, **kwargs)


class test_utils(unittest.TestCase):

    def test_maybe_list(self):
        self.assertEqual(utils.maybe_list(None), [])
        self.assertEqual(utils.maybe_list(1), [1])
        self.assertEqual(utils.maybe_list([1, 2, 3]), [1, 2, 3])


class test_UUID(unittest.TestCase):

    def test_uuid4(self):
        self.assertNotEqual(utils.uuid4(),
                            utils.uuid4())

    def test_uuid(self):
        i1 = utils.uuid()
        i2 = utils.uuid()
        self.assertIsInstance(i1, str)
        self.assertNotEqual(i1, i2)

    @skip_if_module('__pypy__')
    def test_uuid_without_ctypes(self):
        old_utils = sys.modules.pop("kombu.utils")

        @mask_modules("ctypes")
        def with_ctypes_masked():
            from ..utils import ctypes, uuid

            self.assertIsNone(ctypes)
            tid = uuid()
            self.assertTrue(tid)
            self.assertIsInstance(tid, basestring)

        try:
            with_ctypes_masked()
        finally:
            sys.modules["celery.utils"] = old_utils


class test_Misc(unittest.TestCase):

    def test_kwdict(self):

        def f(**kwargs):
            return kwargs

        kw = {u"foo": "foo",
              u"bar": "bar"}
        self.assertTrue(f(**utils.kwdict(kw)))


class MyStringIO(StringIO):

    def close(self):
        pass


class MyBytesIO(BytesIO):

    def close(self):
        pass


class test_emergency_dump_state(unittest.TestCase):

    @redirect_stdouts
    def test_dump(self, stdout, stderr):
        fh = MyBytesIO()

        utils.emergency_dump_state({"foo": "bar"}, open_file=lambda n, m: fh)
        self.assertDictEqual(pickle.loads(fh.getvalue()), {"foo": "bar"})
        self.assertTrue(stderr.getvalue())
        self.assertFalse(stdout.getvalue())

    @redirect_stdouts
    def test_dump_second_strategy(self, stdout, stderr):
        fh = MyStringIO()

        def raise_something(*args, **kwargs):
            raise KeyError("foo")

        utils.emergency_dump_state({"foo": "bar"}, open_file=lambda n, m: fh,
                                                   dump=raise_something)
        self.assertIn("'foo': 'bar'", fh.getvalue())
        self.assertTrue(stderr.getvalue())
        self.assertFalse(stdout.getvalue())


def insomnia(fun):

    @wraps(fun)
    def _inner(*args, **kwargs):
        def mysleep(i):
            pass

        prev_sleep = utils.sleep
        utils.sleep = mysleep
        try:
            return fun(*args, **kwargs)
        finally:
            utils.sleep = prev_sleep

    return _inner


class test_retry_over_time(unittest.TestCase):

    def setUp(self):
        self.index = 0

    class Predicate(Exception):
        pass

    def myfun(self):
        if self.index < 9:
            raise self.Predicate()
        return 42

    def errback(self, exc, interval):
        sleepvals = (None, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 16.0)
        self.index += 1
        self.assertEqual(interval, sleepvals[self.index])

    @insomnia
    def test_simple(self):
        x = utils.retry_over_time(self.myfun, self.Predicate,
                errback=self.errback, interval_max=14)
        self.assertEqual(x, 42)
        self.assertEqual(self.index, 9)

    @insomnia
    def test_retry_once(self):
        self.assertRaises(self.Predicate, utils.retry_over_time,
                self.myfun, self.Predicate,
                max_retries=1, errback=self.errback, interval_max=14)
        self.assertEqual(self.index, 2)

    @insomnia
    def test_retry_never(self):
        self.assertRaises(self.Predicate, utils.retry_over_time,
                self.myfun, self.Predicate,
                max_retries=0, errback=self.errback, interval_max=14)
        self.assertEqual(self.index, 1)
