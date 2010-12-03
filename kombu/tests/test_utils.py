import pickle
import sys
from kombu.tests.utils import unittest

if sys.version_info >= (3, 0):
    from io import StringIO, BytesIO
else:
    from StringIO import StringIO, StringIO as BytesIO

from kombu import utils
from kombu.utils.functional import wraps

from kombu.tests.utils import redirect_stdouts, mask_modules

partition = utils._compat_partition
rpartition = utils._compat_rpartition


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

    def assert_partition(self, p, t=str):
        self.assertEqual(p(t("foo.bar.baz"), "."),
                ("foo", ".", "bar.baz"))
        self.assertEqual(p(t("foo"), "."),
                ("foo", "", ""))
        self.assertEqual(p(t("foo."), "."),
                ("foo", ".", ""))
        self.assertEqual(p(t(".bar"), "."),
                ("", ".", "bar"))
        self.assertEqual(p(t("."), "."),
                ('', ".", ''))

    def assert_rpartition(self, p, t=str):
        self.assertEqual(p(t("foo.bar.baz"), "."),
                ("foo.bar", ".", "baz"))
        self.assertEqual(p(t("foo"), "."),
                ("", "", "foo"))
        self.assertEqual(p(t("foo."), "."),
                ("foo", ".", ""))
        self.assertEqual(p(t(".bar"), "."),
                ("", ".", "bar"))
        self.assertEqual(p(t("."), "."),
                ('', ".", ''))

    def test_compat_partition(self):
        self.assert_partition(partition)

    def test_compat_rpartition(self):
        self.assert_rpartition(rpartition)

    def test_partition(self):
        self.assert_partition(utils.partition)

    def test_rpartition(self):
        self.assert_rpartition(utils.rpartition)

    def test_partition_oldstr(self):
        self.assert_partition(utils.partition, OldString)

    def test_rpartition_oldstr(self):
        self.assert_rpartition(utils.rpartition, OldString)


class test_UUID(unittest.TestCase):

    def test_uuid4(self):
        self.assertNotEqual(utils.uuid4(),
                            utils.uuid4())

    def test_gen_unique_id(self):
        i1 = utils.gen_unique_id()
        i2 = utils.gen_unique_id()
        self.assertIsInstance(i1, str)
        self.assertNotEqual(i1, i2)

    def test_gen_unique_id_without_ctypes(self):
        old_utils = sys.modules.pop("kombu.utils")

        @mask_modules("ctypes")
        def with_ctypes_masked():
            from kombu.utils import ctypes, gen_unique_id

            self.assertIsNone(ctypes)
            uuid = gen_unique_id()
            self.assertTrue(uuid)
            self.assertIsInstance(uuid, basestring)

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

    def test_repeatlast(self):
        x = [1, 2, 3, 4]
        it = utils.repeatlast(x)
        self.assertEqual(it.next(), 1)
        self.assertEqual(it.next(), 2)
        self.assertEqual(it.next(), 3)
        self.assertEqual(it.next(), 4)
        self.assertEqual(it.next(), 4)


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


_tried_to_sleep = [None]


def insomnia(fun):

    @wraps(fun)
    def _inner(*args, **kwargs):
        _tried_to_sleep[0] = None

        def mysleep(i):
            _tried_to_sleep[0] = i

        prev_sleep = utils.sleep
        utils.sleep = mysleep
        try:
            return fun(*args, **kwargs)
        finally:
            utils.sleep = prev_sleep

    return _inner


class test_retry_over_Time(unittest.TestCase):

    @insomnia
    def test_simple(self):
        index = [0]

        class Predicate(Exception):
            pass

        def myfun():
            sleepvals = {0: None,
                         1: 2,
                         2: 4,
                         3: 6,
                         4: 8,
                         5: 10,
                         6: 12,
                         7: 14,
                         8: 14,
                         9: 14}
            self.assertEqual(_tried_to_sleep[0], sleepvals[index[0]])
            if index[0] < 9:
                raise Predicate()
            return 42

        def errback(exc, interval):
            index[0] += 1

        x = utils.retry_over_time(myfun, Predicate, errback=errback,
                                                    interval_max=14)
        self.assertEqual(x, 42)
        _tried_to_sleep[0] = None
        index[0] = 0
        self.assertRaises(Predicate,
                          utils.retry_over_time, myfun, Predicate,
                          max_retries=1, errback=errback, interval_max=14)
