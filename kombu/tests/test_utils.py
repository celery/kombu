import unittest2 as unittest

from kombu import utils

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
