import unittest2 as unittest

from kombu.backends.base import BaseBackend


class test_interface(unittest.TestCase):

    def test_get_channel(self):
        self.assertRaises(NotImplementedError,
                          BaseBackend(None).get_channel)

    def test_establish_connection(self):
        self.assertRaises(NotImplementedError,
                          BaseBackend(None).establish_connection)

    def test_close_connection(self):
        self.assertRaises(NotImplementedError,
                          BaseBackend(None).close_connection)
