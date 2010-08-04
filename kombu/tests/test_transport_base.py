import unittest2 as unittest

from kombu.transport.base import Transport


class test_interface(unittest.TestCase):

    def test_establish_connection(self):
        self.assertRaises(NotImplementedError,
                          Transport(None).establish_connection)

    def test_close_connection(self):
        self.assertRaises(NotImplementedError,
                          Transport(None).close_connection, None)
