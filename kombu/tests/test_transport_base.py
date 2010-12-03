from kombu.tests.utils import unittest

from kombu.transport.base import Transport


class test_interface(unittest.TestCase):

    def test_establish_connection(self):
        self.assertRaises(NotImplementedError,
                          Transport(None).establish_connection)

    def test_close_connection(self):
        self.assertRaises(NotImplementedError,
                          Transport(None).close_connection, None)

    def test_create_channel(self):
        self.assertRaises(NotImplementedError,
                          Transport(None).create_channel, None)

    def test_close_channel(self):
        self.assertRaises(NotImplementedError,
                          Transport(None).close_channel, None)

    def test_drain_events(self):
        self.assertRaises(NotImplementedError,
                          Transport(None).drain_events, None)
