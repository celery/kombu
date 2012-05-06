from __future__ import absolute_import
from __future__ import with_statement

from mock import patch

from kombu import transport

from .utils import TestCase


class test_transport(TestCase):

    def test_resolve_transport__no_class_name(self):
        with self.assertRaises(KeyError):
            transport.resolve_transport("nonexistant")

    def test_resolve_transport_when_callable(self):
        self.assertTupleEqual(transport.resolve_transport(
                lambda: "kombu.transport.memory.Transport"),
                ("kombu.transport.memory", "Transport"))


class test_transport_gettoq(TestCase):

    @patch("warnings.warn")
    def test_compat(self, warn):
        x = transport._ghettoq("Redis", "redis", "redis")

        self.assertEqual(x(), "kombu.transport.redis.Transport")
        self.assertTrue(warn.called)
