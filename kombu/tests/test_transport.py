from __future__ import absolute_import

from .. import transport

from .utils import unittest


class test_transport(unittest.TestCase):

    def test_resolve_transport__no_class_name(self):
        self.assertRaises(KeyError, transport.resolve_transport,
                          "nonexistant")
