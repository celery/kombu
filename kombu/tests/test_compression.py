from __future__ import absolute_import

from nose import SkipTest

from .. import compression
from .utils import unittest


class test_compression(unittest.TestCase):

    def setUp(self):
        try:
            import bz2  # noqa
        except ImportError:
            self.has_bzip2 = False
        else:
            self.has_bzip2 = True

    def test_encoders(self):
        encoders = compression.encoders()
        self.assertIn("application/x-gzip", encoders)
        if self.has_bzip2:
            self.assertIn("application/x-bz2", encoders)

    def test_compress__decompress__zlib(self):
        text = "The Quick Brown Fox Jumps Over The Lazy Dog"
        c, ctype = compression.compress(text, "zlib")
        self.assertNotEqual(text, c)
        d = compression.decompress(c, ctype)
        self.assertEqual(d, text)

    def test_compress__decompress__bzip2(self):
        if not self.has_bzip2:
            raise SkipTest("bzip2 not available")
        text = "The Brown Quick Fox Over The Lazy Dog Jumps"
        c, ctype = compression.compress(text, "bzip2")
        self.assertNotEqual(text, c)
        d = compression.decompress(c, ctype)
        self.assertEqual(d, text)
