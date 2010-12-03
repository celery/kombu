from kombu.tests.utils import unittest

from kombu import compression


class test_compression(unittest.TestCase):

    def test_encoders(self):
        encoders = compression.encoders()
        self.assertIn("application/x-gzip", encoders)
        self.assertIn("application/x-bz2", encoders)

    def test_compress__decompress__zlib(self):
        text = "The Quick Brown Fox Jumps Over The Lazy Dog"
        c, ctype = compression.compress(text, "zlib")
        self.assertNotEqual(text, c)
        d = compression.decompress(c, ctype)
        self.assertEqual(d, text)

    def test_compress__decompress__bzip2(self):
        text = "The Brown Quick Fox Over The Lazy Dog Jumps"
        c, ctype = compression.compress(text, "bzip2")
        self.assertNotEqual(text, c)
        d = compression.decompress(c, ctype)
        self.assertEqual(d, text)
