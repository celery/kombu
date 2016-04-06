from __future__ import absolute_import, unicode_literals

import sys

from kombu import compression

from .case import Case, mock, skip


class test_compression(Case):

    @mock.mask_modules('bz2')
    def test_no_bz2(self):
        c = sys.modules.pop('kombu.compression')
        try:
            import kombu.compression
            self.assertFalse(hasattr(kombu.compression, 'bz2'))
        finally:
            if c is not None:
                sys.modules['kombu.compression'] = c

    def test_encoders__gzip(self):
        self.assertIn('application/x-gzip', compression.encoders())

    @skip.unless_module('bz2')
    def test_encoders__bz2(self):
        self.assertIn('application/x-bz2', compression.encoders())

    def test_compress__decompress__zlib(self):
        text = b'The Quick Brown Fox Jumps Over The Lazy Dog'
        c, ctype = compression.compress(text, 'zlib')
        self.assertNotEqual(text, c)
        d = compression.decompress(c, ctype)
        self.assertEqual(d, text)

    @skip.unless_module('bz2')
    def test_compress__decompress__bzip2(self):
        text = b'The Brown Quick Fox Over The Lazy Dog Jumps'
        c, ctype = compression.compress(text, 'bzip2')
        self.assertNotEqual(text, c)
        d = compression.decompress(c, ctype)
        self.assertEqual(d, text)
