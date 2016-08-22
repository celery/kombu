from __future__ import absolute_import, unicode_literals

import sys

from case import mock, skip

from kombu import compression


class test_compression:

    @mock.mask_modules('bz2')
    def test_no_bz2(self):
        c = sys.modules.pop('kombu.compression')
        try:
            import kombu.compression
            assert not hasattr(kombu.compression, 'bz2')
        finally:
            if c is not None:
                sys.modules['kombu.compression'] = c

    def test_encoders__gzip(self):
        assert 'application/x-gzip' in compression.encoders()

    @skip.unless_module('bz2')
    def test_encoders__bz2(self):
        assert 'application/x-bz2' in compression.encoders()

    def test_compress__decompress__zlib(self):
        text = b'The Quick Brown Fox Jumps Over The Lazy Dog'
        c, ctype = compression.compress(text, 'zlib')
        assert text != c
        d = compression.decompress(c, ctype)
        assert d == text

    @skip.unless_module('bz2')
    def test_compress__decompress__bzip2(self):
        text = b'The Brown Quick Fox Over The Lazy Dog Jumps'
        c, ctype = compression.compress(text, 'bzip2')
        assert text != c
        d = compression.decompress(c, ctype)
        assert d == text
