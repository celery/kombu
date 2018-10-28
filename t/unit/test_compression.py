from __future__ import absolute_import, unicode_literals

from kombu import compression


class test_compression:

    def test_encoders__gzip(self):
        assert 'application/x-gzip' in compression.encoders()

    def test_encoders__bz2(self):
        assert 'application/x-bz2' in compression.encoders()

    def test_compress__decompress__zlib(self):
        text = b'The Quick Brown Fox Jumps Over The Lazy Dog'
        c, ctype = compression.compress(text, 'zlib')
        assert text != c
        d = compression.decompress(c, ctype)
        assert d == text

    def test_compress__decompress__bzip2(self):
        text = b'The Brown Quick Fox Over The Lazy Dog Jumps'
        c, ctype = compression.compress(text, 'bzip2')
        assert text != c
        d = compression.decompress(c, ctype)
        assert d == text
