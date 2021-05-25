import sys

import pytest
from case import mock

from kombu import compression


class test_compression:

    def test_encoders__gzip(self):
        assert 'application/x-gzip' in compression.encoders()

    def test_encoders__bz2(self):
        pytest.importorskip('bz2')
        assert 'application/x-bz2' in compression.encoders()

    def test_encoders__brotli(self):
        pytest.importorskip('brotli')

        assert 'application/x-brotli' in compression.encoders()

    def test_encoders__lzma(self):
        pytest.importorskip('lzma')

        assert 'application/x-lzma' in compression.encoders()

    def test_encoders__zstd(self):
        pytest.importorskip('zstandard')

        assert 'application/zstd' in compression.encoders()

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

    def test_compress__decompress__brotli(self):
        pytest.importorskip('brotli')

        text = b'The Brown Quick Fox Over The Lazy Dog Jumps'
        c, ctype = compression.compress(text, 'brotli')
        assert text != c
        d = compression.decompress(c, ctype)
        assert d == text

    def test_compress__decompress__lzma(self):
        pytest.importorskip('lzma')

        text = b'The Brown Quick Fox Over The Lazy Dog Jumps'
        c, ctype = compression.compress(text, 'lzma')
        assert text != c
        d = compression.decompress(c, ctype)
        assert d == text

    def test_compress__decompress__zstd(self):
        pytest.importorskip('zstandard')

        text = b'The Brown Quick Fox Over The Lazy Dog Jumps'
        c, ctype = compression.compress(text, 'zstd')
        assert text != c
        d = compression.decompress(c, ctype)
        assert d == text

    @mock.mask_modules('bz2')
    def test_no_bz2(self):
        c = sys.modules.pop('kombu.compression')
        try:
            import kombu.compression
            assert not hasattr(kombu.compression, 'bz2')
        finally:
            if c is not None:
                sys.modules['kombu.compression'] = c

    @mock.mask_modules('lzma')
    def test_no_lzma(self):
        c = sys.modules.pop('kombu.compression')
        try:
            import kombu.compression
            assert not hasattr(kombu.compression, 'lzma')
        finally:
            if c is not None:
                sys.modules['kombu.compression'] = c
