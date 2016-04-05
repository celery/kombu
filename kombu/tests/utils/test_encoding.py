# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import sys

from contextlib import contextmanager

from kombu.five import bytes_t, string_t
from kombu.utils.encoding import (
    get_default_encoding_file, safe_str,
    set_default_encoding_file, default_encoding,
)

from kombu.tests.case import Case, patch, skip


@contextmanager
def clean_encoding():
    old_encoding = sys.modules.pop('kombu.utils.encoding', None)
    import kombu.utils.encoding
    try:
        yield kombu.utils.encoding
    finally:
        if old_encoding:
            sys.modules['kombu.utils.encoding'] = old_encoding


class test_default_encoding(Case):

    def test_set_default_file(self):
        prev = get_default_encoding_file()
        try:
            set_default_encoding_file('/foo.txt')
            self.assertEqual(get_default_encoding_file(), '/foo.txt')
        finally:
            set_default_encoding_file(prev)

    @patch('sys.getfilesystemencoding')
    def test_default(self, getdefaultencoding):
        getdefaultencoding.return_value = 'ascii'
        with clean_encoding() as encoding:
            enc = encoding.default_encoding()
            if sys.platform.startswith('java'):
                self.assertEqual(enc, 'utf-8')
            else:
                self.assertEqual(enc, 'ascii')
                getdefaultencoding.assert_called_with()


@skip.if_python3
class test_encoding_utils(Case):

    def test_str_to_bytes(self):
        with clean_encoding() as e:
            self.assertIsInstance(e.str_to_bytes('foobar'), bytes_t)

    def test_from_utf8(self):
        with clean_encoding() as e:
            self.assertIsInstance(e.from_utf8('foobar'), bytes_t)

    def test_default_encode(self):
        with clean_encoding() as e:
            self.assertTrue(e.default_encode(b'foo'))


class test_safe_str(Case):

    def setup(self):
        self._cencoding = patch('sys.getfilesystemencoding')
        self._encoding = self._cencoding.__enter__()
        self._encoding.return_value = 'ascii'

    def teardown(self):
        self._cencoding.__exit__()

    def test_when_bytes(self):
        self.assertEqual(safe_str('foo'), 'foo')

    def test_when_unicode(self):
        self.assertIsInstance(safe_str('foo'), string_t)

    def test_when_encoding_utf8(self):
        with patch('sys.getfilesystemencoding') as encoding:
            encoding.return_value = 'utf-8'
            self.assertEqual(default_encoding(), 'utf-8')
            s = 'The quiæk fåx jømps øver the lazy dåg'
            res = safe_str(s)
            self.assertIsInstance(res, str)

    def test_when_containing_high_chars(self):
        with patch('sys.getfilesystemencoding') as encoding:
            encoding.return_value = 'ascii'
            s = 'The quiæk fåx jømps øver the lazy dåg'
            res = safe_str(s)
            self.assertIsInstance(res, str)
            self.assertEqual(len(s), len(res))

    def test_when_not_string(self):
        o = object()
        self.assertEqual(safe_str(o), repr(o))

    def test_when_unrepresentable(self):

        class O(object):

            def __repr__(self):
                raise KeyError('foo')

        self.assertIn('<Unrepresentable', safe_str(O()))
