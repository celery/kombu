from __future__ import absolute_import, unicode_literals

import sys

from kombu.message import Message

from .case import Case, Mock, patch


class test_Message(Case):

    def test_repr(self):
        self.assertTrue(repr(Message(Mock(), 'b')))

    def test_decode(self):
        m = Message(Mock(), 'body')
        decode = m._decode = Mock()
        self.assertIsNone(m._decoded_cache)
        self.assertIs(m.decode(), m._decode.return_value)
        self.assertIs(m._decoded_cache, m._decode.return_value)
        m._decode.assert_called_with()
        m._decode = Mock()
        self.assertIs(m.decode(), decode.return_value)

    def test_reraise_error(self):
        m = Message(Mock(), 'body')
        callback = Mock(name='callback')
        try:
            raise KeyError('foo')
        except KeyError:
            m.errors.append(sys.exc_info())
        m._reraise_error(callback)
        callback.assert_called()

        with self.assertRaises(KeyError):
            m._reraise_error(None)

    @patch('kombu.message.decompress')
    def test_decompression_stores_error(self, decompress):
        decompress.side_effect = RuntimeError()
        m = Message(Mock(), 'body', headers={'compression': 'zlib'})
        with self.assertRaises(RuntimeError):
            m._reraise_error(None)
