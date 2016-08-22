from __future__ import absolute_import, unicode_literals

import pytest
import sys

from case import Mock, patch

from kombu.message import Message


class test_Message:

    def test_repr(self):
        assert repr(Message(Mock(), 'b'))

    def test_decode(self):
        m = Message(Mock(), 'body')
        decode = m._decode = Mock()
        assert m._decoded_cache is None
        assert m.decode() is m._decode.return_value
        assert m._decoded_cache is m._decode.return_value
        m._decode.assert_called_with()
        m._decode = Mock()
        assert m.decode() is decode.return_value

    def test_reraise_error(self):
        m = Message(Mock(), 'body')
        callback = Mock(name='callback')
        try:
            raise KeyError('foo')
        except KeyError:
            m.errors.append(sys.exc_info())
        m._reraise_error(callback)
        callback.assert_called()

        with pytest.raises(KeyError):
            m._reraise_error(None)

    @patch('kombu.message.decompress')
    def test_decompression_stores_error(self, decompress):
        decompress.side_effect = RuntimeError()
        m = Message(Mock(), 'body', headers={'compression': 'zlib'})
        with pytest.raises(RuntimeError):
            m._reraise_error(None)
