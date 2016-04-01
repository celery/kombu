from __future__ import absolute_import, unicode_literals

from kombu.utils.encoding import str_to_bytes
from kombu.utils.json import _DecodeError, dumps, loads

from kombu.tests.case import Case, MagicMock, Mock, case_no_python3


class Custom(object):

    def __init__(self, data):
        self.data = data

    def __json__(self):
        return self.data


class test_dumps_loads(Case):

    def test_dumps_custom_object(self):
        x = {'foo': Custom({'a': 'b'})}
        self.assertEqual(loads(dumps(x)), {'foo': x['foo'].__json__()})

    def test_dumps_custom_object_no_json(self):
        x = {'foo': object()}
        with self.assertRaises(TypeError):
            dumps(x)

    def test_loads_memoryview(self):
        self.assertEqual(
            loads(memoryview(bytearray(dumps({'x': 'z'}), encoding='utf-8'))),
            {'x': 'z'},
        )

    def test_loads_bytearray(self):
        self.assertEqual(
            loads(bytearray(dumps({'x': 'z'}), encoding='utf-8')),
            {'x': 'z'})

    def test_loads_bytes(self):
        self.assertEqual(
            loads(str_to_bytes(dumps({'x': 'z'})), decode_bytes=True),
            {'x': 'z'},
        )

    @case_no_python3
    def test_loads_buffer(self):
        self.assertEqual(loads(buffer(dumps({'x': 'z'}))), {'x': 'z'})

    def test_loads_DecodeError(self):
        _loads = Mock(name='_loads')
        _loads.side_effect = _DecodeError(
            MagicMock(), MagicMock(), MagicMock())
        self.assertEqual(loads(dumps({'x': 'z'}), _loads=_loads), {'x': 'z'})
