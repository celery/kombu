import pytest
import pytz

from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from case import MagicMock, Mock

from kombu.utils.encoding import str_to_bytes
from kombu.utils.json import _DecodeError, dumps, loads


class Custom:

    def __init__(self, data):
        self.data = data

    def __json__(self):
        return self.data


class test_JSONEncoder:

    def test_datetime(self):
        now = datetime.utcnow()
        now_utc = now.replace(tzinfo=pytz.utc)
        stripped = datetime(*now.timetuple()[:3])
        serialized = loads(dumps({
            'datetime': now,
            'tz': now_utc,
            'date': now.date(),
            'time': now.time()},
        ))
        assert serialized == {
            'datetime': now.isoformat(),
            'tz': '{}Z'.format(now_utc.isoformat().split('+', 1)[0]),
            'time': now.time().isoformat(),
            'date': stripped.isoformat(),
        }

    def test_Decimal(self):
        d = Decimal('3314132.13363235235324234123213213214134')
        assert loads(dumps({'d': d})), {'d': str(d)}

    def test_UUID(self):
        id = uuid4()
        assert loads(dumps({'u': id})), {'u': str(id)}

    def test_default(self):
        with pytest.raises(TypeError):
            dumps({'o': object()})


class test_dumps_loads:

    def test_dumps_custom_object(self):
        x = {'foo': Custom({'a': 'b'})}
        assert loads(dumps(x)) == {'foo': x['foo'].__json__()}

    def test_dumps_custom_object_no_json(self):
        x = {'foo': object()}
        with pytest.raises(TypeError):
            dumps(x)

    def test_loads_memoryview(self):
        assert loads(
            memoryview(bytearray(dumps({'x': 'z'}), encoding='utf-8'))
        ) == {'x': 'z'}

    def test_loads_bytearray(self):
        assert loads(
            bytearray(dumps({'x': 'z'}), encoding='utf-8')
        ) == {'x': 'z'}

    def test_loads_bytes(self):
        assert loads(
            str_to_bytes(dumps({'x': 'z'})),
            decode_bytes=True) == {'x': 'z'}

    def test_loads_DecodeError(self):
        _loads = Mock(name='_loads')
        _loads.side_effect = _DecodeError(
            MagicMock(), MagicMock(), MagicMock())
        assert loads(dumps({'x': 'z'}), _loads=_loads) == {'x': 'z'}
