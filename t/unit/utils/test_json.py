from __future__ import annotations

import sys
import uuid
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from kombu.utils.encoding import str_to_bytes
from kombu.utils.json import (_register_default_types, dumps, loads,
                              register_type)

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


class Custom:
    def __init__(self, data):
        self.data = data

    def __json__(self):
        return self.data


class test_JSONEncoder:
    @pytest.fixture(autouse=True)
    def reset_registered_types(self):
        _register_default_types()

    @pytest.mark.freeze_time("2015-10-21")
    def test_datetime(self):
        now = datetime.utcnow()
        now_utc = now.replace(tzinfo=ZoneInfo("UTC"))

        original = {
            'datetime': now,
            'tz': now_utc,
            'date': now.date(),
            'time': now.time(),
        }

        serialized = loads(dumps(original))

        assert serialized == original

    @given(message=st.binary())
    @settings(print_blob=True)
    def test_binary(self, message):
        serialized = loads(dumps({
            'args': (message,),
        }))
        assert serialized == {
            'args': [message],
        }

    def test_Decimal(self):
        original = {'d': Decimal('3314132.13363235235324234123213213214134')}
        serialized = loads(dumps(original))

        assert serialized == original

    def test_namedtuple(self):
        Foo = namedtuple('Foo', ['bar'])
        assert loads(dumps(Foo(123))) == [123]

    def test_UUID(self):
        constructors = [
            uuid.uuid1,
            lambda: uuid.uuid3(uuid.NAMESPACE_URL, "https://example.org"),
            uuid.uuid4,
            lambda: uuid.uuid5(uuid.NAMESPACE_URL, "https://example.org"),
            # The uuids below correspond to v6, v7 and v8 respectively and were
            # generated using the package uuid6.
            lambda: uuid.UUID("1ee0b1e6-dd55-63d2-867f-88cb9205458f"),
            lambda: uuid.UUID("0188bcbb-8475-7605-a094-fe41c58df798"),
            lambda: uuid.UUID("0188bcbb-8cb2-8bf7-b3b5-fd1faa0431bd"),
        ]
        for constructor in constructors:
            id = constructor()
            loaded_value = loads(dumps({'u': id}))
            assert loaded_value == {'u': id}
            assert loaded_value["u"].version == id.version

    def test_register_type_overrides_defaults(self):
        # This type is already registered by default, let's override it
        register_type(uuid.UUID, "uuid", lambda o: "custom", lambda o: o)
        value = uuid.uuid4()
        loaded_value = loads(dumps({'u': value}))
        assert loaded_value == {'u': "custom"}

    def test_register_type_with_new_type(self):
        # Guaranteed never before seen type
        @dataclass()
        class SomeType:
            a: int

        register_type(SomeType, "some_type", lambda o: "custom", lambda o: o)
        value = SomeType(42)
        loaded_value = loads(dumps({'u': value}))
        assert loaded_value == {'u': "custom"}

    def test_register_type_with_empty_marker(self):
        register_type(
            datetime,
            None,
            lambda o: o.isoformat(),
            lambda o: "should never be used"
        )
        now = datetime.utcnow()
        serialized_str = dumps({'now': now})
        deserialized_value = loads(serialized_str)

        assert "__type__" not in serialized_str
        assert "__value__" not in serialized_str

        # Check that there is no extra deserialization happening
        assert deserialized_value == {'now': now.isoformat()}

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
