from __future__ import annotations

from kombu.utils.uuid import uuid


class test_UUID:

    def test_uuid4(self) -> None:
        assert uuid() != uuid()

    def test_uuid(self) -> None:
        i1 = uuid()
        i2 = uuid()
        assert isinstance(i1, str)
        assert i1 != i2
