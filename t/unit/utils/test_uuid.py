from __future__ import absolute_import, unicode_literals

from kombu.utils.uuid import uuid


class test_UUID(object):

    def test_uuid4(self):
        assert uuid() != uuid()

    def test_uuid(self):
        i1 = uuid()
        i2 = uuid()
        assert isinstance(i1, str)
        assert i1 != i2
