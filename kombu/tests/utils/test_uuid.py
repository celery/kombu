from __future__ import absolute_import, unicode_literals

from kombu.utils.uuid import uuid

from kombu.tests.case import Case


class test_UUID(Case):

    def test_uuid4(self):
        self.assertNotEqual(uuid(), uuid())

    def test_uuid(self):
        i1 = uuid()
        i2 = uuid()
        self.assertIsInstance(i1, str)
        self.assertNotEqual(i1, i2)
