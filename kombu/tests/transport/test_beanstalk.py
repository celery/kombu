from __future__ import absolute_import, unicode_literals

from kombu.tests.case import Case


class FakeJob(object):

    def __init__(self, tube):
        self.tube = tube
        self.body = '""'

    def stats(self):
        return {
            'tube': self.tube
        }

    def delete(self):
        pass

    def bury(self):
        pass


class FakeBeanstalkConnection(object):

    def watch(self, tube):
        self.tube = tube
        return 1

    def watching(self):
        return []

    def close(self):
        pass

    def reserve(self, timeout=None):
        return FakeJob(self.tube)

    def ignore(self, tube):
        pass


class TestBeanstalk(Case):

    def create_channel(self):
        from kombu.transport import beanstalk

        class _Channel(beanstalk.Channel):

            def __init__(self):
                pass

            def _open(self):
                return FakeBeanstalkConnection()
        channel = _Channel()
        channel._tube_map = {}
        return channel

    def test_valid_beanstalk_tube_name_in_get_many(self):
        self.channel = self.create_channel()
        _, tube = self.channel._get_many(['test'])
        self.assertIn('test', self.channel._tube_map)
        self.assertEqual('test', tube)

    def test_valid_beanstalk_tube_name_in_get(self):
        self.channel = self.create_channel()
        self.channel._get('test')
        self.assertIn('test', self.channel._tube_map)

    def test_invalid_beanstalk_tube_name_in_get_many(self):
        self.channel = self.create_channel()
        invalid_tube_name = 'celery@laptop.celery.pidbox'
        _, tube = self.channel._get_many([invalid_tube_name])
        self.assertNotIn(invalid_tube_name, self.channel._tube_map)
        self.assertIn('celery.laptop.celery.pidbox', self.channel._tube_map)
        self.assertEqual(invalid_tube_name, tube)

    def test_invalid_beanstalk_tube_name_in_get(self):
        self.channel = self.create_channel()
        invalid_tube_name = 'celery@laptop.celery.pidbox'
        self.channel._get(invalid_tube_name)
        self.assertNotIn(invalid_tube_name, self.channel._tube_map)
        self.assertIn('celery.laptop.celery.pidbox', self.channel._tube_map)
