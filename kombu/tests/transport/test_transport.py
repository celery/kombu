from __future__ import absolute_import

from kombu import transport

from kombu.tests.case import Case, patch


class test_transport(Case):

    def test_resolve_transport_when_callable(self):
        from kombu.transport.memory import Transport
        self.assertIs(transport.resolve_transport(
            'kombu.transport.memory:Transport'),
            Transport)


class test_transport_gettoq(Case):

    @patch('warnings.warn')
    def test_compat(self, warn):
        x = transport._ghettoq('Redis', 'redis', 'redis')

        self.assertEqual(x(), 'kombu.transport.redis.Transport')
        self.assertTrue(warn.called)
