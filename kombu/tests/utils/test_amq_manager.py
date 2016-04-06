from __future__ import absolute_import, unicode_literals

from kombu import Connection

from kombu.tests.case import Case, mock, patch


class test_get_manager(Case):

    @mock.mask_modules('pyrabbit')
    def test_without_pyrabbit(self):
        with self.assertRaises(ImportError):
            Connection('amqp://').get_manager()

    @mock.module_exists('pyrabbit')
    def test_with_pyrabbit(self):
        with patch('pyrabbit.Client', create=True) as Client:
            manager = Connection('amqp://').get_manager()
            self.assertIsNotNone(manager)
            Client.assert_called_with(
                'localhost:15672', 'guest', 'guest',
            )

    @mock.module_exists('pyrabbit')
    def test_transport_options(self):
        with patch('pyrabbit.Client', create=True) as Client:
            manager = Connection('amqp://', transport_options={
                'manager_hostname': 'admin.mq.vandelay.com',
                'manager_port': 808,
                'manager_userid': 'george',
                'manager_password': 'bosco',
            }).get_manager()
            self.assertIsNotNone(manager)
            Client.assert_called_with(
                'admin.mq.vandelay.com:808', 'george', 'bosco',
            )
