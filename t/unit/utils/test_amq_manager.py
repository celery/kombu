from __future__ import absolute_import, unicode_literals

import pytest

from case import mock, patch

from kombu import Connection


class test_get_manager:

    @mock.mask_modules('pyrabbit')
    def test_without_pyrabbit(self):
        with pytest.raises(ImportError):
            Connection('amqp://').get_manager()

    @mock.module_exists('pyrabbit')
    def test_with_pyrabbit(self):
        with patch('pyrabbit.Client', create=True) as Client:
            manager = Connection('amqp://').get_manager()
            assert manager is not None
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
            assert manager is not None
            Client.assert_called_with(
                'admin.mq.vandelay.com:808', 'george', 'bosco',
            )
