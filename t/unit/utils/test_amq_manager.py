from __future__ import annotations

from unittest.mock import patch

import pytest

from kombu import Connection


class test_get_manager:

    @pytest.mark.masked_modules('pyrabbit')
    def test_without_pyrabbit(self, mask_modules):
        with pytest.raises(ImportError):
            Connection('amqp://').get_manager()

    @pytest.mark.ensured_modules('pyrabbit')
    def test_with_pyrabbit(self, module_exists):
        with patch('pyrabbit.Client', create=True) as Client:
            manager = Connection('amqp://').get_manager()
            assert manager is not None
            Client.assert_called_with(
                'localhost:15672', 'guest', 'guest',
            )

    @pytest.mark.ensured_modules('pyrabbit')
    def test_transport_options(self, module_exists):
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
