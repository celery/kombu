from __future__ import annotations

import logging
from unittest.mock import Mock

import pytest

from kombu.transport.native_delayed_delivery import (
    CELERY_DELAYED_DELIVERY_EXCHANGE,
    bind_queue_to_native_delayed_delivery_exchange, calculate_routing_key,
    declare_native_delayed_delivery_exchanges_and_queues, level_name)


class test_native_delayed_delivery_level_name:
    def test_level_name_with_negative_level(self):
        with pytest.raises(ValueError, match="level must be a non-negative number"):
            level_name(-1)

    def test_level_name_with_level_0(self):
        assert level_name(0) == 'celery_delayed_0'

    def test_level_name_with_level_1(self):
        assert level_name(1) == 'celery_delayed_1'


class test_declare_native_delayed_delivery_exchanges_and_queues:
    def test_invalid_queue_type(self):
        with pytest.raises(ValueError, match="queue_type must be either classic or quorum"):
            declare_native_delayed_delivery_exchanges_and_queues(Mock(), 'foo')

    def test_classic_queue_type(self):
        declare_native_delayed_delivery_exchanges_and_queues(Mock(), 'classic')

    def test_quorum_queue_type(self):
        declare_native_delayed_delivery_exchanges_and_queues(Mock(), 'quorum')


class test_bind_queue_to_native_delayed_delivery_exchange:
    def test_bind_to_direct_exchange(self, caplog):
        with caplog.at_level(logging.WARNING):
            queue_mock = Mock()
            queue_mock.bind().exchange.bind().type = 'direct'
            queue_mock.bind().exchange.bind().name = 'foo'

            bind_queue_to_native_delayed_delivery_exchange(Mock(), queue_mock)

        assert len(caplog.records) == 1

        record = caplog.records[0]
        assert (record.message == "Exchange foo is a direct exchange "
                                  "and native delayed delivery do not support direct exchanges.\n"
                                  "ETA tasks published to this exchange will "
                                  "block the worker until the ETA arrives.")

    def test_bind_to_topic_exchange(self):
        queue_mock = Mock()
        queue_mock.bind().exchange.bind().type = 'topic'
        queue_mock.bind().exchange.bind().name = 'foo'
        queue_mock.bind().routing_key = 'foo'

        bind_queue_to_native_delayed_delivery_exchange(Mock(), queue_mock)
        queue_mock.bind().exchange.bind().bind_to.assert_called_once_with(
            CELERY_DELAYED_DELIVERY_EXCHANGE,
            routing_key="#.foo"
        )
        queue_mock.bind().bind_to.assert_called_once_with(
            'foo',
            routing_key="#.foo"
        )

    def test_bind_to_topic_exchange_with_wildcard_routing_key(self):
        queue_mock = Mock()
        queue_mock.bind().exchange.bind().type = 'topic'
        queue_mock.bind().exchange.bind().name = 'foo'
        queue_mock.bind().routing_key = '#.foo'

        bind_queue_to_native_delayed_delivery_exchange(Mock(), queue_mock)
        queue_mock.bind().exchange.bind().bind_to.assert_called_once_with(
            CELERY_DELAYED_DELIVERY_EXCHANGE,
            routing_key="#.foo"
        )
        queue_mock.bind().bind_to.assert_called_once_with(
            'foo',
            routing_key="#.foo"
        )


class test_calculate_routing_key:
    def test_calculate_routing_key(self):
        assert (calculate_routing_key(1, 'destination')
                == '0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.1.destination')

    def test_negative_countdown(self):
        with pytest.raises(ValueError, match="countdown must be a positive number"):
            calculate_routing_key(-1, 'foo')

    def test_zero_countdown(self):
        with pytest.raises(ValueError, match="countdown must be a positive number"):
            calculate_routing_key(0, 'foo')

    def test_empty_routing_key(self):
        with pytest.raises(ValueError, match="routing_key must be non-empty"):
            calculate_routing_key(1, '')

    def test_none_routing_key(self):
        with pytest.raises(ValueError, match="routing_key must be non-empty"):
            calculate_routing_key(1, None)
