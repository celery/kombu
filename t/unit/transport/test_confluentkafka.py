from __future__ import annotations

from unittest.mock import Mock

import pytest

pytest.importorskip('confluent_kafka')
from confluent_kafka import TopicPartition  # noqa: E402

from kombu.transport.confluentkafka import QoS  # noqa: E402


class test_QoS:

    def test_unacked_messages_are_tracked_per_channel(self):
        busy = QoS(Mock(), prefetch_count=1)
        idle = QoS(Mock(), prefetch_count=1)

        busy.append(Mock(topic='orders'), 'tag-1')

        assert not busy.can_consume()
        assert idle.can_consume()
        assert idle.can_consume_max_estimate() == 1

    def test_ack_commits_consumer_and_frees_prefetch_slot(self):
        channel = Mock()
        qos = QoS(channel, prefetch_count=1)
        qos.append(Mock(topic='orders', partition=0, offset=10), 'tag-1')

        qos.ack('tag-1')

        channel._get_consumer.assert_called_once_with('orders')
        channel._get_consumer.return_value.commit.assert_called_once_with(
            offsets=[TopicPartition('orders', 0, 11)]
        )
        assert qos.can_consume()

    def test_ack_does_not_commit_past_unacked_message(self):
        channel = Mock()
        qos = QoS(channel, prefetch_count=2)
        qos.append(Mock(topic='orders', partition=0, offset=5), 'tag-1')
        qos.append(Mock(topic='orders', partition=0, offset=6), 'tag-2')

        qos.ack('tag-2')

        channel._get_consumer.return_value.commit.assert_called_once_with(
            offsets=[TopicPartition('orders', 0, 5)]
        )

        qos.ack('tag-1')

        assert channel._get_consumer.return_value.commit.call_args_list == [
            ((), {'offsets': [TopicPartition('orders', 0, 5)]}),
            ((), {'offsets': [TopicPartition('orders', 0, 7)]}),
        ]

    def test_ack_only_commits_offset_for_message_partition(self):
        channel = Mock()
        qos = QoS(channel, prefetch_count=2)
        qos.append(Mock(topic='orders', partition=0, offset=5), 'tag-1')
        qos.append(Mock(topic='orders', partition=1, offset=10), 'tag-2')

        qos.ack('tag-1')

        channel._get_consumer.return_value.commit.assert_called_once_with(
            offsets=[TopicPartition('orders', 0, 6)]
        )
