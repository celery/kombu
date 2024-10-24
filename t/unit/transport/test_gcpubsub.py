from __future__ import annotations

from concurrent.futures import Future
from datetime import datetime
from queue import Empty
from unittest.mock import MagicMock, call, patch

import pytest
from _socket import timeout as socket_timeout
from google.api_core.exceptions import (AlreadyExists, DeadlineExceeded,
                                        PermissionDenied)
from google.pubsub_v1.types.pubsub import Subscription

from kombu.transport.gcpubsub import (AtomicCounter, Channel, QueueDescriptor,
                                      Transport, UnackedIds)


class test_UnackedIds:
    def setup_method(self):
        self.unacked_ids = UnackedIds()

    def test_append(self):
        self.unacked_ids.append('test_id')
        assert self.unacked_ids[0] == 'test_id'

    def test_extend(self):
        self.unacked_ids.extend(['test_id1', 'test_id2'])
        assert self.unacked_ids[0] == 'test_id1'
        assert self.unacked_ids[1] == 'test_id2'

    def test_pop(self):
        self.unacked_ids.append('test_id')
        popped_id = self.unacked_ids.pop()
        assert popped_id == 'test_id'
        assert len(self.unacked_ids) == 0

    def test_remove(self):
        self.unacked_ids.append('test_id')
        self.unacked_ids.remove('test_id')
        assert len(self.unacked_ids) == 0

    def test_len(self):
        self.unacked_ids.append('test_id')
        assert len(self.unacked_ids) == 1

    def test_getitem(self):
        self.unacked_ids.append('test_id')
        assert self.unacked_ids[0] == 'test_id'


class test_AtomicCounter:
    def setup_method(self):
        self.counter = AtomicCounter()

    def test_inc(self):
        assert self.counter.inc() == 1
        assert self.counter.inc(5) == 6

    def test_dec(self):
        self.counter.inc(5)
        assert self.counter.dec() == 4
        assert self.counter.dec(2) == 2

    def test_get(self):
        self.counter.inc(7)
        assert self.counter.get() == 7


@pytest.fixture
def channel():
    with patch.object(Channel, '__init__', lambda self: None):
        channel = Channel()
        channel.connection = MagicMock()
        channel.queue_name_prefix = "kombu-"
        channel.project_id = "test_project"
        channel._queue_cache = {}
        channel._n_channels = MagicMock()
        channel._stop_extender = MagicMock()
        channel.subscriber = MagicMock()
        channel.publisher = MagicMock()
        channel.closed = False
        with patch.object(
            Channel, 'conninfo', new_callable=MagicMock
        ), patch.object(
            Channel, 'transport_options', new_callable=MagicMock
        ), patch.object(
            Channel, 'qos', new_callable=MagicMock
        ):
            yield channel


class test_Channel:
    @patch('kombu.transport.gcpubsub.ThreadPoolExecutor')
    @patch('kombu.transport.gcpubsub.threading.Event')
    @patch('kombu.transport.gcpubsub.threading.Thread')
    @patch(
        'kombu.transport.gcpubsub.Channel._get_free_channel_id',
        return_value=1,
    )
    @patch(
        'kombu.transport.gcpubsub.Channel._n_channels.inc',
        return_value=1,
    )
    def test_channel_init(
        self,
        n_channels_in_mock,
        channel_id_mock,
        mock_thread,
        mock_event,
        mock_executor,
    ):
        mock_connection = MagicMock()
        ch = Channel(mock_connection)
        ch._n_channels.inc.assert_called_once()
        mock_thread.assert_called_once_with(
            target=ch._extend_unacked_deadline,
            daemon=True,
        )
        mock_thread.return_value.start.assert_called_once()

    def test_entity_name(self, channel):
        name = "test_queue"
        result = channel.entity_name(name)
        assert result == "kombu-test_queue"

    @patch('kombu.transport.gcpubsub.uuid3', return_value='uuid')
    @patch('kombu.transport.gcpubsub.gethostname', return_value='hostname')
    @patch('kombu.transport.gcpubsub.getpid', return_value=1234)
    def test_queue_bind_direct(
        self, mock_pid, mock_hostname, mock_uuid, channel
    ):
        exchange = 'direct'
        routing_key = 'test_routing_key'
        pattern = 'test_pattern'
        queue = 'test_queue'
        subscription_path = 'projects/project-id/subscriptions/test_queue'
        channel.subscriber.subscription_path = MagicMock(
            return_value=subscription_path
        )
        channel._create_topic = MagicMock(return_value='topic_path')
        channel._create_subscription = MagicMock()

        # Mock the state and exchange type
        mock_connection = MagicMock(name='mock_connection')
        channel.connection = mock_connection
        channel.state.exchanges = {exchange: {'type': 'direct'}}
        mock_exchange = MagicMock(name='mock_exchange', type='direct')
        channel.exchange_types = {'direct': mock_exchange}

        channel._queue_bind(exchange, routing_key, pattern, queue)

        channel._create_topic.assert_called_once_with(
            channel.project_id, exchange, channel.expiration_seconds
        )
        channel._create_subscription.assert_called_once_with(
            topic_path='topic_path',
            subscription_path=subscription_path,
            filter_args={'filter': f'attributes.routing_key="{routing_key}"'},
            msg_retention=channel.expiration_seconds,
        )
        assert channel.entity_name(queue) in channel._queue_cache

    @patch('kombu.transport.gcpubsub.uuid3', return_value='uuid')
    @patch('kombu.transport.gcpubsub.gethostname', return_value='hostname')
    @patch('kombu.transport.gcpubsub.getpid', return_value=1234)
    def test_queue_bind_fanout(
        self, mock_pid, mock_hostname, mock_uuid, channel
    ):
        exchange = 'test_exchange'
        routing_key = 'test_routing_key'
        pattern = 'test_pattern'
        queue = 'test_queue'
        uniq_sub_name = 'test_queue-uuid'
        subscription_path = (
            f'projects/project-id/subscriptions/{uniq_sub_name}'
        )
        channel.subscriber.subscription_path = MagicMock(
            return_value=subscription_path
        )
        channel._create_topic = MagicMock(return_value='topic_path')
        channel._create_subscription = MagicMock()

        # Mock the state and exchange type
        mock_connection = MagicMock(name='mock_connection')
        channel.connection = mock_connection
        channel.state.exchanges = {exchange: {'type': 'fanout'}}
        mock_exchange = MagicMock(name='mock_exchange', type='fanout')
        channel.exchange_types = {'fanout': mock_exchange}

        channel._queue_bind(exchange, routing_key, pattern, queue)

        channel._create_topic.assert_called_once_with(
            channel.project_id, exchange, 600
        )
        channel._create_subscription.assert_called_once_with(
            topic_path='topic_path',
            subscription_path=subscription_path,
            filter_args={},
            msg_retention=600,
        )
        assert channel.entity_name(queue) in channel._queue_cache
        assert subscription_path in channel._tmp_subscriptions
        assert exchange in channel._fanout_exchanges

    def test_queue_bind_not_implemented(self, channel):
        exchange = 'test_exchange'
        routing_key = 'test_routing_key'
        pattern = 'test_pattern'
        queue = 'test_queue'
        channel.typeof = MagicMock(return_value=MagicMock(type='unsupported'))

        with pytest.raises(NotImplementedError):
            channel._queue_bind(exchange, routing_key, pattern, queue)

    def test_create_topic(self, channel):
        channel.project_id = "project_id"
        topic_id = "topic_id"
        channel._is_topic_exists = MagicMock(return_value=False)
        channel.publisher.topic_path = MagicMock(return_value="topic_path")
        channel.publisher.create_topic = MagicMock()
        result = channel._create_topic(channel.project_id, topic_id)
        assert result == "topic_path"
        channel.publisher.create_topic.assert_called_once()

        channel._create_topic(
            channel.project_id, topic_id, message_retention_duration=10
        )
        assert (
            dict(
                request={
                    'name': 'topic_path',
                    'message_retention_duration': '10s',
                }
            )
            in channel.publisher.create_topic.call_args
        )
        channel.publisher.create_topic.side_effect = AlreadyExists(
            "test_error"
        )
        channel._create_topic(
            channel.project_id, topic_id, message_retention_duration=10
        )

    def test_is_topic_exists(self, channel):
        topic_path = "projects/project-id/topics/test_topic"
        mock_topic = MagicMock()
        mock_topic.name = topic_path
        channel.publisher.list_topics.return_value = [mock_topic]

        result = channel._is_topic_exists(topic_path)

        assert result is True
        channel.publisher.list_topics.assert_called_once_with(
            request={"project": f'projects/{channel.project_id}'}
        )

    def test_is_topic_not_exists(self, channel):
        topic_path = "projects/project-id/topics/test_topic"
        channel.publisher.list_topics.return_value = []

        result = channel._is_topic_exists(topic_path)

        assert result is False
        channel.publisher.list_topics.assert_called_once_with(
            request={"project": f'projects/{channel.project_id}'}
        )

    def test_create_subscription(self, channel):
        channel.project_id = "project_id"
        topic_id = "topic_id"
        subscription_path = "subscription_path"
        topic_path = "topic_path"
        channel.subscriber.subscription_path = MagicMock(
            return_value=subscription_path
        )
        channel.publisher.topic_path = MagicMock(return_value=topic_path)
        channel.subscriber.create_subscription = MagicMock()
        result = channel._create_subscription(
            project_id=channel.project_id,
            topic_id=topic_id,
            subscription_path=subscription_path,
            topic_path=topic_path,
        )
        assert result == subscription_path
        channel.subscriber.create_subscription.assert_called_once()

    def test_create_subscription_protobuf_compat(self):
        request = {
            'name': 'projects/my_project/subscriptions/kombu-1111-2222',
            'topic': 'projects/jether-fox/topics/reply.celery.pidbox',
            'ack_deadline_seconds': 240,
            'expiration_policy': {'ttl': '86400s'},
            'message_retention_duration': '86400s',
            'filter': 'attributes.routing_key="1111-2222"',
        }
        Subscription(request)

    def test_delete(self, channel):
        queue = "test_queue"
        subscription_path = "projects/project-id/subscriptions/test_queue"
        qdesc = QueueDescriptor(
            name=queue,
            topic_path="projects/project-id/topics/test_topic",
            subscription_id=queue,
            subscription_path=subscription_path,
        )
        channel.subscriber = MagicMock()
        channel._queue_cache[channel.entity_name(queue)] = qdesc

        channel._delete(queue)

        channel.subscriber.delete_subscription.assert_called_once_with(
            request={"subscription": subscription_path}
        )
        assert queue not in channel._queue_cache

    def test_put(self, channel):
        queue = "test_queue"
        message = {
            "properties": {"delivery_info": {"routing_key": "test_key"}}
        }
        channel.entity_name = MagicMock(return_value=queue)
        channel._queue_cache[channel.entity_name(queue)] = QueueDescriptor(
            name=queue,
            topic_path="topic_path",
            subscription_id=queue,
            subscription_path="subscription_path",
        )
        channel._get_routing_key = MagicMock(return_value="test_key")
        channel.publisher.publish = MagicMock()
        channel._put(queue, message)
        channel.publisher.publish.assert_called_once()

    def test_put_fanout(self, channel):
        exchange = "test_exchange"
        message = {
            "properties": {"delivery_info": {"routing_key": "test_key"}}
        }
        routing_key = "test_key"

        channel._lookup = MagicMock()
        channel.publisher.topic_path = MagicMock(return_value="topic_path")
        channel.publisher.publish = MagicMock()

        channel._put_fanout(exchange, message, routing_key)

        channel._lookup.assert_called_once_with(exchange, routing_key)
        channel.publisher.topic_path.assert_called_once_with(
            channel.project_id, exchange
        )
        assert 'topic_path', (
            b'{"properties": {"delivery_info": {"routing_key": "test_key"}}}'
            in channel.publisher.publish.call_args
        )

    def test_get(self, channel):
        queue = "test_queue"
        channel.entity_name = MagicMock(return_value=queue)
        channel._queue_cache[queue] = QueueDescriptor(
            name=queue,
            topic_path="topic_path",
            subscription_id=queue,
            subscription_path="subscription_path",
        )
        channel.subscriber.pull = MagicMock(
            return_value=MagicMock(
                received_messages=[
                    MagicMock(
                        ack_id="ack_id",
                        message=MagicMock(
                            data=b'{"properties": '
                            b'{"delivery_info": '
                            b'{"exchange": "exchange"},"delivery_mode": 1}}'
                        ),
                    )
                ]
            )
        )
        channel.subscriber.acknowledge = MagicMock()
        payload = channel._get(queue)
        assert (
            payload["properties"]["delivery_info"]["exchange"] == "exchange"
        )
        channel.subscriber.pull.side_effect = DeadlineExceeded("test_error")
        with pytest.raises(Empty):
            channel._get(queue)

    def test_get_bulk(self, channel):
        queue = "test_queue"
        subscription_path = "projects/project-id/subscriptions/test_queue"
        qdesc = QueueDescriptor(
            name=queue,
            topic_path="projects/project-id/topics/test_topic",
            subscription_id=queue,
            subscription_path=subscription_path,
        )
        channel._queue_cache[channel.entity_name(queue)] = qdesc

        data = b'{"properties": {"delivery_info": {"exchange": "exchange"}}}'
        received_message = MagicMock(
            ack_id="ack_id",
            message=MagicMock(data=data),
        )
        channel.subscriber.pull = MagicMock(
            return_value=MagicMock(received_messages=[received_message])
        )
        channel.bulk_max_messages = 10
        channel._is_auto_ack = MagicMock(return_value=True)
        channel._do_ack = MagicMock()
        channel.qos.can_consume_max_estimate = MagicMock(return_value=None)
        queue, payloads = channel._get_bulk(queue, timeout=10)

        assert len(payloads) == 1
        assert (
            payloads[0]["properties"]["delivery_info"]["exchange"]
            == "exchange"
        )
        channel._do_ack.assert_called_once_with(["ack_id"], subscription_path)

        channel.subscriber.pull.side_effect = DeadlineExceeded("test_error")
        with pytest.raises(Empty):
            channel._get_bulk(queue, timeout=10)

    def test_lookup(self, channel):
        exchange = "test_exchange"
        routing_key = "test_key"
        default = None

        channel.connection = MagicMock()
        channel.state.exchanges = {exchange: {"type": "direct"}}
        channel.typeof = MagicMock(
            return_value=MagicMock(lookup=MagicMock(return_value=["queue1"]))
        )
        channel.get_table = MagicMock(return_value="table")

        result = channel._lookup(exchange, routing_key, default)

        channel.typeof.return_value.lookup.assert_called_once_with(
            "table", exchange, routing_key, default
        )
        assert result == ["queue1"]

        # Test the case where no queues are bound to the exchange
        channel.typeof.return_value.lookup.return_value = None
        channel.queue_bind = MagicMock()

        result = channel._lookup(exchange, routing_key, default)

        channel.queue_bind.assert_called_once_with(
            exchange, exchange, routing_key
        )
        assert result == [exchange]

    @patch('kombu.transport.gcpubsub.monitoring_v3')
    @patch('kombu.transport.gcpubsub.query.Query')
    def test_size(self, mock_query, mock_monitor, channel):
        queue = "test_queue"
        subscription_id = "test_subscription"
        qdesc = QueueDescriptor(
            name=queue,
            topic_path="projects/project-id/topics/test_topic",
            subscription_id=subscription_id,
            subscription_path="projects/project-id/subscriptions/test_subscription",  # E501
        )
        channel._queue_cache[channel.entity_name(queue)] = qdesc

        mock_query_result = MagicMock()
        mock_query_result.select_resources.return_value = [
            MagicMock(points=[MagicMock(value=MagicMock(int64_value=5))])
        ]
        mock_query.return_value = mock_query_result
        size = channel._size(queue)
        assert size == 5

        # Test the case where the queue is not in the cache
        size = channel._size("non_existent_queue")
        assert size == 0

        # Test the case where the query raises PermissionDenied
        mock_item = MagicMock()
        mock_item.points.__getitem__.side_effect = PermissionDenied(
            'test_error'
        )
        mock_query_result.select_resources.return_value = [mock_item]
        size = channel._size(queue)
        assert size == -1

    def test_basic_ack(self, channel):
        delivery_tag = "test_delivery_tag"
        ack_id = "test_ack_id"
        queue = "test_queue"
        subscription_path = (
            "projects/project-id/subscriptions/test_subscription"
        )
        qdesc = QueueDescriptor(
            name=queue,
            topic_path="projects/project-id/topics/test_topic",
            subscription_id="test_subscription",
            subscription_path=subscription_path,
        )
        channel._queue_cache[queue] = qdesc

        delivery_info = {
            'gcpubsub_message': {
                'queue': queue,
                'ack_id': ack_id,
                'subscription_path': subscription_path,
            }
        }
        channel.qos.get = MagicMock(
            return_value=MagicMock(delivery_info=delivery_info)
        )
        channel._do_ack = MagicMock()

        channel.basic_ack(delivery_tag)

        channel._do_ack.assert_called_once_with([ack_id], subscription_path)
        assert ack_id not in qdesc.unacked_ids

    def test_do_ack(self, channel):
        ack_ids = ["ack_id1", "ack_id2"]
        subscription_path = (
            "projects/project-id/subscriptions/test_subscription"
        )
        channel.subscriber = MagicMock()

        channel._do_ack(ack_ids, subscription_path)
        assert subscription_path, (
            ack_ids in channel.subscriber.acknowledge.call_args
        )

    def test_purge(self, channel):
        queue = "test_queue"
        subscription_path = f"projects/project-id/subscriptions/{queue}"
        qdesc = QueueDescriptor(
            name=queue,
            topic_path="projects/project-id/topics/test_topic",
            subscription_id="test_subscription",
            subscription_path=subscription_path,
        )
        channel._queue_cache[channel.entity_name(queue)] = qdesc
        channel.subscriber = MagicMock()

        with patch.object(channel, '_size', return_value=10), patch(
            'kombu.transport.gcpubsub.datetime.datetime'
        ) as dt_mock:
            dt_mock.now.return_value = datetime(2021, 1, 1)
            result = channel._purge(queue)
            assert result == 10
            channel.subscriber.seek.assert_called_once_with(
                request={
                    "subscription": subscription_path,
                    "time": datetime(2021, 1, 1),
                }
            )

        # Test the case where the queue is not in the cache
        result = channel._purge("non_existent_queue")
        assert result is None

    def test_extend_unacked_deadline(self, channel):
        queue = "test_queue"
        subscription_path = (
            "projects/project-id/subscriptions/test_subscription"
        )
        ack_ids = ["ack_id1", "ack_id2"]
        qdesc = QueueDescriptor(
            name=queue,
            topic_path="projects/project-id/topics/test_topic",
            subscription_id="test_subscription",
            subscription_path=subscription_path,
        )
        channel.transport_options = {"ack_deadline_seconds": 240}
        channel._queue_cache[channel.entity_name(queue)] = qdesc
        qdesc.unacked_ids.extend(ack_ids)

        channel._stop_extender.wait = MagicMock(side_effect=[False, True])
        channel.subscriber.modify_ack_deadline = MagicMock()

        channel._extend_unacked_deadline()

        channel.subscriber.modify_ack_deadline.assert_called_once_with(
            request={
                "subscription": subscription_path,
                "ack_ids": ack_ids,
                "ack_deadline_seconds": 240,
            }
        )
        for _ in ack_ids:
            qdesc.unacked_ids.pop()
        channel._stop_extender.wait = MagicMock(side_effect=[False, True])
        modify_ack_deadline_calls = (
            channel.subscriber.modify_ack_deadline.call_count
        )
        channel._extend_unacked_deadline()
        assert (
            channel.subscriber.modify_ack_deadline.call_count
            == modify_ack_deadline_calls
        )

    def test_after_reply_message_received(self, channel):
        queue = 'test-queue'
        subscription_path = f'projects/test-project/subscriptions/{queue}'

        channel.subscriber.subscription_path.return_value = subscription_path
        channel.after_reply_message_received(queue)

        # Check that the subscription path is added to _tmp_subscriptions
        assert subscription_path in channel._tmp_subscriptions

    def test_subscriber(self, channel):
        assert channel.subscriber

    def test_publisher(self, channel):
        assert channel.publisher

    def test_transport_options(self, channel):
        assert channel.transport_options

    def test_bulk_max_messages_default(self, channel):
        assert channel.bulk_max_messages == channel.transport_options.get(
            'bulk_max_messages'
        )

    def test_close(self, channel):
        channel._tmp_subscriptions = {'sub1', 'sub2'}
        channel._n_channels.dec.return_value = 0

        with patch.object(
            Channel._unacked_extender, 'join'
        ) as mock_join, patch(
            'kombu.transport.virtual.Channel.close'
        ) as mock_super_close:
            channel.close()

            channel.subscriber.delete_subscription.assert_has_calls(
                [
                    call(request={"subscription": 'sub1'}),
                    call(request={"subscription": 'sub2'}),
                ],
                any_order=True,
            )
            channel._stop_extender.set.assert_called_once()
            mock_join.assert_called_once()
            mock_super_close.assert_called_once()


@pytest.fixture
def transport():
    return Transport(client=MagicMock())


class test_Transport:
    def test_driver_version(self, transport):
        assert transport.driver_version()

    def test_as_uri(self, transport):
        result = transport.as_uri('gcpubsub://')
        assert result == 'gcpubsub://'

    def test_drain_events_timeout(self, transport):
        transport.polling_interval = 4
        with patch.object(
            transport, '_drain_from_active_queues', side_effect=Empty
        ), patch(
            'kombu.transport.gcpubsub.monotonic',
            side_effect=[0, 1, 2, 3, 4, 5],
        ), patch(
            'kombu.transport.gcpubsub.sleep'
        ) as mock_sleep:
            with pytest.raises(socket_timeout):
                transport.drain_events(None, timeout=3)
            mock_sleep.assert_called()

    def test_drain_events_no_timeout(self, transport):
        with patch.object(
            transport, '_drain_from_active_queues', side_effect=[Empty, None]
        ), patch(
            'kombu.transport.gcpubsub.monotonic', side_effect=[0, 1]
        ), patch(
            'kombu.transport.gcpubsub.sleep'
        ) as mock_sleep:
            transport.drain_events(None, timeout=None)
            mock_sleep.assert_called()

    def test_drain_events_polling_interval(self, transport):
        transport.polling_interval = 2
        with patch.object(
            transport, '_drain_from_active_queues', side_effect=[Empty, None]
        ), patch(
            'kombu.transport.gcpubsub.monotonic', side_effect=[0, 1, 2]
        ), patch(
            'kombu.transport.gcpubsub.sleep'
        ) as mock_sleep:
            transport.drain_events(None, timeout=5)
            mock_sleep.assert_called_with(2)

    def test_drain_from_active_queues_empty(self, transport):
        with patch.object(
            transport, '_rm_empty_bulk_requests'
        ) as mock_rm_empty, patch.object(
            transport, '_submit_get_bulk_requests'
        ) as mock_submit, patch(
            'kombu.transport.gcpubsub.wait', return_value=(set(), set())
        ) as mock_wait:
            with pytest.raises(Empty):
                transport._drain_from_active_queues(timeout=10)
            mock_rm_empty.assert_called_once()
            mock_submit.assert_called_once_with(timeout=10)
            mock_wait.assert_called_once()

    def test_drain_from_active_queues_done(self, transport):
        future = Future()
        future.set_result(('queue', [{'properties': {'delivery_info': {}}}]))

        with patch.object(
            transport, '_rm_empty_bulk_requests'
        ) as mock_rm_empty, patch.object(
            transport, '_submit_get_bulk_requests'
        ) as mock_submit, patch(
            'kombu.transport.gcpubsub.wait', return_value=({future}, set())
        ) as mock_wait, patch.object(
            transport, '_deliver'
        ) as mock_deliver:
            transport._callbacks = {'queue'}
            transport._drain_from_active_queues(timeout=10)
            mock_rm_empty.assert_called_once()
            mock_submit.assert_called_once_with(timeout=10)
            mock_wait.assert_called_once()
            mock_deliver.assert_called_once_with(
                {'properties': {'delivery_info': {}}}, 'queue'
            )

            mock_deliver_call_count = mock_deliver.call_count
            transport._callbacks = {}
            transport._drain_from_active_queues(timeout=10)
            assert mock_deliver_call_count == mock_deliver.call_count

    def test_drain_from_active_queues_exception(self, transport):
        future = Future()
        future.set_exception(Exception("Test exception"))

        with patch.object(
            transport, '_rm_empty_bulk_requests'
        ) as mock_rm_empty, patch.object(
            transport, '_submit_get_bulk_requests'
        ) as mock_submit, patch(
            'kombu.transport.gcpubsub.wait', return_value=({future}, set())
        ) as mock_wait:
            with pytest.raises(Empty):
                transport._drain_from_active_queues(timeout=10)
            mock_rm_empty.assert_called_once()
            mock_submit.assert_called_once_with(timeout=10)
            mock_wait.assert_called_once()

    def test_rm_empty_bulk_requests(self, transport):
        # Create futures with exceptions to simulate empty requests
        future_with_exception = Future()
        future_with_exception.set_exception(Exception("Test exception"))

        transport._get_bulk_future_to_queue = {
            future_with_exception: 'queue1',
        }

        transport._rm_empty_bulk_requests()

        # Assert that the future with exception is removed
        assert (
            future_with_exception not in transport._get_bulk_future_to_queue
        )

    def test_submit_get_bulk_requests(self, transport):
        channel_mock = MagicMock(spec=Channel)
        channel_mock._active_queues = ['queue1', 'queue2']
        transport.channels = [channel_mock]

        with patch.object(
            transport._pool, 'submit', return_value=MagicMock()
        ) as mock_submit:
            transport._submit_get_bulk_requests(timeout=10)

            # Check that submit was called twice, once for each queue
            assert mock_submit.call_count == 2
            mock_submit.assert_any_call(channel_mock._get_bulk, 'queue1', 10)
            mock_submit.assert_any_call(channel_mock._get_bulk, 'queue2', 10)

    def test_submit_get_bulk_requests_with_existing_futures(self, transport):
        channel_mock = MagicMock(spec=Channel)
        channel_mock._active_queues = ['queue1', 'queue2']
        transport.channels = [channel_mock]

        # Simulate existing futures
        future_mock = MagicMock()
        transport._get_bulk_future_to_queue = {future_mock: 'queue1'}

        with patch.object(
            transport._pool, 'submit', return_value=MagicMock()
        ) as mock_submit:
            transport._submit_get_bulk_requests(timeout=10)

            # Check that submit was called only once for the new queue
            assert mock_submit.call_count == 1
            mock_submit.assert_called_with(
                channel_mock._get_bulk, 'queue2', 10
            )
