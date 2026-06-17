from __future__ import annotations

import socket
from array import array
from queue import Empty
from unittest.mock import Mock, patch

import pytest

from kombu.transport.pgmq import (FANOUT_TOPIC_PREFIX, PGMQ_MAX_MESSAGES,
                                  Channel, Transport)

pytest.importorskip('pgmq')


class test_PGMQ:

    def setup_method(self):
        self.kombu_connection = Mock()
        self.kombu_connection.hostname = 'localhost'
        self.kombu_connection.port = 5432
        self.kombu_connection.userid = 'postgres'
        self.kombu_connection.password = 'postgres'
        self.kombu_connection.virtual_host = 'postgres'
        self.kombu_connection.transport_options = {}

        self.transport = Transport(self.kombu_connection)
        self.transport._pgmq_client = Mock()

        self.connection = Mock()
        self.connection.client = self.kombu_connection
        self.connection._get_pgmq_client = self.transport._get_pgmq_client
        self.connection._deliver = Mock()
        self.connection._used_channel_ids = array('H')
        self.connection.channel_max = 65535

        self.channel = Channel(connection=self.connection)

    def test_entity_name(self):
        assert self.channel.entity_name('celery') == 'celery'
        assert self.channel.entity_name('my.queue') == 'my_queue'

    def test_new_queue(self):
        self.channel._new_queue('celery')
        self.transport._pgmq_client.create_queue.assert_called_once_with('celery')

    def test_new_queue_partitioned(self):
        self.channel._new_queue(
            'celery',
            arguments={
                'x-pgmq-partitioned': True,
                'x-pgmq-partition-interval': 100,
                'x-pgmq-retention-interval': 1000,
            },
        )
        self.transport._pgmq_client.create_partitioned_queue.assert_called_once_with(
            'celery', 100, 1000)
        self.transport._pgmq_client.create_queue.assert_not_called()

    def test_new_queue_fifo_index(self):
        self.channel._new_queue('celery', arguments={'x-pgmq-fifo-index': True})
        self.transport._pgmq_client.create_fifo_index.assert_called_once_with(
            'celery')

    def test_new_queue_enables_notify(self):
        self.kombu_connection.transport_options = {
            'use_notify': True,
            'notify_throttle_interval_ms': 0,
        }
        channel = Channel(connection=self.transport)
        with patch.object(self.transport, '_register_notify_queue') as register:
            channel._new_queue('celery')
        self.transport._pgmq_client.enable_notify.assert_called_once_with(
            'celery', throttle_interval_ms=0)
        register.assert_called_once_with('celery')

    def test_put(self):
        message = {'body': 'hello', 'properties': {}}
        self.channel._put('celery', message)
        self.transport._pgmq_client.send.assert_called_once_with(
            'celery', message)

    def test_put_with_delay_and_headers(self):
        message = {
            'body': 'hello',
            'headers': {'trace': 'abc'},
            'properties': {
                'expiration': '2500',
                'MessageGroupId': 'group-1',
            },
        }
        self.channel._put('celery', message)
        self.transport._pgmq_client.send.assert_called_once_with(
            'celery',
            message,
            headers={'trace': 'abc', 'MessageGroupId': 'group-1'},
            delay=2,
        )

    def test_put_with_delay_seconds(self):
        message = {
            'body': 'hello',
            'properties': {'DelaySeconds': 5},
        }
        self.channel._put('celery', message)
        self.transport._pgmq_client.send.assert_called_once_with(
            'celery', message, delay=5)

    def test_put_topic(self):
        message = {'body': 'hello', 'properties': {}}
        self.channel._put_topic('orders', message, 'orders.created')
        self.transport._pgmq_client.send_topic.assert_called_once_with(
            'orders.orders.created', message)

    def test_topic_exchange_deliver_uses_send_topic(self):
        message = {'body': 'hello', 'properties': {}}
        exchange_type = self.channel.exchange_types['topic']
        exchange_type.deliver(message, 'test_topic', 'orders.created')
        self.transport._pgmq_client.send_topic.assert_called_once_with(
            'test_topic.orders.created', message)
        self.transport._pgmq_client.send.assert_not_called()

    def test_queue_bind_fanout(self):
        exchange_type = Mock(type='fanout')
        with patch.object(self.channel, 'typeof', return_value=exchange_type):
            self.channel._queue_bind('test_fanout', '', None, 'workers')
        self.transport._pgmq_client.create_queue.assert_called_with('workers')
        self.transport._pgmq_client.bind_topic.assert_called_once_with(
            f'{FANOUT_TOPIC_PREFIX}.test_fanout', 'workers')

    def test_queue_bind_topic(self):
        exchange_type = Mock(type='topic')
        with patch.object(self.channel, 'typeof', return_value=exchange_type):
            self.channel._queue_bind('test_topic', 'orders.*', None, 'orders_q')
        self.transport._pgmq_client.bind_topic.assert_called_once_with(
            'test_topic.orders.*', 'orders_q')

    def test_put_fanout(self):
        message = {'body': 'hello', 'properties': {}}
        self.channel._put_fanout('events', message, '')
        self.transport._pgmq_client.send_topic.assert_called_once_with(
            f'{FANOUT_TOPIC_PREFIX}.events', message)

    def test_get_fifo_grouped(self):
        self.kombu_connection.transport_options = {
            'fifo_mode': 'grouped',
            'wait_time_seconds': 0,
        }
        channel = Channel(connection=self.connection)

        payload = {
            'body': 'hello',
            'properties': {'delivery_info': {}},
        }
        pgmq_message = Mock(msg_id=42, read_ct=1, message=payload)
        self.transport._pgmq_client.read_grouped.return_value = [pgmq_message]

        result = channel._get('celery')

        assert result is payload
        self.transport._pgmq_client.read_grouped.assert_called_once_with(
            'celery', vt=1800, qty=1)
        self.transport._pgmq_client.read.assert_not_called()

    def test_get_with_ack(self):
        payload = {
            'body': 'hello',
            'properties': {'delivery_info': {}},
        }
        pgmq_message = Mock(msg_id=42, read_ct=1, message=payload)
        self.transport._pgmq_client.read_with_poll.return_value = [pgmq_message]

        result = self.channel._get('celery')

        assert result is payload
        assert result['properties']['delivery_info']['pgmq_queue'] == 'celery'
        assert result['properties']['delivery_info']['pgmq_msg_id'] == 42
        assert result['properties']['delivery_info']['pgmq_message'] == {
            'msg_id': 42,
            'read_ct': 1,
        }
        self.transport._pgmq_client.read_with_poll.assert_called_once_with(
            'celery',
            vt=1800,
            qty=1,
            max_poll_seconds=10,
            poll_interval_ms=100,
        )

    def test_get_no_ack(self):
        self.channel._noack_queues.add('celery')

        payload = {
            'body': 'hello',
            'properties': {'delivery_info': {}},
        }
        pgmq_message = Mock(msg_id=42, read_ct=1, message=payload)
        self.transport._pgmq_client.pop.return_value = pgmq_message

        result = self.channel._get('celery')

        assert result is payload
        assert 'pgmq_msg_id' not in result['properties']['delivery_info']
        self.transport._pgmq_client.pop.assert_called_once_with('celery', qty=1)

    def test_get_empty(self):
        self.transport._pgmq_client.read_with_poll.return_value = []
        with pytest.raises(Empty):
            self.channel._get('celery')

    def test_get_bulk_delivers_prefetch_messages(self):
        self.channel.qos.prefetch_count = 2
        payloads = [
            {'body': 'one', 'properties': {'delivery_info': {}}},
            {'body': 'two', 'properties': {'delivery_info': {}}},
        ]
        messages = [
            Mock(msg_id=1, read_ct=1, message=payloads[0]),
            Mock(msg_id=2, read_ct=1, message=payloads[1]),
        ]
        self.transport._pgmq_client.read_with_poll.return_value = messages

        self.channel._get_bulk('celery')

        self.transport._pgmq_client.read_with_poll.assert_called_once_with(
            'celery',
            vt=1800,
            qty=2,
            max_poll_seconds=10,
            poll_interval_ms=100,
        )
        assert self.connection._deliver.call_count == 2

    def test_get_bulk_respects_max_messages(self):
        self.channel.qos.prefetch_count = 100
        self.transport._pgmq_client.read_with_poll.return_value = []

        with pytest.raises(Empty):
            self.channel._get_bulk('celery')

        self.transport._pgmq_client.read_with_poll.assert_called_once_with(
            'celery',
            vt=1800,
            qty=PGMQ_MAX_MESSAGES,
            max_poll_seconds=10,
            poll_interval_ms=100,
        )

    def test_reset_cycle_uses_get_bulk(self):
        self.channel._active_queues = ['celery']
        self.channel._reset_cycle()
        assert self.channel._cycle.fun == self.channel._get_bulk

    def test_basic_ack(self):
        message = Mock()
        message.delivery_info = {
            'pgmq_queue': 'celery',
            'pgmq_msg_id': 7,
        }
        self.channel.qos._delivered['tag-1'] = message

        self.channel.basic_ack('tag-1')

        self.transport._pgmq_client.delete.assert_called_once_with('celery', 7)
        assert 'tag-1' in self.channel.qos._dirty

    def test_basic_reject_requeue(self):
        message = Mock()
        message.delivery_info = {
            'pgmq_queue': 'celery',
            'pgmq_msg_id': 7,
        }
        self.channel.qos._delivered['tag-1'] = message

        self.channel.basic_reject('tag-1', requeue=True)

        self.transport._pgmq_client.set_vt.assert_called_once_with(
            'celery', 7, 0)
        self.transport._pgmq_client.delete.assert_not_called()
        assert 'tag-1' in self.channel.qos._dirty

    def test_basic_reject_no_requeue(self):
        message = Mock()
        message.delivery_info = {
            'pgmq_queue': 'celery',
            'pgmq_msg_id': 7,
        }
        self.channel.qos._delivered['tag-1'] = message

        self.channel.basic_reject('tag-1', requeue=False)

        self.transport._pgmq_client.delete.assert_called_once_with('celery', 7)
        self.transport._pgmq_client.set_vt.assert_not_called()

    def test_purge_and_size(self):
        self.transport._pgmq_client.purge.return_value = 3
        self.transport._pgmq_client.metrics.return_value = Mock(
            queue_length=5)

        assert self.channel._purge('celery') == 3
        assert self.channel._size('celery') == 5

    def test_delete_queue(self):
        self.channel._delete('celery')
        self.transport._pgmq_client.drop_queue.assert_called_once_with('celery')

    def test_has_queue(self):
        record = Mock(queue_name='celery')
        self.transport._pgmq_client.list_queues.return_value = [record]

        assert self.channel._has_queue('celery') is True
        assert self.channel._has_queue('missing') is False

    def test_wait_time_seconds_legacy_alias(self):
        self.kombu_connection.transport_options = {'max_poll_seconds': 5}
        channel = Channel(connection=self.connection)
        assert channel.wait_time_seconds == 5

    def test_url_connection(self):
        with patch('kombu.transport.pgmq.PGMQueue') as PGMQueueMock:
            conn = Mock()
            conn.hostname = 'db.example.com'
            conn.port = 5433
            conn.userid = 'user'
            conn.password = 'secret'
            conn.virtual_host = '/mydb'
            conn.transport_options = {}

            transport = Transport(conn)
            transport._get_pgmq_client()

            PGMQueueMock.assert_called_once_with(
                host='db.example.com',
                port='5433',
                database='mydb',
                username='user',
                password='secret',
                vt=1800,
                init_extension=True,
                pool_size=10,
            )

    def test_close_connection_closes_pool(self):
        pool = Mock()
        self.transport._pgmq_client = Mock(pool=pool)

        self.transport.close_connection(Mock())

        pool.close.assert_called_once()
        assert self.transport._pgmq_client is None

    def test_driver_version(self):
        with patch('pgmq.__version__', '1.2.3'):
            assert Transport(self.kombu_connection).driver_version() == '1.2.3'

    def test_drain_events_uses_notify_waiter(self):
        self.kombu_connection.transport_options = {
            'use_notify': True,
            'polling_interval': 0.5,
        }
        transport = Transport(self.kombu_connection)
        transport._pgmq_client = self.transport._pgmq_client
        transport._pgmq_client.config.dsn = 'postgresql://localhost/test'
        waiter = Mock()
        waiter.wait.return_value = False
        transport._notify_waiter = waiter

        with patch.object(transport.cycle, 'get', side_effect=Empty()):
            with pytest.raises(socket.timeout):
                transport.drain_events(Mock(), timeout=0.01)

        waiter.wait.assert_called()
