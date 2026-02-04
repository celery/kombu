from __future__ import annotations

from queue import Empty
from typing import cast
from unittest.mock import MagicMock, Mock, patch

import pytest
from rocketmq import (ClientConfiguration, FilterExpression, Producer,
                      SimpleConsumer)
from rocketmq.grpc_protocol import FilterType

import kombu.transport.virtual
from kombu import Connection
from kombu.transport import rocketmq
from kombu.transport.rocketmq import (ACK_ENDPOINTS_KEY, ACK_HANDLE_KEY,
                                      ACK_MESSAGE_ID_KEY, ACK_TOPIC_KEY,
                                      AMQP_QUEUE_KEY, DEFAULT_GROUP_FMT,
                                      DELIVERY_ATTEMPT_KEY,
                                      GLOBAL_PRODUCER_KEY, MAX_BATCH_COUNT,
                                      ConsumerConfig, _ack_rocketmq_message,
                                      _message_to_rocketmq_ack_message,
                                      _queue_to_topic)
from kombu.utils.encoding import str_to_bytes
from kombu.utils.json import dumps
from kombu.utils.uuid import uuid

AK = 'ak'
SK = 'sk'
HOST = 'localhost'
PORT = '8080'


@pytest.fixture(autouse=True)
def mock_fixture(request):
    with patch('kombu.transport.rocketmq.SimpleConsumer') as consumer_cls, \
            patch('kombu.transport.rocketmq.Producer') as producer_cls:

        consumer = MagicMock(spec=SimpleConsumer)
        consumer_cls.return_value = consumer
        producer = MagicMock(spec=Producer)
        producer_cls.return_value = producer

        request.cls.consumer_cls = consumer_cls
        request.cls.consumer = consumer
        request.cls.producer_cls = producer_cls
        request.cls.producer = producer

        transport_options = getattr(request, 'param', {}).get('transport_options')
        connection = Connection(f'rocketmq://{AK}:{SK}@{HOST}:{PORT}//',
                                transport_options=transport_options)
        channel: kombu.transport.rocketmq.Channel = connection.default_channel
        request.cls.channel = channel
        yield


class test_Qos:
    channel: rocketmq.Channel
    consumer: MagicMock

    def test_qos_can_consume(self):
        qos = self.channel.qos
        for _ in range(5):
            qos.append(None, uuid())
        assert qos.prefetch_count == 0
        assert qos.can_consume()

        qos.prefetch_count = 1
        assert not qos.can_consume()

    def test_qos_can_consume_max_estimate(self):
        un_ack_num = 5
        qos = self.channel.qos
        for _ in range(un_ack_num):
            qos.append(None, uuid())
        assert qos.prefetch_count == 0
        assert MAX_BATCH_COUNT == qos.can_consume_max_estimate()

        qos.prefetch_count = 1
        assert 1 == qos.can_consume_max_estimate()  # at least one

        qos.prefetch_count = 10
        assert 10 - un_ack_num == qos.can_consume_max_estimate()

        qos.prefetch_count = 100
        assert MAX_BATCH_COUNT == qos.can_consume_max_estimate()

    def test_qos_ack(self):
        queue = 'new-queue'
        self.channel._get_consumer(queue)
        qos = self.channel.qos
        message = _mock_message(topic=queue)
        tag = uuid()
        qos.append(message, tag)
        qos.ack(tag)
        qos.ack('tag_404')

        self.consumer.ack.assert_called_once()
        assert not qos._not_yet_acked

    @pytest.mark.parametrize('mock_fixture', [{
        'transport_options': {
            'backoff_policy': [1, 2, 3, 4, 5, 6]
        }},
    ], indirect=True)
    def test_qos_reject(self):
        queue = 'new-queue'
        self.channel._get_consumer(queue)
        qos = self.channel.qos
        message = _mock_message(topic=queue, attempts='10', queue=queue)
        tag = uuid()
        qos.append(message, tag)
        qos.reject(tag, requeue=False)
        self.consumer.ack.assert_called_once()

        qos.append(message, tag)
        qos.reject(tag, requeue=True)
        self.consumer.change_invisible_duration_async.assert_called_once()
        call_args, call_kwargs = self.consumer.change_invisible_duration_async.call_args
        next_invisible_time = call_args[1]
        assert next_invisible_time == 6

    def test_qos_discard_message(self):
        qos = self.channel.qos
        qos.append(_mock_message(topic='queue1', queue='queue1'), uuid())
        qos.append(_mock_message(topic='queue1', queue='queue1'), uuid())
        qos.append(_mock_message(topic='queue2', queue='queue2'), uuid())
        assert len(qos._not_yet_acked) == 3
        qos.discard_message('queue1')
        assert len(qos._not_yet_acked) == 1
        qos.restore_unacked_once()
        assert not qos._not_yet_acked


class test_Channel:

    channel: rocketmq.Channel
    # consumer: SimpleConsumer
    consumer: MagicMock
    producer: MagicMock

    @pytest.mark.filterwarnings("ignore::BytesWarning")
    def test_get(self):
        queue = 'new-queue'
        self.channel.basic_consume(queue, True, None, 'cg1')

        self.consumer.receive.return_value = []
        with pytest.raises(Empty):
            self.channel._get(queue)

        rocketmq_message = _message_to_rocketmq_ack_message(_mock_message(topic=queue, queue=queue))
        rocketmq_message.body = str_to_bytes(dumps(self.channel.prepare_message({})))
        self.consumer.receive.return_value = [rocketmq_message]

        self.channel._get(queue)
        self.consumer.ack.assert_called_once()  # auto ack

    @pytest.mark.filterwarnings("ignore::BytesWarning")
    def test_get_bulk(self):

        def callback(message):
            message.ack()
        queue = 'new-queue'
        self.channel.basic_consume(queue, False, callback, 'cg1')
        rocketmq_message = _message_to_rocketmq_ack_message(_mock_message(topic=queue, queue=queue))
        rocketmq_message.body = str_to_bytes(dumps(self.channel.prepare_message({}, properties={
            'delivery_tag': uuid()
        })))
        self.consumer.receive.return_value = [rocketmq_message, rocketmq_message]
        self.channel._get_bulk(queue)

        assert self.consumer.ack.call_count == 2
        assert not self.channel.qos._not_yet_acked

    @pytest.mark.filterwarnings("ignore::BytesWarning")
    def test_put_fanout(self):
        self.channel.exchange_declare('exchange', 'fanout')
        self.channel.queue_declare('queue1')
        self.channel.queue_declare('queue2')
        self.channel.queue_bind('queue1', 'exchange', 'rk1')
        self.channel.queue_bind('queue2', 'exchange', 'rk2')
        message = {'h': 'w'}
        self.channel._put_fanout('exchange', message, 'whatever')

        assert 2 == self.producer.send.call_count

    def test_basic_consume_normal(self):
        queue1 = 'new-queue'
        queue2 = 'new-queue2'
        self.channel.basic_consume(queue1, False, None, 'cg1')
        self.channel.basic_consume(queue2, True, None, 'cg2')

        assert len(self.channel.connection._callbacks) == 2
        assert [queue1, queue2] == self.channel._active_queues
        assert [queue1, queue2] == list(self.channel.connection._topic_to_queue.keys())
        assert [queue1, queue2] == list(self.channel.connection._topic_to_queue.values())
        assert {queue2} == self.channel._auto_ack_topics
        assert not self.channel._rocketmq_consumers  # lazy init

    def test_basic_consume_duplicate(self):
        queue1 = 'new-queue'
        queue2 = 'new-queue'
        self.channel.basic_consume(queue1, False, None, 'cg1')
        with pytest.raises(Exception):
            self.channel.basic_consume(queue2, True, None, 'cg2')

    def test_basic_consume_queue_transform(self):
        queue = 'new.queue'
        queue_t = 'new-queue'
        self.channel.basic_consume(queue, False, None, 'cg1')

        assert self.channel.connection._callbacks[queue] \
            == self.channel.connection._callbacks[queue_t]
        assert [queue] == self.channel._active_queues
        assert queue == self.channel.connection._topic_to_queue[queue_t]

    def test_basic_cancel(self):
        queue = 'new-queue'
        self.channel.basic_consume(queue, True, None, 'cg')
        self.channel._get_consumer(queue)
        assert [queue] == self.channel._active_queues
        assert {queue} == self.channel._auto_ack_topics
        assert queue == self.channel.connection._topic_to_queue[queue]
        assert [queue] == list(self.channel._group_config.keys())
        self.channel.basic_cancel('cg')

        assert not self.channel._active_queues
        assert not self.channel._auto_ack_topics
        assert not self.channel.connection._topic_to_queue
        self.consumer.unsubscribe.assert_called_once_with(queue)

    def test_get_producer_passive(self):
        queue = 'new-queue'
        rst = self.channel._get_producer(queue, True)
        assert not rst

    @pytest.mark.parametrize('mock_fixture', [{
        'transport_options': {
            'request_timeout': 5,
            'tls_enable': True,
            'max_send_attempts': 10
        }},
    ], indirect=True)
    def test_get_producer_not_passive_global_producer(self):
        queue = 'new-queue'
        rst1 = self.channel._get_producer(queue)
        assert rst1 is self.producer

        rst2 = self.channel._get_producer(queue)
        assert rst1 is rst2

        call_args, call_kwargs = self.producer_cls.call_args
        client_config = cast(ClientConfiguration, call_args[0])
        topics = call_args[1]
        tls_enable = call_kwargs['tls_enable']

        assert client_config.request_timeout == 5
        assert client_config.credentials.ak == AK
        assert client_config.credentials.sk == SK
        assert queue in topics
        assert tls_enable
        assert self.producer.MAX_SEND_ATTEMPTS == 10
        assert {GLOBAL_PRODUCER_KEY} == self.channel._rocketmq_producers.keys()
        self.producer.startup.assert_called_once()

    @pytest.mark.parametrize('mock_fixture', [{
        'transport_options': {
            'separate_queue_producer': True
        }},
    ], indirect=True)
    def test_get_producer_not_passive_separate_producer(self):
        queue1 = 'new-queue'
        rst1 = self.channel._get_producer(queue1)
        assert rst1 is self.producer

        queue2 = 'new-queue-2'
        rst2 = self.channel._get_producer(queue2)
        assert rst1 is rst2  # equals only in test
        assert self.producer.startup.call_count == 2
        assert {queue1, queue2} == self.channel._rocketmq_producers.keys()

    def test_get_consumer_passive(self):
        queue = 'new-queue'
        rst = self.channel._get_consumer(queue, True)
        assert not rst

    @pytest.mark.parametrize('mock_fixture', [{
        'transport_options': {
            'invisible_time': 200,
            'await_duration': 15,
            'request_timeout': 6,
            'tls_enable': True
        }},
    ], indirect=True)
    def test_get_consumer_not_passive(self):
        queue = 'new-queue'
        rst1 = self.channel._get_consumer(queue)
        assert rst1 is self.consumer

        rst2 = self.channel._get_consumer(queue)
        assert rst1 is rst2

        call_args, call_kwargs = self.consumer_cls.call_args
        client_config = cast(ClientConfiguration, call_args[0])
        group = call_args[1]
        await_duration = call_kwargs['await_duration']
        tls_enable = call_kwargs['tls_enable']

        assert client_config.request_timeout == 6
        assert client_config.credentials.ak == AK
        assert client_config.credentials.sk == SK
        assert group == DEFAULT_GROUP_FMT.format(queue)
        assert await_duration == 15
        assert tls_enable
        self.consumer.startup.assert_called_once()
        self.consumer.subscribe.assert_called_once()

    def test_get_consumer_config_use_default(self):
        topic = 'new-queue'
        config = self.channel._get_consumer_config(topic)
        assert config.consumer_group == DEFAULT_GROUP_FMT.format(topic)
        assert config.filter_type == FilterType.TAG
        assert config.filter_exp == FilterExpression.TAG_EXPRESSION_SUB_ALL

    def test_get_consumer_config_use_cache(self):
        topic = 'new-queue'
        cache_config = ConsumerConfig(consumer_group='cache-group')
        self.channel._group_config[topic] = cache_config

        config = self.channel._get_consumer_config(topic)
        assert config.consumer_group == 'cache-group'
        assert config.filter_exp == FilterExpression.TAG_EXPRESSION_SUB_ALL

    @pytest.mark.parametrize('mock_fixture', [{
        'transport_options': {
            'consumer_options': {
                'group_format': 'AMQP-{}',
                'global_group_id': 'CID-RocketMQ'
            }
        }},
    ], indirect=True)
    def test_get_consumer_config_use_global_id(self):
        topic = 'new-queue'
        config = self.channel._get_consumer_config(topic)
        assert config.consumer_group == 'CID-RocketMQ'

    @pytest.mark.parametrize('mock_fixture', [{
        'transport_options': {
            'consumer_options': {
                'group_format': 'AMQP-{}-GROUP',
            }
        }},
    ], indirect=True)
    def test_get_consumer_config_use_group_format(self):
        topic = 'new-queue'
        config = self.channel._get_consumer_config(topic)
        assert config.consumer_group == 'AMQP-new-queue-GROUP'

    @pytest.mark.parametrize('mock_fixture', [{
        'transport_options': {
            'consumer_options': {
                'group_format': 'AMQP-{}-GROUP',
                'topic_config': {
                    'new-queue': {
                        'group_id': 'CID_ABC', 'filter_exp': 'TAG_ABC'
                    }
                }
            }
        }},
    ], indirect=True)
    def test_get_consumer_config_use_topic_config(self):
        topic = 'new-queue'
        config = self.channel._get_consumer_config(topic)
        assert config.consumer_group == 'CID_ABC'
        assert config.filter_exp == 'TAG_ABC'

    def test_queue_to_topic(self):
        queue = 'new-123_-_queue'
        assert queue == _queue_to_topic(queue)

        queue = 'another{|%.%|}queue'
        assert _queue_to_topic(queue) == 'another-------queue'

    def test_message_to_rocketmq_ack_message(self):
        message = _mock_message()
        ack_message = _message_to_rocketmq_ack_message(message)
        assert ack_message.endpoints == 'localhost:8080'
        assert ack_message.properties[DELIVERY_ATTEMPT_KEY] == '3'

    def test_ack_rocketmq_message_no_raise(self):
        assert isinstance(self.consumer.ack, Mock)
        message = Mock()

        self.consumer.ack.side_effect = [None]
        _ack_rocketmq_message(self.consumer, message, True)
        assert self.consumer.ack.call_count == 1

        self.consumer.ack.side_effect = [Exception, None]
        _ack_rocketmq_message(self.consumer, message, True)
        assert self.consumer.ack.call_count == 1 + 2

    def test_ack_rocketmq_message_raise(self):
        assert isinstance(self.consumer.ack, Mock)
        message = Mock()
        self.consumer.ack.side_effect = [Exception] * 3

        with pytest.raises(Exception):
            _ack_rocketmq_message(self.consumer, message, raise_exception=True, max_retries=2)
        assert self.consumer.ack.call_count == 3

        _ack_rocketmq_message(self.consumer, message, raise_exception=False, max_retries=2)
        assert self.consumer.ack.call_count == 6


def _mock_message(endpoint='localhost:8080', msg_id='mid',
                  topic='queue', handle='handle', attempts: str = '3', queue='queue'):
    message = Mock()
    message.delivery_info = {
        ACK_ENDPOINTS_KEY: endpoint,
        ACK_MESSAGE_ID_KEY: msg_id,
        ACK_TOPIC_KEY: topic,
        ACK_HANDLE_KEY: handle,
        DELIVERY_ATTEMPT_KEY: attempts,
        AMQP_QUEUE_KEY: queue
    }
    return message
