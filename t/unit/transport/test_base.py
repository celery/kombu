import pytest

from case import Mock

from kombu import Connection, Consumer, Exchange, Producer, Queue
from kombu.message import Message
from kombu.transport.base import (
    StdChannel, Transport, Management, to_rabbitmq_queue_arguments,
)


@pytest.mark.parametrize('args,input,expected', [
    ({}, {'message_ttl': 20}, {'x-message-ttl': 20000}),
    ({}, {'message_ttl': None}, {}),
    ({'foo': 'bar'}, {'expires': 30.3}, {'x-expires': 30300, 'foo': 'bar'}),
    ({'x-expires': 3}, {'expires': 4}, {'x-expires': 4000}),
    ({}, {'max_length': 10}, {'x-max-length': 10}),
    ({}, {'max_length_bytes': 1033}, {'x-max-length-bytes': 1033}),
    ({}, {'max_priority': 303}, {'x-max-priority': 303}),
])
def test_rabbitmq_queue_arguments(args, input, expected):
    assert to_rabbitmq_queue_arguments(args, **input) == expected


class test_StdChannel:

    def setup(self):
        self.conn = Connection('memory://')
        self.channel = self.conn.channel()
        self.channel.queues.clear()
        self.conn.connection.state.clear()

    def test_Consumer(self):
        q = Queue('foo', Exchange('foo'))
        cons = self.channel.Consumer(q)
        assert isinstance(cons, Consumer)
        assert cons.channel is self.channel

    def test_Producer(self):
        prod = self.channel.Producer()
        assert isinstance(prod, Producer)
        assert prod.channel is self.channel

    def test_interface_get_bindings(self):
        with pytest.raises(NotImplementedError):
            StdChannel().get_bindings()

    def test_interface_after_reply_message_received(self):
        assert StdChannel().after_reply_message_received(Queue('foo')) is None


class test_Message:

    def setup(self):
        self.conn = Connection('memory://')
        self.channel = self.conn.channel()
        self.message = Message(channel=self.channel, delivery_tag=313)

    def test_postencode(self):
        m = Message('FOO', channel=self.channel, postencode='ccyzz')
        with pytest.raises(LookupError):
            m._reraise_error()
        m.ack()

    def test_ack_respects_no_ack_consumers(self):
        self.channel.no_ack_consumers = {'abc'}
        self.message.delivery_info['consumer_tag'] = 'abc'
        ack = self.channel.basic_ack = Mock()

        self.message.ack()
        assert self.message._state != 'ACK'
        ack.assert_not_called()

    def test_ack_missing_consumer_tag(self):
        self.channel.no_ack_consumers = {'abc'}
        self.message.delivery_info = {}
        ack = self.channel.basic_ack = Mock()

        self.message.ack()
        ack.assert_called_with(self.message.delivery_tag, multiple=False)

    def test_ack_not_no_ack(self):
        self.channel.no_ack_consumers = set()
        self.message.delivery_info['consumer_tag'] = 'abc'
        ack = self.channel.basic_ack = Mock()

        self.message.ack()
        ack.assert_called_with(self.message.delivery_tag, multiple=False)

    def test_ack_log_error_when_no_error(self):
        ack = self.message.ack = Mock()
        self.message.ack_log_error(Mock(), KeyError)
        ack.assert_called_with(multiple=False)

    def test_ack_log_error_when_error(self):
        ack = self.message.ack = Mock()
        ack.side_effect = KeyError('foo')
        logger = Mock()
        self.message.ack_log_error(logger, KeyError)
        ack.assert_called_with(multiple=False)
        logger.critical.assert_called()
        assert "Couldn't ack" in logger.critical.call_args[0][0]

    def test_reject_log_error_when_no_error(self):
        reject = self.message.reject = Mock()
        self.message.reject_log_error(Mock(), KeyError, requeue=True)
        reject.assert_called_with(requeue=True)

    def test_reject_log_error_when_error(self):
        reject = self.message.reject = Mock()
        reject.side_effect = KeyError('foo')
        logger = Mock()
        self.message.reject_log_error(logger, KeyError)
        reject.assert_called_with(requeue=False)
        logger.critical.assert_called()
        assert "Couldn't reject" in logger.critical.call_args[0][0]


class test_interface:

    def test_establish_connection(self):
        with pytest.raises(NotImplementedError):
            Transport(None).establish_connection()

    def test_close_connection(self):
        with pytest.raises(NotImplementedError):
            Transport(None).close_connection(None)

    def test_create_channel(self):
        with pytest.raises(NotImplementedError):
            Transport(None).create_channel(None)

    def test_close_channel(self):
        with pytest.raises(NotImplementedError):
            Transport(None).close_channel(None)

    def test_drain_events(self):
        with pytest.raises(NotImplementedError):
            Transport(None).drain_events(None)

    def test_heartbeat_check(self):
        Transport(None).heartbeat_check(Mock(name='connection'))

    def test_driver_version(self):
        assert Transport(None).driver_version()

    def test_register_with_event_loop(self):
        Transport(None).register_with_event_loop(
            Mock(name='connection'), Mock(name='loop'),
        )

    def test_unregister_from_event_loop(self):
        Transport(None).unregister_from_event_loop(
            Mock(name='connection'), Mock(name='loop'),
        )

    def test_manager(self):
        assert Transport(None).manager


class test_Management:

    def test_get_bindings(self):
        m = Management(Mock(name='transport'))
        with pytest.raises(NotImplementedError):
            m.get_bindings()
