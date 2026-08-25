from __future__ import annotations

import sys
from itertools import count
from unittest.mock import MagicMock, Mock, patch

import pytest

from kombu import Connection
from kombu.transport import pyamqp


def test_amqps_connection():
    conn = Connection('amqps://')
    assert conn.transport  # evaluate transport, don't connect
    assert conn.ssl


class MockConnection(dict):

    def __setattr__(self, key, value):
        self[key] = value

    def connect(self):
        pass


class test_Channel:

    def setup_method(self):

        class Channel(pyamqp.Channel):
            wait_returns = []

            def _x_open(self, *args, **kwargs):
                pass

            def wait(self, *args, **kwargs):
                return self.wait_returns

            def _send_method(self, *args, **kwargs):
                pass

        self.conn = Mock()
        self.conn._get_free_channel_id.side_effect = count(0).__next__
        self.conn.channels = {}
        self.channel = Channel(self.conn, 0)

    def test_init(self):
        assert not self.channel.no_ack_consumers

    def test_prepare_message(self):
        assert self.channel.prepare_message(
            'foobar', 10, 'application/data', 'utf-8',
            properties={},
        )

    def test_message_to_python(self):
        message = Mock()
        message.headers = {}
        message.properties = {}
        assert self.channel.message_to_python(message)

    def test_close_resolves_connection_cycle(self):
        assert self.channel.connection is not None
        self.channel.close()
        assert self.channel.connection is None

    def test_basic_consume_registers_ack_status(self):
        self.channel.wait_returns = ['my-consumer-tag']
        self.channel.basic_consume('foo', no_ack=True)
        assert 'my-consumer-tag' in self.channel.no_ack_consumers

        self.channel.wait_returns = ['other-consumer-tag']
        self.channel.basic_consume('bar', no_ack=False)
        assert 'other-consumer-tag' not in self.channel.no_ack_consumers

        self.channel.basic_cancel('my-consumer-tag')
        assert 'my-consumer-tag' not in self.channel.no_ack_consumers

    def test_consume_registers_cancel_callback(self):
        on_cancel = Mock()
        self.channel.wait_returns = ['my-consumer-tag']
        self.channel.basic_consume('foo', on_cancel=on_cancel)
        assert self.channel.cancel_callbacks['my-consumer-tag'] == on_cancel


class test_Transport:

    def setup_method(self):
        self.connection = Connection('pyamqp://')
        self.transport = self.connection.transport

    def test_create_channel(self):
        connection = Mock()
        self.transport.create_channel(connection)
        connection.channel.assert_called_with()

    def test_ssl_cert_passed(self):
        ssl_dict = {
            'ca_certs': '/etc/pki/tls/certs/something.crt',
            'cert_reqs': "ssl.CERT_REQUIRED",
        }
        ssl_dict_copy = {k: ssl_dict[k] for k in ssl_dict}
        connection = Connection('amqps://', ssl=ssl_dict_copy)
        assert connection.transport.client.ssl == ssl_dict

    def test_driver_version(self):
        assert self.transport.driver_version()

    def test_drain_events(self):
        connection = Mock()
        self.transport.drain_events(connection, timeout=10.0)
        connection.drain_events.assert_called_with(timeout=10.0)

    def test_dnspython_localhost_resolve_bug(self):

        class Conn:

            def __init__(self, **kwargs):
                vars(self).update(kwargs)

            def connect(self):
                pass

        self.transport.Connection = Conn
        self.transport.client.hostname = 'localhost'
        conn1 = self.transport.establish_connection()
        assert conn1.host == '127.0.0.1:5672'

        self.transport.client.hostname = 'example.com'
        conn2 = self.transport.establish_connection()
        assert conn2.host == 'example.com:5672'

    def test_close_connection(self):
        connection = Mock()
        connection.client = Mock()
        self.transport.close_connection(connection)

        assert connection.client is None
        connection.close.assert_called_with()

    @pytest.mark.masked_modules('ssl')
    def test_import_no_ssl(self, mask_modules):
        pm = sys.modules.pop('amqp.connection')
        try:
            from amqp.connection import SSLError
            assert SSLError.__module__ == 'amqp.connection'
        finally:
            if pm is not None:
                sys.modules['amqp.connection'] = pm


class test_pyamqp:

    def test_default_port(self):

        class Transport(pyamqp.Transport):
            Connection = MockConnection

        c = Connection(port=None, transport=Transport).connect()
        assert c['host'] == f'127.0.0.1:{Transport.default_port}'

    def test_custom_port(self):

        class Transport(pyamqp.Transport):
            Connection = MockConnection

        c = Connection(port=1337, transport=Transport).connect()
        assert c['host'] == '127.0.0.1:1337'

    def test_ssl(self):
        # Test setting TLS by ssl=True.
        class Transport(pyamqp.Transport):
            Connection = MagicMock()

        Connection(transport=Transport, ssl=True).connect()
        Transport.Connection.assert_called_once()
        _, kwargs = Transport.Connection.call_args
        assert kwargs['ssl'] is True

    def test_ssl_dict(self):
        # Test setting TLS by setting ssl as dict.
        class Transport(pyamqp.Transport):
            Connection = MagicMock()

        Connection(transport=Transport, ssl={'a': 1, 'b': 2}).connect()
        Transport.Connection.assert_called_once()
        _, kwargs = Transport.Connection.call_args
        assert kwargs['ssl'] == {'a': 1, 'b': 2}

    @pytest.mark.parametrize(
        'hostname',
        [
            'broker.example.com',
            'amqp://broker.example.com/0',
            'amqps://broker.example.com/0',
            'amqp://guest:guest@broker.example.com/0',
            'amqp://broker.example.com;broker2.example.com'
        ])
    def test_ssl_server_hostname(self, hostname):
        # Test setting server_hostname from URI.
        class Transport(pyamqp.Transport):
            Connection = MagicMock()

        Connection(
            hostname, transport=Transport, ssl={'server_hostname': None}
        ).connect()
        Transport.Connection.assert_called_once()
        _, kwargs = Transport.Connection.call_args
        assert kwargs['ssl'] == {'server_hostname': 'broker.example.com'}

    def test_register_with_event_loop(self):
        t = pyamqp.Transport(Mock())
        conn = Mock(name='conn')
        loop = Mock(name='loop')
        t.register_with_event_loop(conn, loop)
        loop.add_reader.assert_called_with(
            conn.sock, t.on_readable, conn, loop,
        )

    def test_heartbeat_check(self):
        t = pyamqp.Transport(Mock())
        conn = Mock()
        t.heartbeat_check(conn, rate=4.331)
        conn.heartbeat_tick.assert_called_with(rate=4.331)

    def test_get_manager(self):
        with patch('kombu.transport.pyamqp.get_manager') as get_manager:
            t = pyamqp.Transport(Mock())
            t.get_manager(1, kw=2)
            get_manager.assert_called_with(t.client, 1, kw=2)

    def test_qos_semantics_matches_spec(self):
        t = pyamqp.Transport(Mock())
        conn = Mock()

        # Non-RabbitMQ broker: always True
        conn.server_properties = {'product': 'ActiveMQ', 'version': '5.0.0'}
        assert t.qos_semantics_matches_spec(conn) is True

        # RabbitMQ < 3.3: per-channel QoS (True)
        conn.server_properties = {'product': 'RabbitMQ', 'version': '3.2.4'}
        assert t.qos_semantics_matches_spec(conn) is True

        # RabbitMQ 3.3: global QoS semantics introduced (False)
        conn.server_properties = {'product': 'RabbitMQ', 'version': '3.3.0'}
        assert t.qos_semantics_matches_spec(conn) is False

        # RabbitMQ 3.13: still global QoS (False)
        conn.server_properties = {'product': 'RabbitMQ', 'version': '3.13.0'}
        assert t.qos_semantics_matches_spec(conn) is False

        # RabbitMQ 4.0: global QoS removed on classic queues (True)
        conn.server_properties = {'product': 'RabbitMQ', 'version': '4.0.0'}
        assert t.qos_semantics_matches_spec(conn) is True

        # RabbitMQ 4.x future version: still True
        conn.server_properties = {'product': 'RabbitMQ', 'version': '4.2.1'}
        assert t.qos_semantics_matches_spec(conn) is True

        # RabbitMQ with missing 'version' key: returns True (safe default)
        conn.server_properties = {'product': 'RabbitMQ'}
        assert t.qos_semantics_matches_spec(conn) is True

    def test_establish_connection_callable_password(self):
        """Callable password is invoked and resolved string passed to
        amqp.Connection."""
        class Transport(pyamqp.Transport):
            Connection = MagicMock()

        password_func = Mock(return_value='fresh_token')
        conn = Connection(
            'amqp://user@localhost/',
            password=password_func,
            transport=Transport,
        )
        conn.connect()
        password_func.assert_called_once()
        _, kwargs = Transport.Connection.call_args
        assert kwargs['password'] == 'fresh_token'

    def test_establish_connection_string_password_unchanged(self):
        """Static string password still works identically."""
        class Transport(pyamqp.Transport):
            Connection = MagicMock()

        conn = Connection(
            'amqp://user@localhost/',
            password='static_pass',
            transport=Transport,
        )
        conn.connect()
        _, kwargs = Transport.Connection.call_args
        assert kwargs['password'] == 'static_pass'

    def test_reconnection_invokes_callable_each_time(self):
        """Each reconnection invokes the callable for a fresh password."""
        call_count = 0

        def password_func():
            nonlocal call_count
            call_count += 1
            return f'token_{call_count}'

        class Transport(pyamqp.Transport):
            Connection = MockConnection

        conn = Connection(
            'amqp://user@localhost/',
            password=password_func,
            transport=Transport,
        )
        # First connection
        c1 = conn.connect()
        assert c1['password'] == 'token_1'
        assert call_count == 1

        # Simulate disconnect + reconnect
        conn.close()
        c2 = conn.connect()
        assert c2['password'] == 'token_2'
        assert call_count == 2
