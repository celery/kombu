"""Unit tests for the Kombu Apache Qpid Proton transport."""

from __future__ import annotations

import queue
import socket
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from kombu import Connection
from kombu.exceptions import OperationalError

import kombu.transport.proton as module
from kombu.transport.proton import (
    AuthenticationFailure,
    Channel,
    Connection as ProtonConnection,
    Message,
    QoS,
    Transport,
    _Command,
    _ManagementReceiverOption,
    _ManagementSenderOption,
    _ProtonHandler,
    _ProtonState,
    _RabbitMQManagement,
    _Received,
)


class FakeProtonMessage:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.body = kwargs.get("body")
        self.properties = kwargs.get("properties", {})
        self.content_type = kwargs.get("content_type")
        self.content_encoding = kwargs.get("content_encoding")
        self.priority = kwargs.get("priority")
        self.subject = kwargs.get("subject")


class FakeInjector:
    def __init__(self):
        self.events = []

    def trigger(self, event):
        self.events.append(event)


class FakeContainer:
    def __init__(self):
        self.connect_calls = []
        self.sender_calls = []
        self.receiver_calls = []
        self.work_calls = []
        self.stopped = False

    def connect(self, *args, **kwargs):
        self.connect_calls.append((args, kwargs))
        return self.connection

    def selectable(self, injector):
        self.injector = injector

    def create_sender(self, *args, **kwargs):
        self.sender_calls.append((args, kwargs))
        sender = FakeSender()
        return sender

    def create_receiver(self, *args, **kwargs):
        self.receiver_calls.append((args, kwargs))
        receiver = FakeReceiver(
            name=kwargs.get("name"),
            source=kwargs.get("source"),
        )
        return receiver

    def do_work(self, timeout):
        self.work_calls.append(timeout)

    def stop(self):
        self.stopped = True


class FakeSender:
    def __init__(self, credit=1):
        self.credit = credit
        self.messages = []
        self.send_result = None

    def send(self, message):
        self.messages.append(message)
        return self.send_result


class FakeReceiver:
    def __init__(self, name=None, source=None):
        self.name = name
        self.source = source
        self.flow_calls = []
        self.closed = False

    def flow(self, credit):
        self.flow_calls.append(credit)

    def close(self):
        self.closed = True


class FakeDelivery:
    def __init__(self):
        self.accepted = False
        self.rejected = False
        self.released = False
        self.settled = False

    def accept(self):
        self.accepted = True

    def reject(self):
        self.rejected = True

    def release(self):
        self.released = True

    def settle(self):
        self.settled = True


def make_client(**options):
    """Build a Kombu client suitable for transport unit tests."""
    return Connection(
        hostname=options.pop("hostname", "localhost"),
        port=options.pop("port", 5672),
        userid=options.pop("userid", "guest"),
        password=options.pop("password", "guest"),
        virtual_host=options.pop("virtual_host", "/"),
        ssl=options.pop("ssl", False),
        heartbeat=options.pop("heartbeat", 0),
        transport_options=options.pop("transport_options", {}),
        transport="kombu.transport.proton:Transport",
        **options,
    )


@pytest.fixture
def client():
    return make_client()


@pytest.fixture
def transport(client):
    return Transport(client)


@pytest.fixture
def state():
    state = _ProtonState(
        "amqp://localhost:5672",
        {},
        None,
        command_timeout=0.05,
    )
    yield state
    state.close()


def make_channel(transport=None, client=None):
    client = client or make_client()
    transport = transport or Transport(client)
    state = SimpleNamespace(transport=transport)
    connection = ProtonConnection(state, client)
    return Channel(connection, transport), connection, state


class TestTransport:
    def test_driver_name(self, transport):
        assert transport.driver_name == "qpid-proton"
        assert transport.driver_type == "amqp"

    def test_driver_version(self, transport):
        with patch.object(module, "version", return_value="0.40.0"):
            assert transport.driver_version() == "0.40.0"

    def test_default_connection_params(self, transport):
        assert transport.default_connection_params == {
            "hostname": "localhost",
            "port": 5672,
        }

    def test_ssl_default_port(self):
        client = make_client(ssl=True)
        transport = Transport(client)
        assert transport.default_connection_params["port"] == 5671

    def test_address_template_compatibility(self):
        client = make_client(
            transport_options={"address_template": "{exchange}/{routing_key}"}
        )
        transport = Transport(client)
        assert transport.address_template == "{exchange}/{routing_key}"

    def test_transport_options(self):
        client = make_client(
            transport_options={
                "command_timeout": 3,
                "prefetch_count": 7,
                "address_template": "{routing_key}",
                "sasl_enabled": False,
                "custom": "value",
            }
        )
        transport = Transport(client)
        assert transport.command_timeout == 3
        assert transport.prefetch_count == 7
        assert transport.address_template == "{routing_key}"
        assert transport.transport_options == {
            "sasl_enabled": False,
            "custom": "value",
        }

    def test_url(self):
        client = make_client(hostname="broker", port=5678)
        assert Transport(client)._url() == "amqp://broker:5678"

        client = make_client(hostname="broker", ssl=True)
        assert Transport(client)._url() == "amqps://broker:5671"

    def test_ssl_domain_boolean(self):
        client = make_client(ssl=True)
        transport = Transport(client)
        domain = Mock()
        ssl_cls = Mock(return_value=domain)
        ssl_cls.MODE_CLIENT = "client"
        ssl_cls.VERIFY_PEER_NAME = "verify"
        with patch.object(module, "SSLDomain", ssl_cls):
            result = transport._ssl_domain()
        assert result is domain
        ssl_cls.assert_called_once_with("client")
        domain.set_peer_authentication.assert_called_once_with("verify")

    def test_ssl_domain_options(self):
        client = make_client(
            ssl={
                "ca_certs": "/ca.pem",
                "certfile": "/cert.pem",
                "keyfile": "/key.pem",
                "password": "secret",
                "cert_reqs": False,
            }
        )
        transport = Transport(client)
        domain = Mock()
        ssl_cls = Mock(return_value=domain)
        ssl_cls.MODE_CLIENT = "client"
        ssl_cls.ANONYMOUS_PEER = "anonymous"
        ssl_cls.VERIFY_PEER_NAME = "verify"
        with patch.object(module, "SSLDomain", ssl_cls):
            transport._ssl_domain()
        domain.set_credentials.assert_called_once_with(
            "/cert.pem", "/key.pem", "secret"
        )
        domain.set_trusted_ca_db.assert_called_once_with("/ca.pem")
        domain.set_peer_authentication.assert_called_once_with("anonymous")

    def test_establish_connection_builds_state(self):
        client = make_client(
            userid="alice",
            password="secret",
            virtual_host="/vhost",
            transport_options={"sasl_enabled": False},
        )
        transport = Transport(client)
        fake_state = Mock()
        with patch.object(module, "_ProtonState", return_value=fake_state) as state_cls:
            connection = transport.establish_connection()
        state_cls.assert_called_once_with(
            url="amqp://localhost:5672",
            connect_kwargs={
                "user": "alice",
                "password": "secret",
                "virtual_host": "/vhost",
                "sasl_enabled": False,
            },
            ssl_domain=None,
            command_timeout=10.0,
        )
        assert fake_state.transport is transport
        fake_state.start.assert_called_once_with()
        assert isinstance(connection, ProtonConnection)

    def test_establish_connection_callable_password(self):
        password = Mock(return_value="dynamic")
        client = make_client(password=password)
        transport = Transport(client)
        fake_state = Mock()
        with patch.object(module, "_ProtonState", return_value=fake_state):
            transport.establish_connection()
        assert password.call_count == 1
        assert fake_state.start.call_count == 1
        assert fake_state.connect_kwargs == {  # attribute access is Mock-compatible
            "user": "guest",
            "password": "dynamic",
            "virtual_host": "/",
            "sasl_enabled": True,
        }

    def test_login_method_sets_allowed_mechs(self):
        client = make_client()
        client.login_method = "PLAIN"
        transport = Transport(client)
        fake_state = Mock()
        with patch.object(module, "_ProtonState", return_value=fake_state):
            transport.establish_connection()
        assert fake_state.connect_kwargs["allowed_mechs"] == "PLAIN"

    def test_create_and_close_channel(self, transport):
        state = SimpleNamespace(transport=transport, close=Mock())
        connection = ProtonConnection(state, transport.client)
        channel = transport.create_channel(connection)
        assert channel in connection.channels
        transport.close_connection(connection)
        assert connection.connected is False

    def test_verify_connection(self, transport):
        state = SimpleNamespace(error=None)
        connection = SimpleNamespace(state=state, connected=True)
        assert transport.verify_connection(connection) is True
        state.error = OperationalError("boom")
        assert transport.verify_connection(connection) is False
        connection.connected = False
        assert transport.verify_connection(connection) is False

    def test_heartbeat_helpers(self, transport):
        connection = Mock()
        assert transport.get_heartbeat_interval(connection) == 0
        assert transport.heartbeat_check(connection) is None
        assert transport.qos_semantics_matches_spec(connection) is True

    def test_register_unregister_event_loop(self, transport):
        connection = Mock()
        loop = Mock()
        transport.register_with_event_loop(connection, loop)
        loop.add_reader.assert_called_once_with(
            connection.state.read_sock,
            transport._on_readable,
            connection,
        )
        transport.unregister_from_event_loop(connection, loop)
        loop.remove_reader.assert_called_once_with(connection.state.read_sock)

    def test_drain_events_timeout(self, transport):
        state = _ProtonState("amqp://localhost:5672", {}, None, command_timeout=0.01)
        state.transport = transport
        connection = ProtonConnection(state, transport.client)
        try:
            with pytest.raises(socket.timeout):
                transport.drain_events(connection, timeout=0)
        finally:
            state.close()


class TestCommandAndMessage:
    def test_command_defaults_and_expected_codes(self):
        command = _Command("management")
        assert command.name == "management"
        assert command.args == ()
        assert command.kwargs == {}
        assert command.expected_codes == ()
        assert isinstance(command.result, queue.Queue)

    def test_received(self):
        received = _Received("q", "m", "d", "tag")
        assert received.queue == "q"
        assert received.message == "m"

    def test_authentication_failure(self):
        error = AuthenticationFailure("bad credentials")
        assert str(error) == "bad credentials"

    def test_message_maps_headers_and_properties(self):
        raw = FakeProtonMessage(
            body="hello",
            properties={"headers": {"x": 1}, "message_id": "abc"},
            content_type="text/plain",
            content_encoding="utf-8",
        )
        message = Message(raw, delivery="delivery", delivery_tag="tag")
        assert message.body == "hello"
        assert message.headers == {"x": 1}
        assert message.properties == {"message_id": "abc"}
        assert message.content_type == "text/plain"
        assert message.content_encoding == "utf-8"
        assert message.proton_message is raw
        assert message.proton_delivery == "delivery"


class TestQoS:
    def test_can_consume(self):
        channel = Mock()
        qos = QoS(channel, 2)
        assert qos.can_consume() is True
        qos.append("d1", "t1")
        assert qos.can_consume() is True
        qos.append("d2", "t2")
        assert qos.can_consume() is False

    def test_unlimited_prefetch(self):
        qos = QoS(Mock(), 0)
        for i in range(20):
            qos.append(i, i)
        assert qos.can_consume() is True
        assert qos.can_consume_max_estimate() == 1

    def test_max_estimate(self):
        qos = QoS(Mock(), 3)
        assert qos.can_consume_max_estimate() == 3
        qos.append("d", "t")
        assert qos.can_consume_max_estimate() == 2
        qos.append("d2", "t2")
        qos.append("d3", "t3")
        assert qos.can_consume_max_estimate() == 0

    def test_unlimited_max_estimate(self):
        assert QoS(Mock(), 0).can_consume_max_estimate() == 1

    def test_get(self):
        qos = QoS(Mock(), 1)
        qos.append("delivery", "tag")
        assert qos.get("tag") == "delivery"

    def test_ack(self):
        channel = Mock()
        qos = QoS(channel, 1)
        delivery = FakeDelivery()
        qos.append(delivery, "tag")
        qos.ack("tag")
        channel._command.assert_called_once_with("accept", delivery)
        assert "tag" not in qos._not_yet_acked

    def test_reject(self):
        channel = Mock()
        qos = QoS(channel, 2)
        d1 = FakeDelivery()
        d2 = FakeDelivery()
        qos.append(d1, "a")
        qos.append(d2, "b")
        qos.reject("a", requeue=False)
        qos.reject("b", requeue=True)
        assert channel._command.call_args_list == [
            call("reject", d1),
            call("release", d2),
        ]

    def test_restore_unacked(self):
        qos = QoS(Mock(), 1)
        qos.append("delivery", "tag")
        qos.restore_unacked()
        assert not qos._not_yet_acked


class TestLinkOptions:
    @pytest.fixture(autouse=True)
    def proton_link_constants(self):
        self.proton_link = SimpleNamespace(
            SND_SETTLED="snd-settled",
            RCV_FIRST="rcv-first",
        )
        self._patch = patch.object(module, "proton", self.proton_link)
        self._patch.start()
        yield
        self._patch.stop()

    def test_sender_option(self):
        link = SimpleNamespace(source=SimpleNamespace(), target=SimpleNamespace())
        _ManagementSenderOption().apply(link)
        assert link.source.address == "/management"
        assert link.snd_settle_mode == "snd-settled"
        assert link.rcv_settle_mode == "rcv-first"
        assert link.properties == {"paired": True}
        assert link.source.dynamic is False

    def test_receiver_option(self):
        link = SimpleNamespace(source=SimpleNamespace(), target=SimpleNamespace())
        _ManagementReceiverOption().apply(link)
        assert link.target.address == "/management"
        assert link.snd_settle_mode == "snd-settled"
        assert link.rcv_settle_mode == "rcv-first"
        assert link.properties == {"paired": True}
        assert link.source.dynamic is False


class TestManagement:
    @pytest.fixture
    def channel(self):
        channel, _, _ = make_channel()
        channel._command = Mock(return_value="response")
        return channel

    def test_request(self, channel):
        management = _RabbitMQManagement(channel)
        result = management.request({}, "/queues/q", "PUT", (201,))
        assert result == "response"
        channel._command.assert_called_once_with(
            "management", {}, "/queues/q", "PUT", (201,)
        )

    def test_declare_queue(self, channel):
        management = _RabbitMQManagement(channel)
        management.declare_queue(
            "q", durable=True, exclusive=True, auto_delete=True,
            arguments={"x": 1},
        )
        channel._command.assert_called_once_with(
            "management",
            {
                "durable": True,
                "exclusive": True,
                "auto_delete": True,
                "arguments": {"x": 1},
            },
            "/queues/q",
            "PUT",
            (200, 201, 204),
        )

    def test_declare_queue_passive(self, channel):
        _RabbitMQManagement(channel).declare_queue("q", passive=True)
        channel._command.assert_called_once_with(
            "management", None, "/queues/q", "GET", (200,)
        )

    def test_delete_queue(self, channel):
        _RabbitMQManagement(channel).delete_queue("q")
        channel._command.assert_called_once_with(
            "management", None, "/queues/q", "DELETE", (200,)
        )

    def test_declare_exchange(self, channel):
        _RabbitMQManagement(channel).declare_exchange(
            "ex", exchange_type="topic", durable=True,
            auto_delete=True, internal=True, arguments={"x": 1},
        )
        channel._command.assert_called_once_with(
            "management",
            {
                "durable": True,
                "type": "topic",
                "auto_delete": True,
                "internal": True,
                "arguments": {"x": 1},
            },
            "/exchanges/ex",
            "PUT",
            (201, 204),
        )

    def test_delete_exchange(self, channel):
        _RabbitMQManagement(channel).delete_exchange("ex")
        channel._command.assert_called_once_with(
            "management", None, "/exchanges/ex", "DELETE", (204,)
        )

    def test_bind(self, channel):
        _RabbitMQManagement(channel).bind("q", "ex", "rk", {"a": 1})
        channel._command.assert_called_once_with(
            "management",
            {
                "source": "ex",
                "destination_queue": "q",
                "binding_key": "rk",
                "arguments": {"a": 1},
            },
            "/bindings",
            "POST",
            (204,),
        )

    def test_bind_none_routing_key(self, channel):
        _RabbitMQManagement(channel).bind("q", "ex", None)
        assert channel._command.call_args.args[-2:] == ("POST", (204,))
        assert channel._command.call_args.args[1]["binding_key"] == ""

    def test_unbind_encodes_path(self, channel):
        _RabbitMQManagement(channel).unbind("queue name", "exchange/name", "a/b")
        channel._command.assert_called_once_with(
            "management",
            None,
            "/bindings/src=exchange%2Fname;dstq=queue%20name;key=a%2Fb;args=",
            "DELETE",
            (204,),
        )


class TestChannel:
    @pytest.fixture
    def channel(self):
        channel, connection, state = make_channel()
        state.command = Mock()
        return channel

    def test_address_encoding(self, channel):
        assert channel._encode_address_part("a/b c") == "a%2Fb%20c"
        assert channel._encode_address_part("") == ""
        assert channel._encode_address_part(None) == ""

    def test_queue_address(self, channel):
        assert channel._queue_address("queue/name") == "/queues/queue%2Fname"

    def test_exchange_address(self, channel):
        assert channel._exchange_address("exchange") == "/exchanges/exchange"
        assert channel._exchange_address("exchange", "a/b") == "/exchanges/exchange/a%2Fb"

    def test_publish_addresses(self, channel):
        assert channel._publish_address("ex", "rk") == "/exchanges/ex/rk"
        assert channel._publish_address("", "queue") == "/queues/queue"

    def test_prepare_message(self, channel):
        with patch.object(module, "ProtonMessage", FakeProtonMessage):
            message = channel.prepare_message(
                "body",
                content_type="text/plain",
                content_encoding="utf-8",
                headers={"x": "y"},
                properties={"message_id": "1"},
            )
        assert message.body == "body"
        assert message.priority == 4
        assert message.content_type == "text/plain"
        assert message.content_encoding == "utf-8"
        assert message.properties == {
            "message_id": "1",
            "headers": {"x": "y"},
        }

    def test_prepare_message_explicit_priority(self, channel):
        with patch.object(module, "ProtonMessage", FakeProtonMessage):
            message = channel.prepare_message("body", priority=9)
        assert message.priority == 9

    def test_message_to_python(self, channel):
        raw = FakeProtonMessage(body="body", properties={"headers": {}})
        message = channel.message_to_python(raw)
        assert isinstance(message, Message)
        assert message.channel is channel

    def test_default_exchange_type(self, channel):
        assert channel.typeof("anything") == "direct"
        assert channel.typeof("anything", "topic") == "topic"

    def test_basic_consume(self, channel):
        channel._command.reset_mock()
        channel.basic_consume(
            "queue",
            False,
            Mock(),
            "tag",
        )
        assert channel._consumers["tag"][0] == "queue"
        channel._command.assert_called_once_with(
            "consume", "/queues/queue", "queue", "tag", 1
        )

    def test_basic_consume_prefetch(self, channel):
        callback = Mock()
        channel.basic_consume(
            "queue", False, callback, "tag", prefetch_count=8
        )
        channel._command.assert_called_once_with(
            "consume", "/queues/queue", "queue", "tag", 8
        )

    def test_basic_cancel(self, channel):
        channel._consumers["tag"] = ("q", False, Mock())
        channel.basic_cancel("tag")
        assert "tag" not in channel._consumers
        channel._command.assert_called_once_with("cancel", "tag")

    def test_basic_get(self, channel):
        delivery = FakeDelivery()
        raw = FakeProtonMessage(
            body="body", properties={"headers": {}}
        )
        channel._command.return_value = _Received(
            "q", raw, delivery, "get-tag"
        )
        message = channel.basic_get("q")
        assert message.body == "body"
        assert message.delivery_info["routing_key"] == "q"
        assert delivery.accepted is False
        assert list(channel.qos._not_yet_acked.values()) == [delivery]

    def test_basic_get_no_ack(self, channel):
        delivery = FakeDelivery()
        raw = FakeProtonMessage(body="body", properties={"headers": {}})
        channel._command.return_value = _Received("q", raw, delivery, "tag")
        message = channel.basic_get("q", no_ack=True)
        assert message.body == "body"
        assert delivery.accepted is False  # command is mocked
        assert channel._command.call_args_list[-1].args[0] == "accept"

    def test_basic_get_empty(self, channel):
        channel._command.return_value = None
        assert channel.basic_get("q") is None

    def test_basic_publish_proton_message(self, channel):
        raw = FakeProtonMessage(body="body")
        channel.basic_publish(raw, "ex", "rk")
        channel._command.assert_called_once_with("send", raw, "/exchanges/ex/rk")

    def test_basic_publish_kombu_message(self, channel):
        raw = FakeProtonMessage(body="body", properties={"headers": {}})
        message = Message(raw, channel=channel)
        channel.basic_publish(message, "ex", "rk")
        channel._command.assert_called_once_with("send", raw, "/exchanges/ex/rk")

    def test_basic_publish_generic_message(self, channel):
        generic = SimpleNamespace(
            body="body",
            content_type="text/plain",
            content_encoding="utf-8",
            headers={"x": 1},
            properties={"message_id": "id"},
        )
        with patch.object(module, "ProtonMessage", FakeProtonMessage):
            channel.basic_publish(generic, "", "queue")
        sent = channel._command.call_args.args[1]
        assert sent.body == "body"
        assert sent.properties["headers"] == {"x": 1}

    def test_basic_ack(self, channel):
        delivery = FakeDelivery()
        channel.qos.append(delivery, "tag")
        channel.basic_ack("tag")
        channel._command.assert_called_once_with("accept", delivery)

    def test_basic_ack_multiple_not_supported(self, channel):
        with pytest.raises(AssertionError):
            channel.basic_ack("tag", multiple=True)

    def test_basic_reject(self, channel):
        delivery = FakeDelivery()
        channel.qos.append(delivery, "tag")
        channel.basic_reject("tag", requeue=True)
        channel._command.assert_called_once_with("release", delivery)

    def test_queue_declare(self, channel):
        channel._management.declare_queue = Mock()
        result = channel.queue_declare(
            "q", durable=True, exclusive=True, auto_delete=False,
            arguments={"x": 1}
        )
        channel._management.declare_queue.assert_called_once_with(
            "q", durable=True, exclusive=True, auto_delete=False,
            arguments={"x": 1}
        )
        assert result.queue == "q"
        assert "q" in channel._queues

    def test_queue_declare_passive(self, channel):
        channel._management.declare_queue = Mock()
        channel.queue_declare("q", passive=True)
        channel._management.declare_queue.assert_called_once_with(
            "q", passive=True
        )

    def test_queue_delete_cleans_local_state(self, channel):
        channel._management.delete_queue = Mock(side_effect=RuntimeError("x"))
        channel._queues.add("q")
        channel._bindings.update({("q", "ex", "rk"), ("other", "ex", "rk")})
        with pytest.raises(RuntimeError):
            channel.queue_delete("q")
        assert "q" not in channel._queues
        assert ("q", "ex", "rk") not in channel._bindings
        assert ("other", "ex", "rk") in channel._bindings

    def test_exchange_declare(self, channel):
        channel._management.declare_exchange = Mock()
        channel.exchange_declare(
            "ex", type="topic", durable=True, auto_delete=True,
            internal=True, arguments={"x": 1}
        )
        channel._management.declare_exchange.assert_called_once_with(
            "ex", exchange_type="topic", durable=True,
            auto_delete=True, internal=True, arguments={"x": 1}
        )
        assert "ex" in channel._exchanges

    def test_default_exchange_declare_is_noop(self, channel):
        channel._management.declare_exchange = Mock()
        channel.exchange_declare("")
        channel._management.declare_exchange.assert_not_called()

    def test_exchange_delete_cleans_local_state(self, channel):
        channel._management.delete_exchange = Mock(side_effect=RuntimeError("x"))
        channel._exchanges.add("ex")
        channel._bindings.update({("q", "ex", "rk"), ("q", "other", "rk")})
        with pytest.raises(RuntimeError):
            channel.exchange_delete("ex")
        assert "ex" not in channel._exchanges
        assert ("q", "ex", "rk") not in channel._bindings
        assert ("q", "other", "rk") in channel._bindings

    def test_queue_bind(self, channel):
        channel._management.bind = Mock()
        channel.queue_bind("q", "ex", "rk", arguments={"x": 1})
        channel._management.bind.assert_called_once_with(
            "q", "ex", "rk", arguments={"x": 1}
        )
        assert ("q", "ex", "rk") in channel._bindings

    def test_queue_unbind(self, channel):
        channel._management.unbind = Mock()
        channel._bindings.add(("q", "ex", "rk"))
        channel.queue_unbind("q", "ex", "rk")
        channel._management.unbind.assert_called_once_with(
            "q", "ex", "rk", arguments=None
        )
        assert ("q", "ex", "rk") not in channel._bindings

    def test_queue_purge_is_explicitly_unsupported(self, channel):
        with pytest.raises(OperationalError, match="queue purge"):
            channel.queue_purge("q")

    def test_close_cancels_consumers_and_restores_qos(self, channel):
        channel._consumers["tag"] = ("q", False, Mock())
        channel.qos.append("delivery", "tag")
        channel._command.reset_mock()
        channel.close()
        assert channel.closed is True
        assert not channel.qos._not_yet_acked
        channel._command.assert_called_once_with("cancel", "tag")

    def test_close_is_idempotent(self, channel):
        channel.closed = True
        channel._command.reset_mock()
        channel.close()
        channel._command.assert_not_called()

    def test_encode_decode_body(self, channel):
        encoded, encoding = channel.encode_body("hello", "base64")
        decoded, decoded_encoding = channel.decode_body(encoded, encoding)
        assert decoded == "hello"
        assert decoded_encoding == "base64"
        assert channel.encode_body("hello") == ("hello", None)
        assert channel.decode_body("hello") == ("hello", None)


class TestHandler:
    @pytest.fixture
    def state(self):
        s = SimpleNamespace(
            container=FakeContainer(),
            injector=None,
            connect_kwargs={"user": "guest"},
            ssl_domain=None,
            url="amqp://localhost:5672",
            connection=None,
            management_sender=None,
            management_receiver=None,
            management_pending=None,
            connected_event=threading.Event(),
            disconnected_event=threading.Event(),
            start_event=threading.Event(),
            incoming=queue.Queue(),
            commands=queue.Queue(),
            receiver_tags={},
            receiver_queues={},
            senders={},
            receivers={},
            error=None,
            notify=Mock(),
            set_error=Mock(),
        )
        s.container.connection = Mock()
        return s

    def test_on_start(self, state):
        handler = _ProtonHandler(state)
        event = SimpleNamespace(container=state.container)
        with patch.object(module, "EventInjector", FakeInjector):
            handler.on_start(event)
        assert state.container is event.container
        assert state.injector is not None
        assert state.start_event.is_set()
        assert state.container.connect_calls

    def test_connect_includes_ssl_domain(self, state):
        handler = _ProtonHandler(state)
        state.ssl_domain = "ssl-domain"
        handler._connect()
        args, kwargs = state.container.connect_calls[-1]
        assert args == ("amqp://localhost:5672",)
        assert kwargs["reconnect"] is False
        assert kwargs["ssl_domain"] == "ssl-domain"
        assert kwargs["user"] == "guest"

    def test_connection_opened_creates_management_links(self, state):
        state.connection = Mock()
        handler = _ProtonHandler(state)
        handler.on_connection_opened(SimpleNamespace())
        assert state.management_sender is not None
        assert state.management_receiver is not None
        assert state.management_receiver.flow_calls == [1]
        assert state.connected_event.is_set()
        assert len(state.container.sender_calls) == 1
        assert len(state.container.receiver_calls) == 1

    def test_connection_opened_does_not_duplicate_links(self, state):
        state.connection = Mock()
        state.management_sender = FakeSender()
        state.management_receiver = FakeReceiver()
        handler = _ProtonHandler(state)
        handler.on_connection_opened(SimpleNamespace())
        assert state.management_sender is not None
        assert state.management_receiver.flow_calls == []
        assert state.container.sender_calls == []

    def test_connection_error(self, state):
        handler = _ProtonHandler(state)
        event = SimpleNamespace(connection=SimpleNamespace(condition="bad auth"))
        handler.on_connection_error(event)
        state.set_error.assert_called_once_with("bad auth")
        assert state.connected_event.is_set()
        state.notify.assert_called_once_with()

    def test_transport_error(self, state):
        handler = _ProtonHandler(state)
        event = SimpleNamespace(transport=SimpleNamespace(condition="network"))
        handler.on_transport_error(event)
        state.set_error.assert_called_once_with("network")
        state.notify.assert_called_once_with()

    def test_disconnected(self, state):
        handler = _ProtonHandler(state)
        handler.on_disconnected(SimpleNamespace())
        assert state.disconnected_event.is_set()
        state.notify.assert_called_once_with()

    def test_fail_management_wraps_string(self, state):
        command = _Command("management")
        state.management_pending = command
        handler = _ProtonHandler(state)
        handler._fail_management("disconnected")
        ok, error = command.result.get_nowait()
        assert ok is False
        assert isinstance(error, OperationalError)
        assert str(error) == "disconnected"
        assert state.management_pending is None

    def test_fail_management_preserves_exception(self, state):
        command = _Command("management")
        state.management_pending = command
        error = RuntimeError("boom")
        _ProtonHandler(state)._fail_management(error)
        ok, result = command.result.get_nowait()
        assert ok is False
        assert result is error

    def test_on_message_management_success(self, state):
        receiver = FakeReceiver()
        state.management_receiver = receiver
        command = _Command("management", expected_codes=(200, 204))
        state.management_pending = command
        response = FakeProtonMessage(subject="204", body={"ok": True})
        handler = _ProtonHandler(state)
        handler.on_message(SimpleNamespace(receiver=receiver, message=response))
        ok, value = command.result.get_nowait()
        assert ok is True
        assert value is response
        assert receiver.flow_calls == [1]
        state.notify.assert_called_once_with()

    def test_on_message_management_error_status(self, state):
        receiver = FakeReceiver()
        state.management_receiver = receiver
        command = _Command("management", expected_codes=(204,))
        state.management_pending = command
        response = FakeProtonMessage(subject="404", body="not found")
        _ProtonHandler(state).on_message(
            SimpleNamespace(receiver=receiver, message=response)
        )
        ok, error = command.result.get_nowait()
        assert ok is False
        assert "HTTP 404" in str(error)
        assert receiver.flow_calls == [1]

    def test_on_message_management_missing_status(self, state):
        receiver = FakeReceiver()
        state.management_receiver = receiver
        command = _Command("management", expected_codes=(200,))
        state.management_pending = command
        response = FakeProtonMessage(subject=None)
        _ProtonHandler(state).on_message(SimpleNamespace(receiver=receiver, message=response))
        ok, error = command.result.get_nowait()
        assert ok is False
        assert "no status code" in str(error)
        assert receiver.flow_calls == [1]

    def test_on_message_management_invalid_status(self, state):
        receiver = FakeReceiver()
        state.management_receiver = receiver
        command = _Command("management", expected_codes=(200,))
        state.management_pending = command
        response = FakeProtonMessage(subject="wat")
        _ProtonHandler(state).on_message(SimpleNamespace(receiver=receiver, message=response))
        ok, error = command.result.get_nowait()
        assert ok is False
        assert "Invalid RabbitMQ management response status" in str(error)
        assert receiver.flow_calls == [1]

    def test_on_message_management_unexpected_response_replenishes_credit(self, state):
        receiver = FakeReceiver()
        state.management_receiver = receiver
        _ProtonHandler(state).on_message(SimpleNamespace(receiver=receiver, message=Mock()))
        assert receiver.flow_calls == [1]
        state.notify.assert_not_called()

    def test_on_message_regular_delivery(self, state):
        receiver = FakeReceiver(name="tag")
        state.receiver_tags[receiver] = "tag"
        state.receiver_queues[receiver] = "queue"
        message = Mock()
        delivery = FakeDelivery()
        _ProtonHandler(state).on_message(
            SimpleNamespace(receiver=receiver, message=message, delivery=delivery)
        )
        received = state.incoming.get_nowait()
        assert received == _Received("queue", message, delivery, "tag")
        state.notify.assert_called_once_with()

    def test_regular_delivery_generates_consumer_tag(self, state):
        receiver = FakeReceiver()
        _ProtonHandler(state).on_message(
            SimpleNamespace(receiver=receiver, message="m", delivery="d")
        )
        received = state.incoming.get_nowait()
        assert received.queue == ""
        assert received.consumer_tag

    def test_dispatch_accept_reject_release(self, state):
        handler = _ProtonHandler(state)
        for name, attr in [("accept", "accepted"), ("reject", "rejected"), ("release", "released")]:
            delivery = FakeDelivery()
            command = _Command(name, args=(delivery,))
            assert handler._dispatch(command, name, delivery) is None
            assert getattr(delivery, attr) is True
            assert delivery.settled is True

    def test_dispatch_unknown(self, state):
        with pytest.raises(ValueError, match="Unknown Proton command"):
            _ProtonHandler(state)._dispatch(_Command("wat"), "wat")

    def test_sender_is_cached(self, state):
        handler = _ProtonHandler(state)
        first = handler._sender("/queues/q")
        second = handler._sender("/queues/q")
        assert first is second
        assert len(state.container.sender_calls) == 1

    def test_send_waits_for_credit(self, state):
        sender = FakeSender(credit=0)
        state.senders["addr"] = sender
        handler = _ProtonHandler(state)
        message = Mock()
        handler._send(message, "addr")
        assert state.container.work_calls == [0.05]
        assert sender.messages == [message]

    def test_consume_creates_and_caches_receiver(self, state):
        handler = _ProtonHandler(state)
        receiver = handler._consume("/queues/q", "q", "tag", 5)
        assert receiver.flow_calls == [5]
        assert state.receivers["tag"] is receiver
        assert state.receiver_queues[receiver] == "q"
        assert state.receiver_tags[receiver] == "tag"
        handler._consume("/queues/q", "q", "tag", 2)
        assert receiver.flow_calls == [5, 2]
        assert len(state.container.receiver_calls) == 1

    def test_cancel_removes_receiver(self, state):
        receiver = FakeReceiver(name="tag")
        state.receivers["tag"] = receiver
        state.receiver_queues[receiver] = "q"
        state.receiver_tags[receiver] = "tag"
        _ProtonHandler(state)._cancel("tag")
        assert receiver.closed is True
        assert "tag" not in state.receivers
        assert receiver not in state.receiver_queues
        assert receiver not in state.receiver_tags

    def test_cancel_unknown_is_noop(self, state):
        _ProtonHandler(state)._cancel("missing")

    def test_management_rejects_missing_links(self, state):
        with pytest.raises(OperationalError, match="links"):
            _ProtonHandler(state)._management(
                _Command("management"), {}, "/queues/q", "PUT", (204,)
            )

    def test_management_rejects_existing_pending(self, state):
        state.management_sender = FakeSender()
        state.management_receiver = FakeReceiver()
        state.management_pending = _Command("old")
        with pytest.raises(OperationalError, match="already pending"):
            _ProtonHandler(state)._management(
                _Command("management"), {}, "/queues/q", "PUT", (204,)
            )

    def test_management_sends_deferred_command(self, state):
        state.management_sender = FakeSender()
        state.management_receiver = FakeReceiver()
        command = _Command("management")
        with patch.object(module, "ProtonMessage", FakeProtonMessage):
            result = _ProtonHandler(state)._management(
                command, {"x": 1}, "/queues/q", "PUT", (200, 201)
            )
        assert result is module._DEFERRED
        assert command.expected_codes == (200, 201)
        assert state.management_pending is command
        sent = state.management_sender.messages[0]
        assert sent.address == "/queues/q"
        assert sent.subject == "PUT"
        assert sent.reply_to == "$me"
        assert sent.body == {"x": 1}

    def test_management_send_failure_clears_pending(self, state):
        sender = FakeSender()
        sender.send = Mock(side_effect=RuntimeError("send failed"))
        state.management_sender = sender
        state.management_receiver = FakeReceiver()
        command = _Command("management")
        with patch.object(module, "ProtonMessage", FakeProtonMessage):
            with pytest.raises(RuntimeError):
                _ProtonHandler(state)._management(
                    command, {}, "/queues/q", "PUT", (200,)
                )
        assert state.management_pending is None

    def test_get_returns_matching_delivery(self, state):
        state.get_timeout = 0
        state.container = FakeContainer()
        handler = _ProtonHandler(state)
        result = handler._get("/queues/q", "q")
        assert result is None
        assert state.container.receiver_calls

    def test_close_dispatch_stops_container(self, state):
        state.connection = Mock()
        state.container = FakeContainer()
        result = _ProtonHandler(state)._dispatch(_Command("close"), "close")
        assert result is None
        state.connection.close.assert_called_once_with()
        assert state.container.stopped is True


class TestProtonState:
    def test_command_requires_running_reactor(self, state):
        with pytest.raises(OperationalError, match="not running"):
            state.command("send")

    def test_command_round_trip(self, state):
        state.injector = FakeInjector()
        def complete(item):
            item.result.put((True, "result"))

        # Exercise the real command path by completing the exact command queued.
        original_put = state.commands.put
        state.commands.put = lambda item: (original_put(item), complete(item))[-1]
        assert state.command("send") == "result"
        assert state.injector.events

    def test_command_error_is_raised(self, state):
        state.injector = FakeInjector()
        error = RuntimeError("boom")
        original_put = state.commands.put
        state.commands.put = lambda item: (original_put(item), item.result.put((False, error)))[-1]
        with pytest.raises(RuntimeError, match="boom"):
            state.command("send")

    def test_command_timeout(self, state):
        state.injector = FakeInjector()
        with pytest.raises(OperationalError, match="Proton command timed out: send"):
            state.command("send")

    def test_notify_and_drain_notifications(self, state):
        state.notify()
        assert state.read_sock.recv(1) == b"1"
        state.notify()
        state.drain_notifications()
        with pytest.raises(BlockingIOError):
            state.read_sock.recv(1)

    def test_set_error(self, state):
        state.set_error("broken")
        assert isinstance(state.error, OperationalError)
        assert str(state.error) == "broken"
        existing = state.error
        state.set_error(None)
        assert state.error is existing

    def test_fail_pending_commands(self, state):
        error = OperationalError("reactor stopped")
        pending = _Command("management")
        state.management_pending = pending
        queued = _Command("send")
        state.commands.put(queued)
        state._fail_pending_commands(error)
        assert state.management_pending is None
        assert pending.result.get_nowait() == (False, error)
        assert queued.result.get_nowait() == (False, error)

    def test_fail_pending_commands_ignores_full_result_queue(self, state):
        error = OperationalError("stopped")
        pending = _Command("management")
        pending.result.put((True, "already done"))
        state.management_pending = pending
        state._fail_pending_commands(error)
        assert pending.result.get_nowait() == (True, "already done")

    def test_run_reactor_failure_sets_error_and_fails_commands(self, state):
        queued = _Command("send")
        state.commands.put(queued)
        failure = RuntimeError("reactor failed")
        container = Mock()
        container.run.side_effect = failure
        with patch.object(module, "Container", return_value=container):
            state._run()
        assert isinstance(state.error, OperationalError)
        assert "reactor failed" in str(state.error)
        assert queued.result.get_nowait()[0] is False

    def test_run_normal_stop_fails_pending(self, state):
        queued = _Command("send")
        state.commands.put(queued)
        container = Mock()
        with patch.object(module, "Container", return_value=container):
            state._run()
        ok, error = queued.result.get_nowait()
        assert ok is False
        assert "reactor stopped" in str(error)

    def test_close_is_safe_without_injector(self, state):
        state.close()
        assert not state.thread.is_alive()

    def test_close_sends_close_command_when_running(self, state):
        state.injector = FakeInjector()
        state.command = Mock()
        state.thread = Mock()
        state.thread.is_alive.return_value = True
        state.close()
        state.command.assert_called_once_with("close")
        state.thread.join.assert_called_once_with(timeout=state.command_timeout)


class TestConnection:
    def test_client(self):
        client = make_client()
        state = SimpleNamespace(transport=Transport(client))
        connection = ProtonConnection(state, client)
        assert connection.client is client
        assert connection.connected is True

    def test_channel(self):
        client = make_client()
        transport = Transport(client)
        state = SimpleNamespace(transport=transport)
        connection = ProtonConnection(state, client)
        channel = connection.channel()
        assert isinstance(channel, Channel)
        assert connection.channels == [channel]

    def test_close(self):
        state = Mock()
        connection = ProtonConnection(state, Mock())
        connection.close()
        state.close.assert_called_once_with()
        assert connection.connected is False

    def test_close_idempotent(self):
        state = Mock()
        connection = ProtonConnection(state, Mock())
        connection.close()
        connection.close()
        state.close.assert_called_once_with()


class TestTransportAliases:
    def test_transport_alias(self):
        from kombu.transport import TRANSPORT_ALIASES

        assert TRANSPORT_ALIASES["proton"] == "kombu.transport.proton:Transport"

    def test_import_without_proton_is_explicit(self):
        with patch.object(module, "proton", None):
            with pytest.raises(ImportError, match="python-qpid-proton"):
                Transport(make_client())
