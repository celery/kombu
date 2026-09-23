"""Unit tests for the Proton AMQP 1.0 transport."""

from __future__ import annotations

from importlib.metadata import version
from unittest.mock import MagicMock

import pytest

proton = pytest.importorskip("proton")

from kombu import Connection
from kombu.transport import proton as transport


def make_client(**options):
    client = MagicMock()

    client.transport_options = options.pop(
        "transport_options",
        {},
    )

    client.ssl = options.pop(
        "ssl",
        False,
    )

    client.hostname = options.pop(
        "hostname",
        "localhost",
    )

    client.port = options.pop(
        "port",
        5672,
    )

    client.userid = options.pop(
        "userid",
        None,
    )

    client.password = options.pop(
        "password",
        None,
    )

    client.virtual_host = options.pop(
        "virtual_host",
        "/",
    )

    client.login_method = options.pop(
        "login_method",
        None,
    )

    client.heartbeat = options.pop(
        "heartbeat",
        0,
    )

    return client


class test_Transport:

    def test_driver(self):
        client = make_client()

        transport_ = transport.Transport(
            client
        )

        assert (
            transport_.driver_name
            == "qpid-proton"
        )

        assert (
            transport_.driver_type
            == "amqp"
        )

        assert (
            transport_.driver_version()
            == version("python-qpid-proton")
        )

    def test_default_connection_params(self):
        client = make_client()

        transport_ = transport.Transport(
            client
        )

        params = (
            transport_.default_connection_params
        )

        assert params["hostname"] == "localhost"
        assert params["port"] == 5672

    def test_default_ssl_port(self):
        client = make_client(
            ssl=True
        )

        transport_ = transport.Transport(
            client
        )

        params = (
            transport_.default_connection_params
        )

        assert params["port"] == 5671

    def test_address_template_compatibility(self):
        client = make_client(
            transport_options={
                "address_template":
                    "{exchange}/{routing_key}",
            }
        )

        transport_ = transport.Transport(
            client
        )

        assert (
            transport_.address_template
            == "{exchange}/{routing_key}"
        )

    def test_transport_options(self):
        client = make_client(
            transport_options={
                "command_timeout": 5,
                "prefetch_count": 10,
                "custom_option": "value",
            }
        )

        transport_ = transport.Transport(
            client
        )

        assert transport_.command_timeout == 5
        assert transport_.prefetch_count == 10
        assert (
            transport_.transport_options[
                "custom_option"
            ]
            == "value"
        )


class test_QoS:

    def test_can_consume(self):
        channel = MagicMock()

        qos = transport.QoS(
            channel,
            prefetch_count=1,
        )

        assert qos.can_consume()

        qos.append(
            MagicMock(),
            "delivery-1",
        )

        assert not qos.can_consume()

    def test_unlimited_prefetch(self):
        channel = MagicMock()

        qos = transport.QoS(
            channel,
            prefetch_count=0,
        )

        for index in range(100):
            qos.append(
                MagicMock(),
                str(index),
            )

        assert qos.can_consume()

    def test_can_consume_max_estimate(self):
        channel = MagicMock()

        qos = transport.QoS(
            channel,
            prefetch_count=3,
        )

        assert (
            qos.can_consume_max_estimate()
            == 3
        )

        qos.append(
            MagicMock(),
            "delivery-1",
        )

        assert (
            qos.can_consume_max_estimate()
            == 2
        )

    def test_unlimited_can_consume_max_estimate(self):
        channel = MagicMock()

        qos = transport.QoS(
            channel,
            prefetch_count=0,
        )

        for index in range(100):
            qos.append(
                MagicMock(),
                str(index),
            )

        assert (
            qos.can_consume_max_estimate()
            == 1
        )

    def test_get(self):
        channel = MagicMock()

        qos = transport.QoS(
            channel,
            prefetch_count=1,
        )

        delivery = MagicMock()

        qos.append(
            delivery,
            "tag",
        )

        assert qos.get("tag") is delivery

    def test_ack(self):
        channel = MagicMock()

        qos = transport.QoS(
            channel,
            prefetch_count=1,
        )

        delivery = MagicMock()

        qos.append(
            delivery,
            "tag",
        )

        qos.ack("tag")

        channel._command.assert_called_once_with(
            "accept",
            delivery,
        )

        assert not qos._not_yet_acked

    def test_reject(self):
        channel = MagicMock()

        qos = transport.QoS(
            channel,
            prefetch_count=1,
        )

        delivery = MagicMock()

        qos.append(
            delivery,
            "tag",
        )

        qos.reject(
            "tag",
            requeue=False,
        )

        channel._command.assert_called_once_with(
            "reject",
            delivery,
        )

        assert not qos._not_yet_acked

    def test_release(self):
        channel = MagicMock()

        qos = transport.QoS(
            channel,
            prefetch_count=1,
        )

        delivery = MagicMock()

        qos.append(
            delivery,
            "tag",
        )

        qos.reject(
            "tag",
            requeue=True,
        )

        channel._command.assert_called_once_with(
            "release",
            delivery,
        )

        assert not qos._not_yet_acked

    def test_restore_unacked(self):
        channel = MagicMock()

        qos = transport.QoS(
            channel,
            prefetch_count=2,
        )

        qos.append(
            MagicMock(),
            "tag-1",
        )

        qos.append(
            MagicMock(),
            "tag-2",
        )

        assert len(
            qos._not_yet_acked
        ) == 2

        qos.restore_unacked()

        assert not qos._not_yet_acked


class test_Channel:

    @pytest.fixture()
    def channel(self):
        client = make_client()

        transport_ = transport.Transport(
            client
        )

        connection = object.__new__(
            transport.Connection
        )

        connection.state = MagicMock()
        connection.client = client
        connection.channels = []
        connection.connected = True

        channel = object.__new__(
            transport.Channel
        )

        channel.connection = connection
        channel.transport = transport_
        channel.state = connection.state
        channel.closed = False
        channel.command_timeout = 10.0
        channel.qos = transport.QoS(
            channel,
            transport_.prefetch_count,
        )
        channel._consumers = {}
        channel._queues = set()
        channel._exchanges = set()
        channel._bindings = set()

        return channel

    def test_encode_address_part(
        self,
        channel,
    ):
        assert (
            channel._encode_address_part(
                "orders"
            )
            == "orders"
        )

        assert (
            channel._encode_address_part(
                "orders/created"
            )
            == "orders%2Fcreated"
        )

        assert (
            channel._encode_address_part(
                "orders created"
            )
            == "orders%20created"
        )

    def test_queue_address(
        self,
        channel,
    ):
        assert (
            channel._queue_address(
                "orders"
            )
            == "/queues/orders"
        )

    def test_queue_address_encodes_name(
        self,
        channel,
    ):
        assert (
            channel._queue_address(
                "orders/created"
            )
            == "/queues/orders%2Fcreated"
        )

    def test_exchange_address(
        self,
        channel,
    ):
        assert (
            channel._exchange_address(
                "events",
                "created",
            )
            == "/exchanges/events/created"
        )

    def test_exchange_address_encodes_parts(
        self,
        channel,
    ):
        assert (
            channel._exchange_address(
                "events/main",
                "order created",
            )
            == "/exchanges/events%2Fmain/"
               "order%20created"
        )

    def test_exchange_address_without_routing_key(
        self,
        channel,
    ):
        assert (
            channel._exchange_address(
                "events"
            )
            == "/exchanges/events"
        )

    def test_publish_address_exchange(
        self,
        channel,
    ):
        assert (
            channel._publish_address(
                "events",
                "created",
            )
            == "/exchanges/events/created"
        )

    def test_publish_address_default_exchange(
        self,
        channel,
    ):
        assert (
            channel._publish_address(
                "",
                "orders",
            )
            == "/queues/orders"
        )

    def test_prepare_message(self):
        channel = object.__new__(
            transport.Channel
        )

        message = channel.prepare_message(
            body={"hello": "world"},
            content_type="application/json",
            headers={
                "x-test": "1",
            },
        )

        assert (
            message.body
            == {"hello": "world"}
        )

        assert (
            message.content_type
            == "application/json"
        )

        assert (
            message.priority
            == 4
        )

        assert (
            message.properties[
                "headers"
            ]["x-test"]
            == "1"
        )

    @pytest.mark.parametrize(
        "priority",
        [0, 1, 4, 9],
    )
    def test_prepare_message_priority(
        self,
        priority,
    ):
        channel = object.__new__(
            transport.Channel
        )

        message = channel.prepare_message(
            body="hello",
            priority=priority,
        )

        assert (
            message.priority
            == priority
        )

    def test_prepare_message_properties(self):
        channel = object.__new__(
            transport.Channel
        )

        message = channel.prepare_message(
            body="hello",
            properties={
                "message-id": "123",
                "custom": "value",
            },
        )

        assert (
            message.properties[
                "message-id"
            ]
            == "123"
        )

        assert (
            message.properties[
                "custom"
            ]
            == "value"
        )

        assert (
            message.properties[
                "headers"
            ]
            == {}
        )

    def test_prepare_message_headers_without_properties(
        self,
    ):
        channel = object.__new__(
            transport.Channel
        )

        message = channel.prepare_message(
            body="hello",
            headers={
                "x-test": "1",
            },
        )

        assert (
            message.properties[
                "headers"
            ]["x-test"]
            == "1"
        )

    def test_prepare_message_default_headers(
        self,
    ):
        channel = object.__new__(
            transport.Channel
        )

        message = channel.prepare_message(
            body="hello",
        )

        assert (
            message.properties[
                "headers"
            ]
            == {}
        )

    def test_exchange_type_default(self):
        channel = object.__new__(
            transport.Channel
        )

        assert (
            channel.typeof(
                "missing"
            )
            == "direct"
        )

    def test_basic_consume_uses_queue_address(
        self,
        channel,
    ):
        channel._command = MagicMock()

        callback = MagicMock()

        channel.basic_consume(
            queue="orders",
            no_ack=False,
            callback=callback,
            consumer_tag="consumer-1",
        )

        channel._command.assert_called_once_with(
            "consume",
            "/queues/orders",
            "orders",
            "consumer-1",
            1,
        )

    def test_basic_consume_encodes_queue_address(
        self,
        channel,
    ):
        channel._command = MagicMock()

        channel.basic_consume(
            queue="orders/created",
            no_ack=False,
            callback=MagicMock(),
            consumer_tag="consumer-1",
        )

        channel._command.assert_called_once_with(
            "consume",
            "/queues/orders%2Fcreated",
            "orders/created",
            "consumer-1",
            1,
        )

    def test_basic_get_uses_queue_address(
        self,
        channel,
    ):
        channel._command = MagicMock(
            return_value=None
        )

        result = channel.basic_get(
            "orders"
        )

        assert result is None

        channel._command.assert_called_once_with(
            "get",
            "/queues/orders",
            "orders",
        )

    def test_basic_publish_uses_exchange_address(
        self,
        channel,
    ):
        channel._command = MagicMock()

        message = MagicMock()
        message.body = "hello"
        message.content_type = None
        message.content_encoding = None
        message.headers = {}
        message.properties = {}

        channel.basic_publish(
            message,
            exchange="events",
            routing_key="created",
        )

        channel._command.assert_called_once()

        args = (
            channel._command.call_args.args
        )

        assert args[0] == "send"
        assert (
            args[2]
            == "/exchanges/events/created"
        )

    def test_basic_publish_default_exchange_uses_queue(
        self,
        channel,
    ):
        channel._command = MagicMock()

        message = MagicMock()
        message.body = "hello"
        message.content_type = None
        message.content_encoding = None
        message.headers = {}
        message.properties = {}

        channel.basic_publish(
            message,
            exchange="",
            routing_key="orders",
        )

        args = (
            channel._command.call_args.args
        )

        assert args[0] == "send"
        assert (
            args[2]
            == "/queues/orders"
        )


class test_Connection:

    def test_client(self):
        client = make_client()

        state = MagicMock()

        connection = transport.Connection(
            state,
            client,
        )

        assert connection.client is client
        assert connection.state is state
        assert connection.connected

    def test_channel(self):
        client = make_client()

        state = MagicMock()

        connection = transport.Connection(
            state,
            client,
        )

        channel = connection.channel()

        assert (
            channel.connection
            is connection
        )

        assert (
            channel.transport
            is state.transport
        )

        assert (
            channel in connection.channels
        )

    def test_close(self):
        client = make_client()

        state = MagicMock()

        connection = transport.Connection(
            state,
            client,
        )

        connection.close()

        state.close.assert_called_once()
        assert not connection.connected

    def test_close_idempotent(self):
        client = make_client()

        state = MagicMock()

        connection = transport.Connection(
            state,
            client,
        )

        connection.close()
        connection.close()

        state.close.assert_called_once()


def test_transport_alias():
    connection = Connection(
        "proton://localhost:5672"
    )

    assert connection.transport_cls == "proton"