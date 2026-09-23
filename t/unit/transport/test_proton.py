"""Unit tests for the Proton AMQP 1.0 transport."""

from __future__ import annotations

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

    def test_address_template(self):
        client = make_client(
            transport_options={
                "address_template":
                    "{exchange}/{routing_key}",
            }
        )

        transport_ = transport.Transport(
            client
        )

        channel = object.__new__(
            transport.Channel
        )

        channel.transport = transport_

        assert (
            channel._address(
                "orders",
                "created",
            )
            == "orders/created"
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


class test_Channel:

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
            message.properties[
                "headers"
            ]["x-test"]
            == "1"
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


def test_transport_alias():
    connection = Connection(
        "proton://localhost:5672"
    )

    assert (
        connection.transport_cls
        is transport.Transport
    )