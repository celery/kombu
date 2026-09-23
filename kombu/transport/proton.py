"""AMQP 1.0 transport using Apache Qpid Proton.

This transport is a modern rewrite of the AMQP 1.0 work from Kombu #810.

Unlike the historical Qpid transport, this implementation does not depend on:

* qpid-python
* qpid-tools
* QMF

The Proton reactor runs in a dedicated thread while the Kombu API remains
synchronous. Commands are injected into the Proton reactor using Proton's
EventInjector and incoming deliveries are forwarded to Kombu through a
socketpair.

The messaging implementation uses AMQP 1.0. RabbitMQ-specific topology
management is implemented through RabbitMQ's AMQP 1.0 management endpoint.
The messaging/address layer remains separate from topology administration.

The initial messaging address mapping follows RabbitMQ's AMQP 1.0 address
v2 model. Other AMQP 1.0 brokers may use different address semantics.
"""

from __future__ import annotations

import queue
import socket
import threading
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from importlib.metadata import version
from time import monotonic
from urllib.parse import quote

import amqp.protocol

from kombu.exceptions import OperationalError
from kombu.log import get_logger
from kombu.transport import base, virtual
from kombu.transport.virtual import Base64

try:
    import proton
    from proton import Message as ProtonMessage
    from proton import SSLDomain
    from proton.handlers import MessagingHandler
    from proton.reactor import (ApplicationEvent, AtLeastOnce, Container,
                                EventInjector, LinkOption)
except ImportError:  # pragma: no cover
    proton = None
    ProtonMessage = None
    SSLDomain = None
    MessagingHandler = object
    ApplicationEvent = None
    AtLeastOnce = None
    Container = None
    EventInjector = None
    LinkOption = object


logger = get_logger(__name__)


DEFAULT_PORT = 5672
DEFAULT_SSL_PORT = 5671
DEFAULT_PREFETCH = 1

COMMAND_EVENT = "kombu_proton_command"

_MANAGEMENT_ADDRESS = "/management"
_MANAGEMENT_REPLY_TO = "$me"
_MANAGEMENT_PUT = "PUT"
_MANAGEMENT_POST = "POST"
_MANAGEMENT_DELETE = "DELETE"
_MANAGEMENT_GET = "GET"

_DEFERRED = object()


@dataclass
class _Command:
    name: str
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    result: queue.Queue = field(
        default_factory=lambda: queue.Queue(maxsize=1)
    )


@dataclass
class _Received:
    queue: str
    message: object
    delivery: object
    consumer_tag: str


class AuthenticationFailure(Exception):
    """Authentication failed."""


class QoS:
    """Track unsettled AMQP 1.0 deliveries."""

    def __init__(self, channel, prefetch_count=DEFAULT_PREFETCH):
        self.channel = channel
        self.prefetch_count = prefetch_count
        self._not_yet_acked = OrderedDict()

    def can_consume(self):
        return (
            not self.prefetch_count
            or len(self._not_yet_acked) < self.prefetch_count
        )

    def can_consume_max_estimate(self):
        if not self.prefetch_count:
            return 1

        return max(
            0,
            self.prefetch_count - len(self._not_yet_acked),
        )

    def append(self, delivery, delivery_tag):
        self._not_yet_acked[delivery_tag] = delivery

    def get(self, delivery_tag):
        return self._not_yet_acked[delivery_tag]

    def ack(self, delivery_tag):
        delivery = self._not_yet_acked.pop(delivery_tag)

        self.channel._command(
            "accept",
            delivery,
        )

    def reject(self, delivery_tag, requeue=False):
        delivery = self._not_yet_acked.pop(delivery_tag)

        self.channel._command(
            "release" if requeue else "reject",
            delivery,
        )

    def restore_unacked(self):
        self._not_yet_acked.clear()


class Message(virtual.Message):
    """Kombu Message backed by a Proton Message."""

    def __init__(
        self,
        message,
        channel=None,
        delivery=None,
        delivery_tag=None,
        **kwargs,
    ):
        properties = dict(message.properties or {})
        headers = properties.pop("headers", {}) or {}

        super().__init__(
            body=message.body,
            channel=channel,
            delivery_tag=delivery_tag,
            content_type=message.content_type,
            content_encoding=message.content_encoding,
            properties=properties,
            headers=headers,
            delivery_info=kwargs.pop(
                "delivery_info",
                None,
            ),
            **kwargs,
        )

        self.proton_message = message
        self.proton_delivery = delivery


if proton is not None:

    class _ManagementSenderOption(LinkOption):
        """Configure the RabbitMQ management sender link."""

        def apply(self, link):
            link.snd_settle_mode = proton.Link.SND_SETTLED
            link.rcv_settle_mode = proton.Link.RCV_FIRST
            link.properties = {
                "paired": True,
            }

    class _ManagementReceiverOption(LinkOption):
        """Configure the RabbitMQ management receiver link."""

        def apply(self, link):
            link.source.address = _MANAGEMENT_ADDRESS
            link.snd_settle_mode = proton.Link.SND_SETTLED
            link.rcv_settle_mode = proton.Link.RCV_FIRST
            link.properties = {
                "paired": True,
            }
            link.source.dynamic = False

else:  # pragma: no cover

    class _ManagementSenderOption:
        pass

    class _ManagementReceiverOption:
        pass


class _ProtonHandler(MessagingHandler):
    """Own Proton objects and dispatch Proton events."""

    def __init__(self, state):
        super().__init__()
        self.state = state

    def on_start(self, event):
        self.state.container = event.container

        self.state.injector = EventInjector()
        event.container.selectable(
            self.state.injector
        )

        self._connect()

        self.state.start_event.set()

    def _connect(self):
        kwargs = dict(
            self.state.connect_kwargs
        )

        if self.state.ssl_domain is not None:
            kwargs["ssl_domain"] = (
                self.state.ssl_domain
            )

        self.state.connection = (
            self.state.container.connect(
                self.state.url,
                reconnect=False,
                **kwargs,
            )
        )

    def on_connection_opened(self, event):
        if (
            self.state.management_sender is None
        ):
            self.state.management_sender = (
                self.state.container.create_sender(
                    self.state.connection,
                    target=_MANAGEMENT_ADDRESS,
                    name="kombu-management-sender",
                    options=_ManagementSenderOption(),
                )
            )

        if (
            self.state.management_receiver is None
        ):
            self.state.management_receiver = (
                self.state.container.create_receiver(
                    self.state.connection,
                    name="kombu-management-receiver",
                    options=_ManagementReceiverOption(),
                )
            )

            self.state.management_receiver.flow(1)

        self.state.connected_event.set()

    def on_connection_error(self, event):
        self._fail_management(
            getattr(
                event.connection,
                "condition",
                None,
            )
        )

        self.state.set_error(
            getattr(
                event.connection,
                "condition",
                None,
            )
        )

        self.state.connected_event.set()
        self.state.notify()

    def on_transport_error(self, event):
        self._fail_management(
            getattr(
                event.transport,
                "condition",
                None,
            )
        )

        self.state.set_error(
            getattr(
                event.transport,
                "condition",
                None,
            )
        )

        self.state.notify()

    def on_disconnected(self, event):
        self._fail_management(
            "Proton connection disconnected"
        )

        self.state.disconnected_event.set()
        self.state.notify()

    def _fail_management(self, error):
        command = self.state.management_pending

        if command is None:
            return

        self.state.management_pending = None

        if isinstance(error, BaseException):
            exc = error
        else:
            exc = OperationalError(
                str(error)
            )

        try:
            command.result.put_nowait(
                (False, exc)
            )
        except queue.Full:
            pass

    def on_message(self, event):
        receiver = event.receiver

        if (
            receiver is self.state.management_receiver
        ):
            command = self.state.management_pending

            if command is None:
                logger.debug(
                    "Ignoring unexpected RabbitMQ "
                    "management response"
                )
                return

            self.state.management_pending = None

            response = event.message
            subject = response.subject

            if subject is None:
                error = OperationalError(
                    "RabbitMQ management response "
                    "has no status code"
                )

                command.result.put(
                    (False, error)
                )

                self.state.notify()
                return

            try:
                status = int(subject)
            except (TypeError, ValueError) as exc:
                command.result.put(
                    (
                        False,
                        OperationalError(
                            "Invalid RabbitMQ management "
                            f"response status: {subject!r}"
                        ),
                    )
                )

                self.state.notify()
                return

            expected_codes = command.kwargs.get(
                "expected_codes",
                (),
            )

            if status not in expected_codes:
                body = response.body

                error = OperationalError(
                    "RabbitMQ management request "
                    f"failed with HTTP {status}: "
                    f"{body!r}"
                )

                command.result.put(
                    (False, error)
                )
            else:
                command.result.put(
                    (True, response)
                )

            self.state.management_receiver.flow(1)
            self.state.notify()
            return

        consumer_tag = (
            self.state.receiver_tags.get(
                receiver,
                getattr(
                    receiver,
                    "name",
                    None,
                ),
            )
        )

        if consumer_tag is None:
            consumer_tag = str(uuid.uuid4())

        received = _Received(
            queue=self.state.receiver_queues.get(
                receiver,
                "",
            ),
            message=event.message,
            delivery=event.delivery,
            consumer_tag=consumer_tag,
        )

        self.state.incoming.put(received)
        self.state.notify()

    def on_kombu_proton_command(self, event):
        while True:
            try:
                command = (
                    self.state.commands.get_nowait()
                )
            except queue.Empty:
                return

            try:
                value = self._dispatch(
                    command.name,
                    *command.args,
                    **command.kwargs,
                )
            except BaseException as exc:
                command.result.put(
                    (False, exc)
                )
            else:
                if value is not _DEFERRED:
                    command.result.put(
                        (True, value)
                    )

    def _dispatch(
        self,
        name,
        *args,
        **kwargs,
    ):
        if name == "send":
            return self._send(
                *args,
                **kwargs,
            )

        if name == "consume":
            return self._consume(
                *args,
                **kwargs,
            )

        if name == "cancel":
            return self._cancel(
                *args,
                **kwargs,
            )

        if name == "accept":
            delivery = args[0]
            delivery.accept()
            delivery.settle()
            return None

        if name == "reject":
            delivery = args[0]
            delivery.reject()
            delivery.settle()
            return None

        if name == "release":
            delivery = args[0]
            delivery.release()
            delivery.settle()
            return None

        if name == "get":
            return self._get(
                *args,
                **kwargs,
            )

        if name == "management":
            return self._management(
                *args,
                **kwargs,
            )

        if name == "close":
            if self.state.connection is not None:
                self.state.connection.close()

            self.state.container.stop()
            return None

        raise ValueError(
            f"Unknown Proton command: {name}"
        )

    def _sender(self, address):
        sender = self.state.senders.get(
            address
        )

        if sender is None:
            sender = (
                self.state.container.create_sender(
                    self.state.connection,
                    target=address,
                    options=AtLeastOnce(),
                )
            )

            self.state.senders[address] = (
                sender
            )

        return sender

    def _send(self, message, address):
        sender = self._sender(address)

        if sender.credit <= 0:
            self.state.container.do_work(
                0.05
            )

        return sender.send(message)

    def _consume(
        self,
        source_address,
        queue_name,
        consumer_tag,
        prefetch,
    ):
        receiver = self.state.receivers.get(
            consumer_tag
        )

        if receiver is None:
            receiver = (
                self.state.container.create_receiver(
                    self.state.connection,
                    source=source_address,
                    name=consumer_tag,
                    options=AtLeastOnce(),
                )
            )

            self.state.receivers[
                consumer_tag
            ] = receiver

            self.state.receiver_queues[
                receiver
            ] = queue_name

            self.state.receiver_tags[
                receiver
            ] = consumer_tag

        receiver.flow(
            prefetch or 1
        )

        return receiver

    def _cancel(self, consumer_tag):
        receiver = (
            self.state.receivers.pop(
                consumer_tag,
                None,
            )
        )

        if receiver is not None:
            self.state.receiver_queues.pop(
                receiver,
                None,
            )

            self.state.receiver_tags.pop(
                receiver,
                None,
            )

            receiver.close()

    def _get(
        self,
        source_address,
        queue_name,
    ):
        """Implement Kombu basic_get."""

        consumer_tag = (
            f"kombu-get-{uuid.uuid4()}"
        )

        receiver = (
            self.state.container.create_receiver(
                self.state.connection,
                source=source_address,
                name=consumer_tag,
                options=AtLeastOnce(),
            )
        )

        self.state.receiver_queues[
            receiver
        ] = queue_name

        self.state.receiver_tags[
            receiver
        ] = consumer_tag

        receiver.flow(1)

        deadline = (
            monotonic()
            + self.state.get_timeout
        )

        while monotonic() < deadline:
            self.state.container.do_work(
                0.05
            )

            try:
                received = (
                    self.state.incoming.get_nowait()
                )
            except queue.Empty:
                continue

            if (
                received.consumer_tag
                == consumer_tag
            ):
                receiver.close()

                self.state.receiver_queues.pop(
                    receiver,
                    None,
                )

                self.state.receiver_tags.pop(
                    receiver,
                    None,
                )

                return received

            self.state.incoming.put(
                received
            )

        receiver.close()

        self.state.receiver_queues.pop(
            receiver,
            None,
        )

        self.state.receiver_tags.pop(
            receiver,
            None,
        )

        return None

    def _management(
        self,
        body,
        path,
        method,
        expected_codes,
    ):
        if (
            self.state.management_sender is None
            or self.state.management_receiver is None
        ):
            raise OperationalError(
                "RabbitMQ management links "
                "are not available"
            )

        if self.state.management_pending is not None:
            raise OperationalError(
                "A RabbitMQ management request "
                "is already pending"
            )

        command = self.state.current_command

        if command is None:
            raise OperationalError(
                "RabbitMQ management request has "
                "no command context"
            )

        message = ProtonMessage(
            id=str(uuid.uuid4()),
            body=body,
            inferred=False,
            reply_to=_MANAGEMENT_REPLY_TO,
            address=path,
            subject=method,
            durable=False,
        )

        command.kwargs[
            "expected_codes"
        ] = tuple(expected_codes)

        self.state.management_pending = (
            command
        )

        self.state.management_sender.send(
            message
        )

        return _DEFERRED


class _ProtonState:
    """Thread-owned Proton runtime."""

    def __init__(
        self,
        url,
        connect_kwargs,
        ssl_domain,
        command_timeout=10.0,
    ):
        self.url = url
        self.connect_kwargs = connect_kwargs
        self.ssl_domain = ssl_domain
        self.command_timeout = (
            command_timeout
        )
        self.get_timeout = command_timeout

        self.commands = queue.Queue()
        self.incoming = queue.Queue()

        self.start_event = threading.Event()
        self.connected_event = threading.Event()
        self.disconnected_event = threading.Event()

        self.container = None
        self.injector = None
        self.connection = None

        self.senders = {}
        self.receivers = {}
        self.receiver_queues = {}
        self.receiver_tags = {}

        self.management_sender = None
        self.management_receiver = None
        self.management_pending = None
        self.current_command = None

        self.error = None

        self.read_sock, self.write_sock = (
            socket.socketpair()
        )

        self.read_sock.setblocking(False)
        self.write_sock.setblocking(False)

        self.thread = threading.Thread(
            target=self._run,
            name="kombu-proton",
            daemon=True,
        )

    def start(self):
        self.thread.start()

        if not self.start_event.wait(
            self.command_timeout
        ):
            raise OperationalError(
                "Timed out starting Proton reactor"
            )

        if not self.connected_event.wait(
            self.command_timeout
        ):
            self.close()

            raise OperationalError(
                f"Timed out connecting to {self.url}"
            )

        if self.error:
            raise self.error

    def _run(self):
        try:
            handler = _ProtonHandler(
                self
            )

            self.container = Container(
                handler
            )

            self.container.run()

        except BaseException as exc:
            self.set_error(exc)
            self.connected_event.set()
            self.notify()

    def command(
        self,
        name,
        *args,
        **kwargs,
    ):
        command = _Command(
            name=name,
            args=args,
            kwargs=kwargs,
        )

        self.commands.put(command)

        if self.injector is None:
            raise OperationalError(
                "Proton reactor is not running"
            )

        self.injector.trigger(
            ApplicationEvent(
                COMMAND_EVENT
            )
        )

        ok = False
        value = None

        try:
            self.current_command = command

            ok, value = command.result.get(
                timeout=self.command_timeout
            )
        finally:
            self.current_command = None

        if not ok:
            raise value

        return value

    def notify(self):
        try:
            self.write_sock.send(
                b"1"
            )
        except (
            BlockingIOError,
            OSError,
        ):
            pass

    def drain_notifications(self):
        try:
            while self.read_sock.recv(
                4096
            ):
                pass
        except (
            BlockingIOError,
            OSError,
        ):
            pass

    def set_error(self, error):
        if error is not None:
            self.error = OperationalError(
                str(error)
            )

    def close(self):
        try:
            if self.injector is not None:
                self.command(
                    "close"
                )
        except Exception:
            logger.debug(
                "Error closing Proton reactor",
                exc_info=True,
            )

        if self.thread.is_alive():
            self.thread.join(
                timeout=self.command_timeout
            )

        for sock in (
            self.read_sock,
            self.write_sock,
        ):
            try:
                sock.close()
            except OSError:
                pass


class _RabbitMQManagement:
    """RabbitMQ topology management over AMQP 1.0."""

    def __init__(self, channel):
        self.channel = channel

    def request(
        self,
        body,
        path,
        method,
        expected_codes,
    ):
        return self.channel._command(
            "management",
            body,
            path,
            method,
            tuple(expected_codes),
        )

    def declare_queue(
        self,
        queue,
        durable=False,
        exclusive=False,
        auto_delete=False,
        arguments=None,
        passive=False,
    ):
        path = self.channel._queue_address(
            queue
        )

        if passive:
            return self.request(
                None,
                path,
                _MANAGEMENT_GET,
                (200,),
            )

        body = {
            "durable": durable,
            "exclusive": exclusive,
            "auto_delete": auto_delete,
            "arguments": arguments or {},
        }

        return self.request(
            body,
            path,
            _MANAGEMENT_PUT,
            (200, 201, 204),
        )

    def delete_queue(self, queue):
        return self.request(
            None,
            self.channel._queue_address(queue),
            _MANAGEMENT_DELETE,
            (200,),
        )

    def declare_exchange(
        self,
        exchange,
        exchange_type="direct",
        durable=False,
        auto_delete=False,
        internal=False,
        arguments=None,
    ):
        body = {
            "durable": durable,
            "type": exchange_type,
            "auto_delete": auto_delete,
            "internal": internal,
            "arguments": arguments or {},
        }

        return self.request(
            body,
            self.channel._exchange_address(
                exchange
            ),
            _MANAGEMENT_PUT,
            (201, 204),
        )

    def delete_exchange(self, exchange):
        return self.request(
            None,
            self.channel._exchange_address(
                exchange
            ),
            _MANAGEMENT_DELETE,
            (204,),
        )

    def bind(
        self,
        queue,
        exchange,
        routing_key,
        arguments=None,
    ):
        body = {
            "source": exchange,
            "destination_queue": queue,
            "binding_key": (
                routing_key
                if routing_key is not None
                else ""
            ),
            "arguments": arguments or {},
        }

        return self.request(
            body,
            "/bindings",
            _MANAGEMENT_POST,
            (204,),
        )

    def unbind(
        self,
        queue,
        exchange,
        routing_key,
        arguments=None,
    ):
        key = (
            routing_key
            if routing_key is not None
            else ""
        )

        binding_path = (
            "/bindings/"
            f"src={self.channel._encode_address_part(exchange)};"
            f"dstq={self.channel._encode_address_part(queue)};"
            f"key={self.channel._encode_address_part(key)};"
            "args="
        )

        return self.request(
            None,
            binding_path,
            _MANAGEMENT_DELETE,
            (204,),
        )


class Channel(base.StdChannel):
    """Kombu native channel backed by Proton."""

    QoS = QoS
    Message = Message

    body_encoding = "base64"

    codecs = {
        "base64": Base64(),
    }

    def __init__(
        self,
        connection,
        transport,
    ):
        self.connection = connection
        self.transport = transport
        self.state = connection.state
        self.closed = False

        self.command_timeout = (
            transport.command_timeout
        )

        self.qos = self.QoS(
            self,
            transport.prefetch_count,
        )

        self._consumers = {}
        self._queues = set()
        self._exchanges = set()
        self._bindings = set()

        self._management = (
            _RabbitMQManagement(self)
        )

    def _command(
        self,
        name,
        *args,
        **kwargs,
    ):
        return self.state.command(
            name,
            *args,
            **kwargs,
        )

    def _encode_address_part(
        self,
        value,
    ):
        return quote(
            value or "",
            safe="",
        )

    def _queue_address(
        self,
        queue,
    ):
        return (
            "/queues/"
            f"{self._encode_address_part(queue)}"
        )

    def _exchange_address(
        self,
        exchange,
        routing_key="",
    ):
        exchange = (
            self._encode_address_part(
                exchange
            )
        )

        if routing_key:
            routing_key = (
                self._encode_address_part(
                    routing_key
                )
            )

            return (
                f"/exchanges/{exchange}/"
                f"{routing_key}"
            )

        return (
            f"/exchanges/{exchange}"
        )

    def _publish_address(
        self,
        exchange,
        routing_key,
    ):
        if exchange:
            return self._exchange_address(
                exchange,
                routing_key,
            )

        return self._queue_address(
            routing_key
        )

    def prepare_message(
        self,
        body,
        priority=None,
        content_type=None,
        content_encoding=None,
        headers=None,
        properties=None,
    ):
        properties = dict(
            properties or {}
        )

        properties["headers"] = dict(
            headers or {}
        )

        if priority is None:
            priority = 4

        return ProtonMessage(
            body=body,
            properties=properties,
            content_type=content_type,
            content_encoding=content_encoding,
            priority=priority,
        )

    def message_to_python(
        self,
        raw_message,
    ):
        return self.Message(
            raw_message,
            channel=self,
        )

    def basic_publish(
        self,
        message,
        exchange,
        routing_key,
        **kwargs,
    ):
        address = self._publish_address(
            exchange,
            routing_key,
        )

        if isinstance(
            message,
            Message,
        ):
            proton_message = (
                message.proton_message
            )
        elif isinstance(
            message,
            ProtonMessage,
        ):
            proton_message = message
        else:
            proton_message = (
                self.prepare_message(
                    getattr(
                        message,
                        "body",
                        None,
                    ),
                    content_type=getattr(
                        message,
                        "content_type",
                        None,
                    ),
                    content_encoding=getattr(
                        message,
                        "content_encoding",
                        None,
                    ),
                    headers=getattr(
                        message,
                        "headers",
                        None,
                    ),
                    properties=getattr(
                        message,
                        "properties",
                        None,
                    ),
                )
            )

        self._command(
            "send",
            proton_message,
            address,
        )

    def basic_consume(
        self,
        queue,
        no_ack,
        callback,
        consumer_tag,
        **kwargs,
    ):
        prefetch = (
            kwargs.get("prefetch_count")
            or self.qos.prefetch_count
            or 1
        )

        self._queues.add(queue)

        self._consumers[
            consumer_tag
        ] = (
            queue,
            no_ack,
            callback,
        )

        self._command(
            "consume",
            self._queue_address(queue),
            queue,
            consumer_tag,
            prefetch,
        )

    def basic_cancel(
        self,
        consumer_tag,
    ):
        self._consumers.pop(
            consumer_tag,
            None,
        )

        self._command(
            "cancel",
            consumer_tag,
        )

    def basic_ack(
        self,
        delivery_tag,
        multiple=False,
    ):
        if multiple:
            raise AssertionError(
                "multiple acknowledgements are "
                "not supported"
            )

        self.qos.ack(
            delivery_tag
        )

    def basic_reject(
        self,
        delivery_tag,
        requeue=False,
    ):
        self.qos.reject(
            delivery_tag,
            requeue=requeue,
        )

    def basic_get(
        self,
        queue,
        no_ack=False,
        **kwargs,
    ):
        received = self._command(
            "get",
            self._queue_address(queue),
            queue,
        )

        if received is None:
            return None

        delivery_tag = str(
            uuid.uuid4()
        )

        message = self.Message(
            received.message,
            channel=self,
            delivery=received.delivery,
            delivery_tag=delivery_tag,
            delivery_info={
                "exchange": "",
                "routing_key": queue,
                "consumer_tag": (
                    received.consumer_tag
                ),
            },
        )

        if no_ack:
            self._command(
                "accept",
                received.delivery,
            )
        else:
            self.qos.append(
                received.delivery,
                delivery_tag,
            )

        return message

    def queue_declare(
        self,
        queue,
        passive=False,
        durable=False,
        exclusive=False,
        auto_delete=True,
        nowait=False,
        arguments=None,
    ):
        arguments = dict(
            arguments or {}
        )

        if passive:
            self._management.declare_queue(
                queue,
                passive=True,
            )
        else:
            self._management.declare_queue(
                queue,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments,
            )

        self._queues.add(queue)

        return amqp.protocol.queue_declare_ok_t(
            queue,
            0,
            0,
        )

    def queue_delete(
        self,
        queue,
        if_unused=False,
        if_empty=False,
        **kwargs,
    ):
        try:
            self._management.delete_queue(
                queue
            )
        finally:
            self._queues.discard(queue)

            self._bindings = {
                binding
                for binding in self._bindings
                if binding[0] != queue
            }

    def exchange_declare(
        self,
        exchange="",
        type="direct",
        durable=False,
        auto_delete=False,
        internal=False,
        arguments=None,
        **kwargs,
    ):
        if not exchange:
            return

        self._management.declare_exchange(
            exchange,
            exchange_type=type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            arguments=arguments,
        )

        self._exchanges.add(exchange)

    def exchange_delete(
        self,
        exchange,
        **kwargs,
    ):
        try:
            self._management.delete_exchange(
                exchange
            )
        finally:
            self._exchanges.discard(
                exchange
            )

            self._bindings = {
                binding
                for binding in self._bindings
                if binding[1] != exchange
            }

    def queue_bind(
        self,
        queue,
        exchange,
        routing_key,
        arguments=None,
        **kwargs,
    ):
        self._management.bind(
            queue,
            exchange,
            routing_key,
            arguments=arguments,
        )

        self._queues.add(queue)
        self._exchanges.add(exchange)

        self._bindings.add(
            (
                queue,
                exchange,
                routing_key,
            )
        )

    def queue_unbind(
        self,
        queue,
        exchange,
        routing_key,
        arguments=None,
        **kwargs,
    ):
        self._management.unbind(
            queue,
            exchange,
            routing_key,
            arguments=arguments,
        )

        self._bindings.discard(
            (
                queue,
                exchange,
                routing_key,
            )
        )

    def queue_purge(
        self,
        queue,
        **kwargs,
    ):
        raise OperationalError(
            "AMQP 1.0 queue purge is not "
            "portable; RabbitMQ management "
            "purge is not implemented yet"
        )

    def close(self):
        if self.closed:
            return

        for consumer_tag in list(
            self._consumers
        ):
            try:
                self.basic_cancel(
                    consumer_tag
                )
            except Exception:
                logger.debug(
                    "Unable to cancel Proton "
                    "consumer",
                    exc_info=True,
                )

        self.qos.restore_unacked()

        self.closed = True

    def encode_body(
        self,
        body,
        encoding=None,
    ):
        if encoding:
            return (
                self.codecs[encoding].encode(
                    body
                ),
                encoding,
            )

        return body, encoding

    def decode_body(
        self,
        body,
        encoding=None,
    ):
        if encoding:
            return (
                self.codecs[encoding].decode(
                    body
                ),
                encoding,
            )

        return body, encoding

    def typeof(
        self,
        exchange,
        default="direct",
    ):
        return default


class Connection:
    """Kombu-facing Proton connection."""

    Channel = Channel

    def __init__(
        self,
        state,
        client,
    ):
        self.state = state
        self.client = client
        self.channels = []
        self.connected = True

    def channel(self):
        channel = self.Channel(
            self,
            self.state.transport,
        )

        self.channels.append(channel)

        return channel

    def close(self):
        if self.connected:
            self.state.close()
            self.connected = False


class Transport(base.Transport):
    """Apache Qpid Proton AMQP 1.0 transport."""

    Connection = Connection
    Channel = Channel

    default_port = DEFAULT_PORT
    default_ssl_port = DEFAULT_SSL_PORT

    driver_type = "amqp"
    driver_name = "qpid-proton"

    connection_errors = (
        OperationalError,
        OSError,
    )

    channel_errors = (
        OperationalError,
    )

    implements = base.Transport.implements.extend(
        asynchronous=True,
        heartbeats=True,
    )

    def __init__(
        self,
        client,
        default_port=None,
        default_ssl_port=None,
        **kwargs,
    ):
        if proton is None:
            raise ImportError(
                "python-qpid-proton is required "
                "for the proton transport"
            )

        super().__init__(
            client,
            **kwargs,
        )

        self.default_port = (
            default_port or self.default_port
        )

        self.default_ssl_port = (
            default_ssl_port
            or self.default_ssl_port
        )

        options = dict(
            client.transport_options or {}
        )

        self.command_timeout = options.pop(
            "command_timeout",
            10.0,
        )

        self.prefetch_count = options.pop(
            "prefetch_count",
            DEFAULT_PREFETCH,
        )

        self.address_template = options.pop(
            "address_template",
            "{routing_key}",
        )

        self.transport_options = options

    def driver_version(self):
        return version(
            "python-qpid-proton"
        )

    @property
    def default_connection_params(self):
        return {
            "hostname": "localhost",
            "port": (
                self.default_ssl_port
                if self.client.ssl
                else self.default_port
            ),
        }

    def _url(self):
        hostname = (
            self.client.hostname
            or "localhost"
        )

        port = (
            self.client.port
            or (
                self.default_ssl_port
                if self.client.ssl
                else self.default_port
            )
        )

        scheme = (
            "amqps"
            if self.client.ssl
            else "amqp"
        )

        return (
            f"{scheme}://"
            f"{hostname}:{port}"
        )

    def _ssl_domain(self):
        ssl = self.client.ssl

        if not ssl:
            return None

        if ssl is True:
            ssl = {}

        domain = SSLDomain(
            SSLDomain.MODE_CLIENT
        )

        ca = (
            ssl.get("ca_certs")
            or ssl.get("cafile")
            or ssl.get("ca")
        )

        cert = (
            ssl.get("certfile")
            or ssl.get("cert")
        )

        key = (
            ssl.get("keyfile")
            or ssl.get("key")
        )

        password = ssl.get(
            "password"
        )

        if cert and key:
            domain.set_credentials(
                cert,
                key,
                password,
            )

        if ca:
            domain.set_trusted_ca_db(
                ca
            )

        if ssl.get(
            "cert_reqs",
            True,
        ) is False:
            domain.set_peer_authentication(
                SSLDomain.ANONYMOUS_PEER
            )
        else:
            domain.set_peer_authentication(
                SSLDomain.VERIFY_PEER_NAME
            )

        return domain

    def establish_connection(self):
        password = self.client.password

        if callable(password):
            password = password()

        options = dict(
            self.transport_options
        )

        options.update(
            {
                "user": self.client.userid,
                "password": password,
                "virtual_host": (
                    self.client.virtual_host
                ),
                "sasl_enabled": options.pop(
                    "sasl_enabled",
                    True,
                ),
            }
        )

        if self.client.login_method:
            options.setdefault(
                "allowed_mechs",
                self.client.login_method,
            )

        state = _ProtonState(
            url=self._url(),
            connect_kwargs=options,
            ssl_domain=self._ssl_domain(),
            command_timeout=(
                self.command_timeout
            ),
        )

        state.transport = self

        state.start()

        return Connection(
            state,
            self.client,
        )

    def create_channel(
        self,
        connection,
    ):
        return connection.channel()

    def close_connection(
        self,
        connection,
    ):
        connection.close()

    def drain_events(
        self,
        connection,
        timeout=None,
        **kwargs,
    ):
        state = connection.state

        if state.error:
            raise state.error

        deadline = (
            None
            if timeout is None
            else monotonic() + timeout
        )

        while True:
            try:
                received = (
                    state.incoming.get_nowait()
                )
            except queue.Empty:
                if (
                    deadline is not None
                    and monotonic() >= deadline
                ):
                    raise socket.timeout()

                wait = 0.1

                if deadline is not None:
                    wait = max(
                        0,
                        min(
                            0.1,
                            deadline
                            - monotonic(),
                        ),
                    )

                try:
                    state.read_sock.settimeout(
                        wait
                    )
                    state.read_sock.recv(1)
                except (
                    socket.timeout,
                    BlockingIOError,
                ):
                    pass
                finally:
                    state.read_sock.setblocking(
                        False
                    )

                continue

            channel = next(
                (
                    ch
                    for ch in connection.channels
                    if (
                        received.consumer_tag
                        in ch._consumers
                    )
                ),
                None,
            )

            if channel is None:
                continue

            message = channel.Message(
                received.message,
                channel=channel,
                delivery=received.delivery,
                delivery_tag=str(
                    uuid.uuid4()
                ),
                delivery_info={
                    "exchange": "",
                    "routing_key": (
                        received.queue
                    ),
                    "consumer_tag": (
                        received.consumer_tag
                    ),
                },
            )

            (
                queue_name,
                no_ack,
                callback,
            ) = channel._consumers[
                received.consumer_tag
            ]

            if not no_ack:
                channel.qos.append(
                    received.delivery,
                    message.delivery_tag,
                )
            else:
                received.delivery.accept()
                received.delivery.settle()

            callback(
                message.body,
                message,
            )

            return message

    def register_with_event_loop(
        self,
        connection,
        loop,
    ):
        loop.add_reader(
            connection.state.read_sock,
            self._on_readable,
            connection,
        )

    def unregister_from_event_loop(
        self,
        connection,
        loop,
    ):
        try:
            loop.remove_reader(
                connection.state.read_sock
            )
        except Exception:
            pass

    def _on_readable(
        self,
        connection,
    ):
        connection.state.drain_notifications()

    def verify_connection(
        self,
        connection,
    ):
        return (
            connection.connected
            and connection.state.error is None
        )

    def get_heartbeat_interval(
        self,
        connection,
    ):
        return (
            self.client.heartbeat
            or 0
        )

    def heartbeat_check(
        self,
        connection,
        rate=2,
    ):
        return None

    def qos_semantics_matches_spec(
        self,
        connection,
    ):
        return True