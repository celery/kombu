"""Qpid Transport.

`Qpid`_ transport using `qpid-python`_ as the client and `qpid-tools`_ for
broker management.

The use this transport you must install the necessary dependencies. These
dependencies are available via PyPI and can be installed using the pip
command:

.. code-block:: console

    $ pip install kombu[qpid]

or to install the requirements manually:

.. code-block:: console

    $ pip install qpid-tools qpid-python

.. admonition:: Python 3 and PyPy Limitations

    The Qpid transport does not support Python 3 or PyPy environments due
    to underlying dependencies not being compatible. This version is
    tested and works with with Python 2.7.

.. _`Qpid`: https://qpid.apache.org/
.. _`qpid-python`: https://pypi.python.org/pypi/qpid-python/
.. _`qpid-tools`: https://pypi.python.org/pypi/qpid-tools/

Authentication
==============

This transport supports SASL authentication with the Qpid broker. Normally,
SASL mechanisms are negotiated from a client list and a server list of
possible mechanisms, but in practice, different SASL client libraries give
different behaviors. These different behaviors cause the expected SASL
mechanism to not be selected in many cases. As such, this transport restricts
the mechanism types based on Kombu's configuration according to the following
table.

+------------------------------------+--------------------+
| **Broker String**                  | **SASL Mechanism** |
+------------------------------------+--------------------+
| qpid://hostname/                   | ANONYMOUS          |
+------------------------------------+--------------------+
| qpid://username:password@hostname/ | PLAIN              |
+------------------------------------+--------------------+
| see instructions below             | EXTERNAL           |
+------------------------------------+--------------------+

The user can override the above SASL selection behaviors and specify the SASL
string using the :attr:`~kombu.Connection.login_method` argument to the
:class:`~kombu.Connection` object. The string can be a single SASL mechanism
or a space separated list of SASL mechanisms. If you are using Celery with
Kombu, this can be accomplished by setting the *BROKER_LOGIN_METHOD* Celery
option.

.. note::

    While using SSL, Qpid users may want to override the SASL mechanism to
    use *EXTERNAL*. In that case, Qpid requires a username to be presented
    that matches the *CN* of the SSL client certificate. Ensure that the
    broker string contains the corresponding username. For example, if the
    client certificate has *CN=asdf* and the client connects to *example.com*
    on port 5671, the broker string should be:

        **qpid://asdf@example.com:5671/**

Transport Options
=================

The :attr:`~kombu.Connection.transport_options` argument to the
:class:`~kombu.Connection` object are passed directly to the
:class:`proton.util.` as keyword arguments. These
options override and replace any other default or specified values. If using
Celery, this can be accomplished by setting the
*BROKER_TRANSPORT_OPTIONS* Celery option.
"""
from __future__ import absolute_import, unicode_literals

from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import object
from collections import namedtuple, OrderedDict
import copy
from gettext import gettext as _
import os
import select
import socket
import sys
import time
import threading
import uuid

import queue

import amqp.protocol

try:
    import fcntl
except ImportError:
    fcntl = None  # noqa

try:
    from qmf.client import BrokerAgent, ReconnectDelays
except ImportError:  # pragma: no cover
    BrokerAgent = None     # noqa
    ReconnectDelays = None  # noqa

try:
    import qpid
except ImportError:  # pragma: no cover
    qpid = None

try:
    import proton
    from proton.handlers import MessagingHandler
    from proton.reactor import Container
    from proton.utils import ConnectionClosed, ConnectionException
except ImportError:  # pragma: no cover
    proton = None
    MessagingHandler = object
    Container = None
    ConnectionClosed = None
    ConnectionException = None


from kombu.five import Empty, items, monotonic
from kombu.log import get_logger
from kombu.transport.virtual import Base64, Message
from kombu.transport import base

logger = get_logger(__name__)


OBJECT_ALREADY_EXISTS_STRING = 'object already exists'

PROTON_PERIOD = 0.25 # The waiting period between ProtonThread check-ins


# Objects used to send "commands" between MainThread and ProtonThread.

# Start an asynchronous Proton consumer listening on 'queue'.
StartConsumer = namedtuple('StartConsumer', ['queue'])

# Get one message from 'queue' and return it via the 'return_queue'.
GetOneMessage = namedtuple('GetOneMessage', ['queue', 'return_queue'])

# Send 'message' to 'target'. When sending is complete MainThread is notified
# via 'send_complete'.
SendMessage = namedtuple('SendMessage',
                         ['message', 'target', 'send_complete'])

# A message received from an asynchronous consumer started with
# 'StartConsumer'. It stores the 'message', its 'delivery' string and the
# 'queue' it was received from.
ReceivedMessage = namedtuple('ReceivedMessage', ['message', 'delivery', 'queue'])

# Acknowledge the message associated with 'delivery'. Notify MainThread via
# 'ack_complete' when done.
AckMessage = namedtuple('AckMessage', ['delivery', 'ack_complete'])

# Reject the message associated with 'delivery'. Notify MainThread via
# 'reject_complete' when done.
RejectMessage = namedtuple('RejectMessage', ['delivery', 'reject_complete'])

# Release the message associated with 'delivery'. Notify MainThread via
# 'release_complete' when done.
ReleaseMessage = namedtuple('ReleaseMessage',
                            ['delivery', 'release_complete'])


def dependency_is_none(dependency):
    """Return True if the dependency is None, otherwise False.

    This is done using a function so that tests can mock this
    behavior easily.

    :param dependency: The module to check if it is None
    :return: True if dependency is None otherwise False.

    """
    return dependency is None


class AuthenticationFailure(Exception):
    """Cannot authenticate with Qpid."""


class SASL_obj(object):
    pass


class QoS(object):
    """A helper object for message prefetch and ACKing purposes.

    :ivar prefetch_count: Initial prefetch count, hard set to 1.
    :type prefetch_count: int

    NOTE: prefetch_count is currently hard set to 1, and needs to be improved

    Messages are added using :meth:`append`, which are held until they are
    ACKed asynchronously through a call to :meth:`ack`. Messages that are
    received, but not ACKed will not be delivered by the broker to another
    consumer until an ACK is received, or the session is closed. Messages
    are referred to using delivery_tag, which are unique per
    :class:`Channel`. Delivery tags are generated outside of this object and
    are passed in with a message to :meth:`append`. Un-ACKed messages can
    be looked up from QoS using :meth:`get` and can be rejected and
    forgotten using :meth:`reject`.

    One QoS object is instantiated with each
    :class:`~.kombu.transport.qpid.Channel` instance.
    """

    def __init__(self, main_thread_commands):
        self.main_thread_commands = main_thread_commands
        self.prefetch_count = 1
        self._not_yet_acked = OrderedDict()

    def can_consume(self):
        """Return True if more messages can be consumed, otherwise False.

        Used to ensure the client adheres to currently active prefetch
        limits.

        :returns: True, if this QoS object can accept more messages
            without violating the prefetch_count. If prefetch_count is 0,
            can_consume will always return True.
        :rtype: bool

        """
        return (
            not self.prefetch_count or
            len(self._not_yet_acked) < self.prefetch_count
        )

    def can_consume_max_estimate(self):
        """Return the maximum remaining message capacity.

        Returns an estimated number of outstanding messages that can be
        accepted without exceeding ``prefetch_count``. If ``prefetch_count``
        is 0, then this method returns 1.

        :returns: The number of estimated messages that can be fetched
            without violating the prefetch_count.
        :rtype: int

        """
        return 1 if not self.prefetch_count else (
            self.prefetch_count - len(self._not_yet_acked)
        )

    def append(self, delivery, delivery_tag):
        """Append message delivery to the list of un-ACKed message deliveries.

        Add a message delivery, referenced by the delivery_tag, for ACKing,
        or rejecting later. Messages are saved into an
        :class:`collections.OrderedDict` by delivery_tag.

        :param delivery: An un-acked message Delivery
        :type delivery: proton.Delivery
        :param delivery_tag: A UUID to refer to this message by
            upon receipt.
        :type delivery_tag: uuid.UUID

        """
        self._not_yet_acked[delivery_tag] = delivery

    def ack(self, delivery_tag):
        """Acknowledge a message delivery by delivery_tag.

        Acknowledge the message delivery and remove it from the QoS object.

        :param delivery_tag: the delivery tag associated with the message
            to be acknowledged.
        :type delivery_tag: uuid.UUID

        """
        delivery = self._not_yet_acked.pop(delivery_tag)
        ack_complete = threading.Event()
        cmd = AckMessage(delivery=delivery, ack_complete=ack_complete)
        self.main_thread_commands.put(cmd)
        ack_complete.wait()

    def reject(self, delivery_tag, requeue=False):
        """Reject a message delivery by delivery_tag.

        Explicitly notify the broker that this message is being rejected and
        remove it from the QoS object.

        If requeue is False, then the message is not requeued for delivery
        to another consumer. If requeue is True, then the message is
        requeued for delivery to another consumer.

        :param delivery_tag: The delivery tag associated with the message
            to be rejected.
        :type delivery_tag: uuid.UUID

        :keyword requeue: If True, the broker will be notified to requeue
            the message. If False, the broker will be told to drop the
            message entirely. In both cases, the message will be removed
            from this object.
        :type requeue: bool

        """
        delivery = self._not_yet_acked.pop(delivery_tag)
        event_complete = threading.Event()
        if requeue:
            cmd = ReleaseMessage(delivery=delivery,
                                 release_complete=event_complete)
        else:
            cmd = RejectMessage(delivery=delivery,
                                reject_complete=event_complete)
        self.main_thread_commands.put(cmd)
        event_complete.wait()


class Channel(base.StdChannel):
    """Supports broker configuration and messaging send and receive.

    :ivar connection: A Connection object that this Channel can
        reference. Currently only used to access callbacks.
    :type connection: kombu.transport.qpid.Connection
    :ivar transport: The Transport this Channel is associated with.
    :type transport: kombu.transport.qpid.Transport

    This Channel object is designed to have method-parity with a Channel as
    defined in AMQP 0-10 and earlier, which allows for the following broker
    actions:

        - exchange declare and delete
        - queue declare and delete
        - queue bind and unbind operations
        - queue length and purge operations
        - sending/receiving/rejecting messages
        - structuring, encoding, and decoding messages
        - supports synchronous and asynchronous reads
        - reading state about the exchange, queues, and bindings

    Channels are designed to all share a single TCP connection with a
    broker, and provide a level of isolated communication. The Channel is
    given its :class:`~kombu.transport.qpid.Connection` object by the
    :class:`~kombu.transport.qpid.Transport` that
    instantiates the channel.

    This Channel inherits from :class:`~kombu.transport.base.StdChannel`,
    which makes this a 'native' channel versus a 'virtual' channel which
    would inherit from :class:`kombu.transports.virtual`.

    Messages sent using this Channel are assigned a delivery_tag. The
    delivery_tag is generated for a message as they are prepared for
    sending by :meth:`basic_publish`. The delivery_tag is unique per
    channel instance. The delivery_tag has no meaningful context in other
    objects, and is only maintained in the memory of this object, and the
    underlying :class:`QoS` object.

    Synchronous reads on a queue are done using a call to :meth:`basic_get`
    Asynchronous reads on a queue are done by starting a consumer using
    :meth:`basic_consume`.

    Each call to :meth:`basic_consume` creates a consumer, which is given a
    consumer tag that is identified by the caller of :meth:`basic_consume`.
    Already started consumers can be cancelled using by their consumer_tag
    using :meth:`basic_cancel`.

    Asynchronous message ACKing is supported through :meth:`basic_ack`,
    and is referenced by delivery_tag.

    """

    #: A class reference that will be instantiated using the qos property.
    QoS = QoS

    #: A class reference that identifies
    # :class:`~kombu.transport.virtual.Message` as the message class type
    Message = Message

    #: Default body encoding.
    #: NOTE: ``transport_options['body_encoding']`` will override this value.
    body_encoding = 'base64'

    #: Binary <-> ASCII codecs.
    codecs = {'base64': Base64()}

    def __init__(self, connection, transport):
        self.connection = connection
        self.transport = transport
        self._broker = connection.get_broker_agent()
        self.closed = False
        self._tag_to_queue = {}
        self._receivers = {}
        self.qos = self.QoS(transport.main_thread_commands)

    def _make_kombu_message_from_proton(self, proton_message):
        """
        Make and return a Kombu Message object from a Proton message object

        :param proton_message: The Proton message to be used
        :type proton_message: :class: `proton.Message`

        :return: A new kombu message
        :rtype: :class:`kombu.transport.virtual.Message`
        """
        pm = proton_message
        payload = {'body': pm.body,
                   'properties': pm.properties,
                   'content-encoding': pm.content_encoding,
                   'content-type': pm.content_type,
                   'headers': pm.properties.pop('headers', {})}
        return self.Message(payload, channel=self)

    def _size(self, queue):
        """Get the number of messages in a queue specified by name.

        An internal method to return the number of messages in a queue
        specified by name. It returns an integer count of the number
        of messages currently in the queue.

        :param queue: The name of the queue to be inspected for the number
            of messages
        :type queue: str

        :return the number of messages in the queue specified by name.
        :rtype: int

        """
        queue_to_check = self._broker.getQueue(queue)
        message_depth = queue_to_check.values['msgDepth']
        return message_depth

    def queue_declare(self, queue, passive=False, durable=False,
                      exclusive=False, auto_delete=True, nowait=False,
                      arguments=None):
        """Create a new queue specified by name.

        If the queue already exists, no change is made to the queue,
        and the return value returns information about the existing queue.

        The queue name is required and specified as the first argument.

        If passive is True, the server will not create the queue. The
        client can use this to check whether a queue exists without
        modifying the server state. Default is False.

        If durable is True, the queue will be durable. Durable queues
        remain active when a server restarts. Non-durable queues (
        transient queues) are purged if/when a server restarts. Note that
        durable queues do not necessarily hold persistent messages,
        although it does not make sense to send persistent messages to a
        transient queue. Default is False.

        If exclusive is True, the queue will be exclusive. Exclusive queues
        may only be consumed by the current connection. Setting the
        'exclusive' flag always implies 'auto-delete'. Default is False.

        If auto_delete is True,  the queue is deleted when all consumers
        have finished using it. The last consumer can be cancelled either
        explicitly or because its channel is closed. If there was no
        consumer ever on the queue, it won't be deleted. Default is True.

        The nowait parameter is unused. It was part of the 0-9-1 protocol,
        but this AMQP client implements 0-10 which removed the nowait option.

        The arguments parameter is a set of arguments for the declaration of
        the queue. Arguments are passed as a dict or None. This field is
        ignored if passive is True. Default is None.

        This method returns a :class:`~collections.namedtuple` with the name
        'queue_declare_ok_t' and the queue name as 'queue', message count
        on the queue as 'message_count', and the number of active consumers
        as 'consumer_count'. The named tuple values are ordered as queue,
        message_count, and consumer_count respectively.

        Due to Celery's non-ACKing of events, a ring policy is set on any
        queue that starts with the string 'celeryev' or ends with the string
        'pidbox'. These are celery event queues, and Celery does not ack
        them, causing the messages to build-up. Eventually Qpid stops serving
        messages unless the 'ring' policy is set, at which point the buffer
        backing the queue becomes circular.

        :param queue: The name of the queue to be created.
        :type queue: str
        :param passive: If True, the sever will not create the queue.
        :type passive: bool
        :param durable: If True, the queue will be durable.
        :type durable: bool
        :param exclusive: If True, the queue will be exclusive.
        :type exclusive: bool
        :param auto_delete: If True, the queue is deleted when all
            consumers have finished using it.
        :type auto_delete: bool
        :param nowait: This parameter is unused since the 0-10
            specification does not include it.
        :type nowait: bool
        :param arguments: A set of arguments for the declaration of the
            queue.
        :type arguments: dict or None

        :return: A named tuple representing the declared queue as a named
            tuple. The tuple values are ordered as queue, message count,
            and the active consumer count.
        :rtype: :class:`~collections.namedtuple`

        """
        options = {'passive': passive,
                   'durable': durable,
                   'exclusive': exclusive,
                   'auto-delete': auto_delete,
                   'arguments': arguments}
        if queue.startswith('celeryev') or queue.endswith('pidbox'):
            options['qpid.policy_type'] = 'ring'
        try:
            self._broker.addQueue(queue, options=options)
        except Exception as exc:
            if OBJECT_ALREADY_EXISTS_STRING not in str(exc):
                raise exc
        queue_to_check = self._broker.getQueue(queue)
        message_count = queue_to_check.values['msgDepth']
        consumer_count = queue_to_check.values['consumerCount']
        return amqp.protocol.queue_declare_ok_t(queue, message_count,
                                                consumer_count)

    def queue_delete(self, queue, if_unused=False, if_empty=False, **kwargs):
        """Delete a queue by name.

        When the if_unused keyword is True, only delete if there are 0
        consumers bound to the queue.

        When the if_empty keyword is True, only delete if there are 0 messages
        in the queue.

        :param queue: The name of the queue to be deleted.
        :type queue: str

        :keyword if_unused: If True, delete only if the queue has 0
            consumers. If False, delete a queue even with consumers bound
            to it.
        :type if_unused: bool

        :keyword if_empty: If True, only delete the queue if it is empty. If
            False, delete the queue if it is empty or not.
        :type if_empty: bool

        """
        queue_obj = self._broker.getQueue(queue)
        if queue_obj:
            if if_empty and self._size(queue):
                return
            consumer_count = queue_obj.getAttributes()['consumerCount']
            if if_unused and consumer_count > 0:
                return
            self.queue_purge(queue)
            self._broker.delQueue(queue)

    def exchange_declare(self, exchange='', type='direct', durable=False,
                         **kwargs):
        """Create a new exchange.

        Create an exchange of a specific type, and optionally have the
        exchange be durable. If an exchange of the requested name already
        exists, no action is taken and no exceptions are raised. Durable
        exchanges will survive a broker restart, non-durable exchanges will
        not.

        Exchanges provide behaviors based on their type. The expected
        behaviors are those defined in the AMQP 0-10 and prior
        specifications including 'direct', 'topic', and 'fanout'
        functionality.

        :keyword type: The exchange type. Valid values include 'direct',
            'topic', and 'fanout'.
        :type type: str
        :keyword exchange: The name of the exchange to be created. If no
            exchange is specified, then a blank string will be used as the
            name.
        :type exchange: str
        :keyword durable: True if the exchange should be durable, or False
            otherwise.
        :type durable: bool

        """
        options = {'durable': durable}
        try:
            self._broker.addExchange(type, exchange, options)
        except Exception as exc:
            if OBJECT_ALREADY_EXISTS_STRING not in str(exc):
                raise exc

    def exchange_delete(self, exchange_name, **kwargs):
        """Delete an exchange specified by name.

        :param exchange_name: The name of the exchange to be deleted.
        :type exchange_name: str

        """
        self._broker.delExchange(exchange_name)

    def queue_bind(self, queue, exchange, routing_key, **kwargs):
        """Bind a queue to an exchange with a bind key.

        Bind a queue specified by name, to an exchange specified by name,
        with a specific bind key. The queue and exchange must already
        exist on the broker for the bind to complete successfully. Queues
        may be bound to exchanges multiple times with different keys.

        :param queue: The name of the queue to be bound.
        :type queue: str
        :param exchange: The name of the exchange that the queue should be
            bound to.
        :type exchange: str
        :param routing_key: The bind key that the specified queue should
            bind to the specified exchange with.
        :type routing_key: str

        """
        self._broker.bind(exchange, queue, routing_key)

    def queue_unbind(self, queue, exchange, routing_key, **kwargs):
        """Unbind a queue from an exchange with a given bind key.

        Unbind a queue specified by name, from an exchange specified by
        name, that is already bound with a bind key. The queue and
        exchange must already exist on the broker, and bound with the bind
        key for the operation to complete successfully. Queues may be
        bound to exchanges multiple times with different keys, thus the
        bind key is a required field to unbind in an explicit way.

        :param queue: The name of the queue to be unbound.
        :type queue: str
        :param exchange: The name of the exchange that the queue should be
            unbound from.
        :type exchange: str
        :param routing_key: The existing bind key between the specified
            queue and a specified exchange that should be unbound.
        :type routing_key: str

        """
        self._broker.unbind(exchange, queue, routing_key)

    def queue_purge(self, queue, **kwargs):
        """Remove all undelivered messages from queue.

        Purge all undelivered messages from a queue specified by name. If the
        queue does not exist an exception is raised. The queue message
        depth is first checked, and then the broker is asked to purge that
        number of messages. The integer number of messages requested to be
        purged is returned. The actual number of messages purged may be
        different than the requested number of messages to purge.

        Sometimes delivered messages are asked to be purged, but are not.
        This case fails silently, which is the correct behavior when a
        message that has been delivered to a different consumer, who has
        not ACKed the message, and still has an active session with the
        broker. Messages in that case are not safe for purging and will be
        retained by the broker. The client is unable to change this
        delivery behavior.

        :param queue: The name of the queue which should have all messages
            removed.
        :type queue: str

        :return: The number of messages requested to be purged.
        :rtype: int
        """
        queue_to_purge = self._broker.getQueue(queue)
        if queue_to_purge is None:
            error_text = "NOT_FOUND - no queue '{0}'".format(queue)
            raise NotFound(code=404, text=error_text)
        message_count = queue_to_purge.values['msgDepth']
        if message_count > 0:
            queue_to_purge.purge(message_count)
        return message_count

    def basic_get(self, queue, no_ack=False, **kwargs):
        """Non-blocking, returns a message, or None.

        :param queue: The queue name to fetch a message from.
        :type queue: str
        :keyword no_ack: The no_ack parameter has no effect on the ACK
            behavior of this method.
        :type noack: bool

        :return: The received message.
        :rtype: :class:`~kombu.transport.virtual.Message`

        """
        return_queue = queue.Queue()
        cmd = GetOneMessage(queue, return_queue)
        self.transport.main_thread_commands.put(cmd)
        message_and_delivery = return_queue.get()
        if message_and_delivery is None:
            return
        message = self._make_kombu_message_from_proton(
            message_and_delivery.message)
        delivery_tag = message.delivery_tag
        self.qos.append(message_and_delivery.delivery, delivery_tag)
        if not no_ack:
            self.qos.ack(delivery_tag)
        return message

    def basic_ack(self, delivery_tag, multiple=False):
        """Acknowledge a message by delivery_tag.

        Acknowledges a message referenced by delivery_tag. Messages can
        only be ACKed using :meth:`basic_ack` if they were acquired using
        :meth:`basic_consume`. This is the ACKing portion of the
        asynchronous read behavior.

        Internally, this method uses the :class:`QoS` object, which stores
        messages and is responsible for the ACKing.

        :param delivery_tag: The delivery tag associated with the message
            to be acknowledged.
        :type delivery_tag: uuid.UUID
        :param multiple: not implemented. If set to True an AssertionError
            is raised.
        :type multiple: bool

        """
        assert multiple is False
        self.qos.ack(delivery_tag)

    def basic_reject(self, delivery_tag, requeue=False):
        """Reject a message by delivery_tag.

        Rejects a message that has been received by the Channel, but not
        yet acknowledged. Messages are referenced by their delivery_tag.

        If requeue is False, the rejected message will be dropped by the
        broker and not delivered to any other consumers. If requeue is
        True, then the rejected message will be requeued for delivery to
        another consumer, potentially to the same consumer who rejected the
        message previously.

        :param delivery_tag: The delivery tag associated with the message
            to be rejected.
        :type delivery_tag: uuid.UUID
        :keyword requeue: If False, the rejected message will be dropped by
            the broker and not delivered to any other consumers. If True,
            then the rejected message will be requeued for delivery to
            another consumer, potentially to the same consumer who rejected
            the message previously.
        :type requeue: bool

        """
        self.qos.reject(delivery_tag, requeue=requeue)

    def basic_consume(self, queue, no_ack, callback, consumer_tag, **kwargs):
        """Start an asynchronous consumer that reads from a queue.

        The consumer started reads messages from a queue until stopped by a
        call to :meth:`basic_cancel`. The queue name to read from is specified
        by 'queue'.

        The callback will be called with the each received message as an
        argument. Callback must accept a single positional argument.

        Each consumer is referenced by a consumer_tag, which is provided by
        the caller of this method.

        All messages are added to the QoS object when received. If no_ack is
        True, the message is ACKed immediately.

        All messages that are un-ACKed  are can be ACKed later with a call
        to :meth:`basic_ack`.

        :param queue: The name of the queue to consume messages from
        :type queue: str

        :param no_ack: If True, then messages will be ACKed immediately
        :type no_ack: bool

        :param callback: a callable that will be called with each message
            that arrives from the queue. Callback should accept a single
            positional argument.
        :type callback: a callable object

        :param consumer_tag: a tag to reference the created consumer by.
            This consumer_tag is needed to cancel the consumer.
        :type consumer_tag: an immutable object
        """
        self._tag_to_queue[consumer_tag] = queue

        def _callback(received_message):
            msg = self._make_kombu_message_from_proton(
                received_message.message
            )
            delivery_tag = msg.delivery_tag
            self.qos.append(received_message.delivery, delivery_tag)
            if no_ack:
                # Celery will not ack this message later, so we should ack now
                self.basic_ack(delivery_tag)

            return callback(msg)

        self.connection._callbacks[queue] = _callback
        self.transport.main_thread_commands.put(StartConsumer(queue))

    def basic_cancel(self, consumer_tag):
        """Cancel consumer by consumer tag.

        Request the consumer stop reading messages from its queue.

        This method also cleans up all lingering references of the consumer.

        :param consumer_tag: The tag which refers to the consumer to be
            cancelled. Originally specified when the consumer was created
            as a parameter to :meth:`basic_consume`.
        :type consumer_tag: an immutable object

        """
        if consumer_tag in self._receivers:
            receiver = self._receivers.pop(consumer_tag)
            receiver.close()
            queue = self._tag_to_queue.pop(consumer_tag, None)
            self.connection._callbacks.pop(queue, None)

    def close(self):
        """Cancel all associated messages and close the Channel.

        This cancels all consumers by calling :meth:`basic_cancel` for each
        known consumer_tag. It also closes the self._broker sessions. Closing
        the sessions implicitly causes all outstanding, un-ACKed messages to
        be considered undelivered by the broker.

        """
        if not self.closed:
            self.closed = True
            for consumer_tag in list(self._receivers.keys()):
                self.basic_cancel(consumer_tag)
            if self.connection is not None:
                self.connection.close_channel(self)
            self._broker.close()

    def basic_qos(self, prefetch_count, *args):
        """Change :class:`QoS` settings for this Channel.

        Set the number of un-acknowledged messages this Channel can fetch and
        hold.

        Currently, this value is hard coded to 1.

        :param prefetch_count: Not used. This method is hard-coded to 1.
        :type prefetch_count: int

        """
        self.qos.prefetch_count = 1

    def prepare_message(self, body, priority=None, content_type=None,
                        content_encoding=None, headers=None, properties=None):
        """Prepare message data for sending.


        :param body: The body of the message
        :type body: str

        :keyword priority: A number between 0 and 9 that sets the priority of
            the message.
        :type priority: int

        :keyword content_type: The content_type the message body should be
            treated as. If unset, the content_type is autodetected from the
            body.
        :type content_type: str

        :keyword content_encoding: The content_encoding the message body is
            encoded as.
        :type content_encoding: str

        :keyword headers: Additional Message headers that should be set.
            Passed in as a key-value pair.
        :type headers: dict

        :keyword properties: Message properties to be set on the message.
        :type properties: dict

        :return: Returns a dict object that encapsulates message
            attributes. See parameters for more details on attributes that
            can be set.
        :rtype: dict

        """
        properties = properties or {}
        info = properties.setdefault('delivery_info', {})
        info['priority'] = priority or 0

        to_return = {'body': body, 'content_encoding': content_encoding,
                     'content_type': content_type,
                     'properties': properties or {}}
        to_return['properties']['headers'] = headers
        return to_return

    def basic_publish(self, message, exchange='', routing_key='', **kwargs):
        """Publish message onto an exchange using a routing key.

        Publish a message onto an exchange specified by 'exchange' using a
        routing key specified by 'routing_key'. The message is prepared in the
        following ways before sending:

        - encodes the body using :meth:`encode_body`
        - sets delivery_tag to a random uuid.UUID
        - sets the exchange and routing_key info as delivery_info

        :param message: A dict containing key value pairs with the message
            data. A valid message dict can be generated using the
            :meth:`prepare_message` method.
        :type message: dict

        :param exchange: The name of the exchange to submit this message
            onto.
        :type exchange: str

        :param routing_key: The routing key to be used as the message is
            submitted onto the exchange.
        :type routing_key: str

        """
        message['body'], body_encoding = self.encode_body(
            message['body'], self.body_encoding,
        )
        delivery_tag = uuid.uuid4()
        props = message['properties']
        props.update(
            body_encoding=body_encoding,
            delivery_tag=delivery_tag,
        )
        props['delivery_info'].update(
            exchange=exchange,
            routing_key=routing_key,
        )

        proton_message = proton.Message(subject=routing_key, **message)
        send_complete = threading.Event()
        send_complete.sending_error = None
        cmd = SendMessage(message=proton_message, target=exchange,
                          send_complete=send_complete)
        self.transport.main_thread_commands.put(cmd)
        send_complete.wait()
        if send_complete.sending_error:
            exc = send_complete.sending_error
            send_complete.sending_error = None
            raise exc

    def encode_body(self, body, encoding=None):
        """Encode a body using an optionally specified encoding.

        The encoding can be specified by name and is looked up in
        self.codecs. self.codecs uses encoding names as strings for its keys.

        :param body: The body to be encoded.
        :type body: str

        :keyword encoding: The encoding type to be used. Must be a supported
            codec listed in self.codecs.
        :type encoding: str

        :return: If encoding is specified, return a tuple with the first
            position being the encoded body, and the second position the
            encoding used. If encoding is not specified, the tuple
            (body, None) is returned.
        :rtype: tuple

        """
        if encoding:
            return self.codecs.get(encoding).encode(body), encoding
        return body, encoding

    def decode_body(self, body, encoding=None):
        """Decode a body using an optionally specified encoding.

        The encoding can be specified by name and is looked up in
        self.codecs. self.codecs uses encoding names as strings for its keys.

        :param body: The body to be encoded.
        :type body: str

        :keyword encoding: The encoding type to be used. Must be a supported
            codec listed in self.codecs.
        :type encoding: str

        :return: If encoding is specified, the decoded body is returned.
            If encoding is not specified, the body is returned unchanged.
        :rtype: str

        """
        if encoding:
            return self.codecs.get(encoding).decode(body)
        return body

    def typeof(self, exchange, default='direct'):
        """Get the exchange type.

        Lookup and return the exchange type for an exchange specified by
        name. Exchange types are expected to be 'direct', 'topic',
        and 'fanout', which correspond with exchange functionality as
        specified in AMQP 0-10 and earlier. If the exchange cannot be
        found, the default exchange type is returned.

        :param exchange: The exchange to have its type lookup up.
        :type exchange: str
        :keyword default: The type of exchange to assume if the exchange does
            not exist.
        :type default: str

        :return: The exchange type either 'direct', 'topic', or 'fanout'.
        :rtype: str

        """
        qpid_exchange = self._broker.getExchange(exchange)
        if qpid_exchange:
            qpid_exchange_attributes = qpid_exchange.getAttributes()
            return qpid_exchange_attributes['type']
        else:
            return default


class Connection(object):
    """Qpid Connection.

    Encapsulate a connection object for the
    :class:`~kombu.transport.qpid.Transport`.

    :ivar host: The host that connections should connect to.
    :ivar port: The port that connection should connect to.
    :ivar username: The username that connections should connect with.
        Optional.
    :ivar password: The password that connections should connect with.
        Optional but requires a username.
    :ivar transport: The transport type that connections should use.
        Either 'tcp', or 'ssl' are expected as values.
    :ivar timeout: the timeout used when a Connection connects
        to the broker.
    :ivar sasl_mechanisms: The sasl authentication mechanism type to use.
        refer to SASL documentation for an explanation of valid
        values.

    A connection object is created by a
    :class:`~kombu.transport.qpid.Transport` during a call to
    :meth:`~kombu.transport.qpid.Transport.establish_connection`.  The
    :class:`~kombu.transport.qpid.Transport` passes in
    connection options as keywords that should be used for any connections
    created. Each :class:`~kombu.transport.qpid.Transport` creates exactly
    one Connection.

    A Connection object maintains a reference to a
    :class:`~proton.utils.BlockingConnection` which can be accessed through a
    bound getter method named :meth:`get_broker_agent` method. Each Channel
    uses the Connection for each :class:`qmf.client.BrokerAgent`.

    The Connection object is also responsible for maintaining the
    dictionary of references to callbacks that should be called when
    messages are received. These callbacks are saved in _callbacks,
    and keyed on the queue name associated with the received message. The
    _callbacks are setup in :meth:`Channel.basic_consume`, removed in
    :meth:`Channel.basic_cancel`, and called in
    :meth:`Transport.drain_events`.
    """

    # A class reference to the :class:`Channel` object
    Channel = Channel

    def __init__(self, conn_opts):
        self.channels = []
        self._callbacks = {}
        self.conn_opts = conn_opts
        self._broker_agent = BrokerAgent.connect(**conn_opts)

    def get_broker_agent(self):
        return self._broker_agent

    def close_channel(self, channel):
        """Close a Channel.

        Close a channel specified by a reference to the
        :class:`~kombu.transport.qpid.Channel` object.

        :param channel: Channel that should be closed.
        :type channel: :class:`~kombu.transport.qpid.Channel`.

        """
        try:
            self.channels.remove(channel)
        except ValueError:
            pass
        finally:
            channel.connection = None


class ProtonMessaging(MessagingHandler):

    def __init__(self, conn_opts, w, main_thread_commands, recv_messages,
                 transport):
        super(ProtonMessaging, self).__init__(auto_accept=False)
        self.conn_opts = conn_opts  # store connection options
        self.w = w
        self.main_thread_commands = main_thread_commands
        self.recv_messages = recv_messages  # async send messages to MainThread
        self.transport = transport
        self.senders = {}  # senders indexes by address
        self.send_complete_events = {}  # event send complete events indexed by sender address
        self.get_one_queues = {}  # get_one send messages to MainThread

    def on_connection_error(self, event):
        logger.warning('Proton raised %s' % event.type)

    def on_transport_error(self, event):
        logger.warning('Proton raised %s' % event.type)

    def on_disconnected(self, event):
        self.transport.proton_error.append(ConnectionClosed(self.conn))
        os.write(self.w, b'e')
        logger.warning('Proton raised %s' % event.type)

    def on_link_error(self, event):
        logger.warning('Proton raised %s' % event.type)

    def on_start(self, event):
        self.conn = event.container.connect(reconnect=False, **self.conn_opts)
        event.container.schedule(PROTON_PERIOD, self)

    def on_timer_task(self, event):
        start_time = time.time()
        elapsed_time = 0

        # Handle any un-acked basic_get requests
        for queue_name in list(self.get_one_queues.keys()):
            return_queue = self.get_one_queues.pop(queue_name)
            return_queue.put(None)

        while elapsed_time < 1.0:
            try:
                command = self.main_thread_commands.get(False)
            except queue.Empty:
                break
            else:
                if isinstance(command, SendMessage):
                    try:
                        sender = self.senders[command.target]
                    except KeyError:
                        sender = event.container.create_sender(self.conn,
                                                               command.target)
                        self.senders[command.target] = sender
                    if sender.transport is None:
                        command.send_complete.sending_error = ConnectionClosed(self.conn)
                        command.send_complete.set()
                    self.send_complete_events[sender.name] = command.send_complete
                    sender.send(command.message)
                elif isinstance(command, StartConsumer):
                    event.container.create_receiver(self.conn, command.queue)
                elif isinstance(command, AckMessage):
                    self.accept(command.delivery)
                    command.ack_complete.set()
                elif isinstance(command, ReleaseMessage):
                    self.release(command.delivery)
                    command.release_complete.set()
                elif isinstance(command, RejectMessage):
                    self.reject(command.delivery)
                    command.reject_complete.set()
                elif isinstance(command, GetOneMessage):
                    receiver = event.container.create_receiver(self.conn, command.queue)
                    self.get_one_queues[receiver.name] = command.return_queue
                else:
                    msg = _('Unrecognized command: {command}')
                    logger.error(msg.format(command=command))
            elapsed_time = time.time() - start_time
        event.container.schedule(PROTON_PERIOD, self)

    def on_unhandled(self, call_name, *args, **kwargs):
        pass

    def on_accepted(self, event):
        send_complete = self.send_complete_events.pop(event.sender.name)
        send_complete.set()

    def on_message(self, event):
        received_message = ReceivedMessage(message=event.message,
                                           delivery=event.delivery,
                                           queue=event.link.source.address)
        return_queue = self.get_one_queues.pop(event.receiver.name,
                                               self.recv_messages)
        return_queue.put(received_message)
        if return_queue is self.recv_messages:
            # We need to trigger this to cause Celery to call into
            # :meth:`drain_events`
            os.write(self.w, b'0')


class ProtonThread(threading.Thread):

    def __init__(self, w, main_thread_commands, recv_messages, conn_opts, transport):
        super(ProtonThread, self).__init__()
        self.main_thread_commands = main_thread_commands
        self.recv_messages = recv_messages
        self.conn_opts = conn_opts
        self.w = w
        self.transport = transport

    def run(self):
        while True:
            try:
                messaging_handler = ProtonMessaging(self.conn_opts, self.w,
                                                    self.main_thread_commands,
                                                    self.recv_messages,
                                                    self.transport)
                Container(messaging_handler).run()
            except Exception:
                msg =_('ProtonThread encountered an exception.')
                logger.exception(msg)
            time.sleep(10)



class Transport(base.Transport):
    """Kombu native transport for a Qpid broker.

    Provide a native transport for Kombu that allows consumers and
    producers to read and write messages to/from a broker. This Transport
    is capable of supporting both synchronous and asynchronous reading.
    All writes are synchronous through the :class:`Channel` objects that
    support this Transport.

    Asynchronous reads are done using a call to :meth:`drain_events`,
    which synchronously reads messages that were fetched asynchronously, and
    then handles them through calls to the callback handlers maintained on
    the :class:`Connection` object.

    The Transport also provides methods to establish and close a connection
    to the broker. This Transport establishes a factory-like pattern that
    allows for singleton pattern to consolidate all Connections into a single
    one.

    The Transport can create :class:`Channel` objects to communicate with the
    broker with using the :meth:`create_channel` method.

    The Transport identifies recoverable connection errors and recoverable
    channel errors according to the Kombu 3.0 interface. These exception are
    listed as tuples and store in the Transport class attribute
    `recoverable_connection_errors` and `recoverable_channel_errors`
    respectively. Any exception raised that is not a member of one of these
    tuples is considered non-recoverable. This allows Kombu support for
    automatic retry of certain operations to function correctly.

    For backwards compatibility to the pre Kombu 3.0 exception interface, the
    recoverable errors are also listed as `connection_errors` and
    `channel_errors`.

    """

    # Reference to the class that should be used as the Connection object
    Connection = Connection

    # This Transport does not specify a polling interval.
    polling_interval = None

    # This Transport does support the Celery asynchronous event model.
    supports_ev = True

    # The driver type and name for identification purposes.
    driver_type = 'qpid'
    driver_name = 'qpid'

    # Exceptions that can be recovered from, but where the connection must be
    # closed and re-established first.
    recoverable_connection_errors = (
        ConnectionException,
        select.error,
    )

    # Exceptions that can be automatically recovered from without
    # re-establishing the connection.
    recoverable_channel_errors = (
    )

    # Support the pre 3.0 Kombu exception labeling interface which treats
    # connection_errors and channel_errors both as recoverable via a
    # reconnect.
    connection_errors = recoverable_connection_errors
    channel_errors = recoverable_channel_errors

    def __init__(self, *args, **kwargs):
        self.verify_runtime_environment()
        self.main_thread_commands = queue.Queue()
        self.recv_messages = queue.Queue()
        self.r, self._w = os.pipe()
        if fcntl is not None:
            fcntl.fcntl(self.r, fcntl.F_SETFL, os.O_NONBLOCK)
        super(Transport, self).__init__(*args, **kwargs)
        self.proton_error = []

    def verify_runtime_environment(self):
        """Verify that the runtime environment is acceptable.

        This method is called as part of __init__ and raises a RuntimeError
        in Python3 or PyPi environments. This module is not compatible with
        Python3 or PyPi. The RuntimeError identifies this to the user up
        front along with suggesting Python 2.6+ be used instead.

        This method also checks that the dependencies 'qmf.client.BrokerAgent'
        and 'proton' are installed. If either one is not installed a
        RuntimeError is raised.

        :raises: RuntimeError if the runtime environment is not acceptable.

        """
        if getattr(sys, 'pypy_version_info', None):
            raise RuntimeError(
                'The Qpid transport for Kombu does not '
                'support PyPy. Try using Python 2.6+',
            )

        if dependency_is_none(proton):
            raise RuntimeError(
                'The Python package "proton" is missing. Install it '
                'with your package manager. You can also try `pip install '
                'python-qpid-proton`.')

    def on_readable(self, connection, loop):
        """Handle any messages associated with this Transport.

        This method clears a single message from the externally monitored
        file descriptor by issuing a read call to the self.r file descriptor
        which removes a single b'0' character that was placed into the pipe
        by the Qpid session message callback handler. Once a b'0' is read,
        all available events are drained through a call to
        :meth:`drain_events`.

        The file descriptor self.r is modified to be non-blocking, ensuring
        that an accidental call to this method when no more messages will
        not cause indefinite blocking.

        Nothing is expected to be returned from :meth:`drain_events` because
        :meth:`drain_events` handles messages by calling callbacks that are
        maintained on the :class:`~kombu.transport.qpid.Connection` object.
        When :meth:`drain_events` returns, all associated messages have been
        handled.

        This method calls drain_events() which reads as many messages as are
        available for this Transport, and then returns. It blocks in the
        sense that reading and handling a large number of messages may take
        time, but it does not block waiting for a new message to arrive. When
        :meth:`drain_events` is called a timeout is not specified, which
        causes this behavior.

        One interesting behavior of note is where multiple messages are
        ready, and this method removes a single b'0' character from
        self.r, but :meth:`drain_events` may handle an arbitrary amount of
        messages. In that case, extra b'0' characters may be left on self.r
        to be read, where messages corresponding with those b'0' characters
        have already been handled. The external epoll loop will incorrectly
        think additional data is ready for reading, and will call
        on_readable unnecessarily, once for each b'0' to be read. Additional
        calls to :meth:`on_readable` produce no negative side effects,
        and will eventually clear out the symbols from the self.r file
        descriptor. If new messages show up during this draining period,
        they will also be properly handled.

        :param connection: The connection associated with the readable
            events, which contains the callbacks that need to be called for
            the readable objects.
        :type connection: kombu.transport.qpid.Connection
        :param loop: The asynchronous loop object that contains epoll like
            functionality.
        :type loop: :class:`kombu.async.Hub`
        """
        os.read(self.r, 1)
        if len(self.proton_error) > 1:
            exc = self.proton_error.pop()
            self.proton_error = []  # Reset the errors before raising one of them
            raise exc
        try:
            self.drain_events(connection)
        except socket.timeout:
            pass

    def register_with_event_loop(self, connection, loop):
        """Register a file descriptor and callback with the loop.

        Register the callback self.on_readable to be called when an
        external epoll loop sees that the file descriptor registered is
        ready for reading. The file descriptor is created by this Transport,
        and is written to when a message is available.

        Because supports_ev == True, Celery expects to call this method to
        give the Transport an opportunity to register a read file descriptor
        for external monitoring by celery using an Event I/O notification
        mechanism such as epoll. A callback is also registered that is to
        be called once the external epoll loop is ready to handle the epoll
        event associated with messages that are ready to be handled for
        this Transport.

        The registration call is made exactly once per Transport after the
        Transport is instantiated.

        :param connection: A reference to the connection associated with
            this Transport.
        :type connection: kombu.transport.qpid.Connection
        :param loop: A reference to the external loop.
        :type loop: kombu.async.hub.Hub

        """
        loop.add_reader(self.r, self.on_readable, connection, loop)

    def establish_connection(self):
        """Establish a Connection object.

        Determines the correct options to use when creating any
        connections needed by this Transport, and create a
        :class:`Connection` object which saves those values for
        connections generated as they are needed. The options are a
        mixture of what is passed in through the creator of the
        Transport, and the defaults provided by
        :meth:`default_connection_params`. Options cover broker network
        settings, timeout behaviors, authentication, and identity
        verification settings.

        :return: The created :class:`Connection` object is returned.
        :rtype: :class:`Connection`

        """
        conninfo = self.client
        for name, default_value in items(self.default_connection_params):
            if not getattr(conninfo, name, None):
                setattr(conninfo, name, default_value)

        hostname = conninfo.hostname
        port = conninfo.port

        protocol_handler = 'amqp://'

        conn_opts = {}

        if conninfo.ssl:
            ssl_domain = proton.SSLDomain(proton.SSLDomain.MODE_CLIENT)

            protocol_handler = 'amqps://'

            key_file = conninfo.ssl['keyfile']
            cert_file = conninfo.ssl['certfile']
            password = None

            ssl_domain.set_credentials(cert_file, key_file, password)

            ssl_domain.set_trusted_ca_db(conninfo.ssl['ca_certs'])

            ssl_domain.set_peer_authentication(
                proton.SSLDomain.VERIFY_PEER_NAME
            )

            conn_opts['ssl_domain'] = ssl_domain

        url = '%s%s:%s' % (protocol_handler, hostname, port)
        conn_opts['url'] = url

        qmf_conn_options = copy.copy(conn_opts)

        if conninfo.login_method is not None or conninfo.userid is not None or conninfo.password is not None:
            if conninfo.login_method is None and (conninfo.userid is not None or conninfo.password is not None):
                conninfo.login_method = 'PLAIN'

            if conninfo.login_method == 'PLAIN':
                if conninfo.userid is None and conninfo.password is not None:
                    raise Exception(
                        'Password configured but no username. SASL PLAIN '
                        'requires a username when using a password.')
                elif conninfo.userid is not None and conninfo.password is None:
                    raise Exception(
                        'Username configured but no password. SASL PLAIN '
                        'requires a password when using a username.')

            sasl = SASL_obj()
            sasl.mechs = conninfo.login_method
            conn_opts['allowed_mechs'] = conninfo.login_method

            if conninfo.userid is not None:
                sasl.user = conninfo.userid
                conn_opts['user'] = conninfo.userid
            if conninfo.password is not None:
                sasl.password = conninfo.password
                conn_opts['password'] = conninfo.password

            qmf_conn_options['sasl'] = sasl
            conn_opts['sasl_enabled'] = True

        qmf_conn_options['reconnect_delays'] = ReconnectDelays(1, 10, True)
        conn =  self.Connection(qmf_conn_options)
        conn.client = self.client

        proton_thread = ProtonThread(
            self._w, self.main_thread_commands, self.recv_messages,
            conn_opts, self)
        proton_thread.daemon = True
        proton_thread.start()

        return conn


    def close_connection(self, connection):
        """Close the :class:`Connection` object.

        :param connection: The Connection that should be closed.
        :type connection: :class:`kombu.transport.qpid.Connection`

        """
        connection.close()

    def drain_events(self, connection, timeout=0, **kwargs):
        """Handle and call callbacks for all ready Transport messages.

        Drains all events that are ready from all receivers that are
        asynchronously fetching messages.

        For each drained message, the message is called to the appropriate
        callback. Callbacks are organized by queue name.

        :param connection: The :class:`~kombu.transport.qpid.Connection` that
            contains the callbacks, indexed by queue name, which will be called
            by this method.
        :type connection: kombu.transport.qpid.Connection
        :keyword timeout: The timeout that limits how long this method will
            run for. The timeout could interrupt a blocking read that is
            waiting for a new message, or cause this method to return before
            all messages are drained. Defaults to 0.
        :type timeout: int

        """
        start_time = monotonic()
        elapsed_time = -1
        while elapsed_time < timeout:
            try:
                received_message = self.recv_messages.get(block=True,
                                                          timeout=timeout)
            except queue.Empty:
                raise socket.timeout()
            else:
                try:
                    callback = connection._callbacks[received_message.queue]
                except KeyError:
                    logger.error('Cannot determine which queue a message was received on. Something is wrong.')
                    raise
                else:
                    callback(received_message)
            elapsed_time = time.time() - start_time
        raise socket.timeout()

    def create_channel(self, connection):
        """Create and return a :class:`~kombu.transport.qpid.Channel`.

        Creates a new channel, and appends the channel to the
        list of channels known by the Connection.  Once the new
        channel is created, it is returned.

        :param connection: The connection that should support the new
            :class:`~kombu.transport.qpid.Channel`.
        :type connection: kombu.transport.qpid.Connection

        :return: The new Channel that is made.
        :rtype: :class:`kombu.transport.qpid.Channel`.

        """
        channel = Channel(connection, self)
        connection.channels.append(channel)
        return channel

    @property
    def default_connection_params(self):
        """Return a dict with default connection parameters.

        These connection parameters will be used whenever the creator of
        Transport does not specify a required parameter.

        :return: A dict containing the default parameters.
        :rtype: dict

        """
        return {
            'hostname': 'localhost',
            'port': 5672,
        }

    def __del__(self):
        """Ensure file descriptors opened in __init__() are closed."""
        for fd in (self.r, self._w):
            try:
                os.close(fd)
            except OSError:
                # ignored
                pass
