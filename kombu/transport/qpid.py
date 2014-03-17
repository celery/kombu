from __future__ import absolute_import

"""
kombu.transport.qpid
=======================

qpid transport.

"""

"""Kombu transport using the Django database as a message store."""

import os
import base64
import threading
import Queue
import socket
import ssl
from time import clock
from itertools import count

from kombu.five import Empty, items
from kombu.utils import kwdict
from kombu.utils.compat import OrderedDict
from kombu.utils.encoding import str_to_bytes, bytes_to_str

from amqp.protocol import queue_declare_ok_t

from qpid.messaging import Connection as QpidConnection
from qpid.messaging import Message as QpidMessage
from qpid.messaging import Disposition as QpidDisposition
from qpid.messaging import REJECTED, RELEASED
from qpid.messaging.exceptions import Empty as QpidEmpty
from qpidtoollibs import BrokerAgent

from . import base

##### Start Monkey Patching #####

from qpid.selector import Selector
import atexit


def default_monkey():
    Selector.lock.acquire()
    try:
        if Selector.DEFAULT is None:
            sel = Selector()
            atexit.register(sel.stop)
            sel.start()
            Selector.DEFAULT = sel
            Selector._current_pid = os.getpid()
        elif Selector._current_pid != os.getpid():
            sel = Selector()
            atexit.register(sel.stop)
            sel.start()
            Selector.DEFAULT = sel
            Selector._current_pid = os.getpid()
        return Selector.DEFAULT
    finally:
        Selector.lock.release()

import qpid.selector
qpid.selector.Selector.default = staticmethod(default_monkey)

from qpid.ops import ExchangeQuery, QueueQuery


def resolve_declare_monkey(self, sst, lnk, dir, action):
    declare = lnk.options.get("create") in ("always", dir)
    assrt = lnk.options.get("assert") in ("always", dir)
    requested_type = lnk.options.get("node", {}).get("type")

    def do_resolved(type, subtype):
        err = None
        if type is None:
            if declare:
                err = self.declare(sst, lnk, action)
            else:
                err = NotFound(text="no such queue: %s" % lnk.name)
        else:
            if assrt:
                expected = lnk.options.get("node", {}).get("type")
                if expected and type != expected:
                    err = AssertionFailed(
                        text="expected %s, got %s" % (expected, type))
            if err is None:
                action(type, subtype)
        if err:
            tgt = lnk.target
            tgt.error = err
            del self._attachments[tgt]
            tgt.closed = True
            return

    self.resolve(sst, lnk.name, do_resolved, node_type=requested_type,
                 force=declare)


def resolve_monkey(self, sst, name, action, force=False, node_type=None):
    if not force and not node_type:
        try:
            type, subtype = self.address_cache[name]
            action(type, subtype)
            return
        except KeyError:
            pass
    args = []

    def do_result(r):
        args.append(r)

    def do_action(r):
        do_result(r)
        er, qr = args
        if node_type == "topic" and not er.not_found:
            type, subtype = "topic", er.type
        elif node_type == "queue" and qr.queue:
            type, subtype = "queue", None
        elif er.not_found and not qr.queue:
            type, subtype = None, None
        elif qr.queue:
            type, subtype = "queue", None
        else:
            type, subtype = "topic", er.type
        if type is not None:
            self.address_cache[name] = (type, subtype)
        action(type, subtype)

    sst.write_query(ExchangeQuery(name), do_result)
    sst.write_query(QueueQuery(name), do_action)


import qpid.messaging.driver

qpid.messaging.driver.Engine.resolve_declare = resolve_declare_monkey
qpid.messaging.driver.Engine.resolve = resolve_monkey

##### End Monkey Patching #####


DEFAULT_PORT = 5672

VERSION = (1, 0, 0)
__version__ = '.'.join(map(str, VERSION))


class QpidMessagingExceptionHandler(object):
    """An exception handling decorator that silences some exceptions.

    An exception handling class designed to silence specific exceptions
    that qpid.messaging raises as part of normal operation. qpid.messaging
    exceptions require string parsing, and are not machine consumable,
    This is designed to be used as a decorator, and accepts a whitelist
    string as an argument.

    Usage:
    @QpidMessagingExceptionHandler('whitelist string goes here')

    :param allowed_exception_string: a string that, if present in the
    exception message, will be silenced.
    :type allowed_exception_string: str
    """

    def __init__(self, allowed_exception_string):
        self.allowed_exception_string = allowed_exception_string

    def __call__(self, original_func):
        """The decorator method.

        Method that wraps the actual function with exception silencing
        functionality. Any exception that contains the string
        self.allowed_exception_string in the message will be silenced.

        :param original_func: function that is automatically passed in
        when this object is used as a decorator.
        :type original_func: function
        """
        decorator_self = self

        def decorator(*args, **kwargs):
            """A runtime-built that will be returned which contains a
            reference to the original function, and wraps a call to it in
            a try/except block that can silence errors.
            """
            try:
                original_func(*args, **kwargs)
            except Exception as error:
                if decorator_self.allowed_exception_string not in error\
                        .message:
                    raise

        return decorator


class Base64(object):
    """A Base64 encoding and decoding helper object.

    Used by the Channel object below as a "supported codec".  Supports
    encoding and decoding of the message payload.
    """

    def encode(self, s):
        """
        Encode a string using Base64.

        :param s: The string to be encoded
        :type s: str
        """
        return bytes_to_str(base64.b64encode(str_to_bytes(s)))

    def decode(self, s):
        """Decode a string using Base64

        :param s: The string to be decoded
        :type s: str
        """
        return base64.b64decode(str_to_bytes(s))


class QoS(object):
    """A helper object for message prefetch and ACKing purposes.

    This object is instantiated 1-for-1 with a Channel. QoS allows
    prefetch_count to be set to the number of outstanding messages the
    corresponding Channel should be allowed to prefetch.  Messages are
    added using the append method, which are held until they are ACKed
    asynchronously through a call to ack().  Messages that are received,
    but not ACKed will not be delivered by the broker to another consumer
    until an ACK is received, or the session is closed. Messages are
    referred to using delivery_tag integers, which are unique per Channel.
    Delivery tags are managed outside of this object and are passed in with
    a message to append().  Un-ACKed messages can be looked up from QoS
    using get() and can be rejected and forgotten using reject().

    :param channel: Channel.
    :keyword prefetch_count: Initial prefetch count (defaults to 0).

    """

    #: current prefetch count value
    prefetch_count = 0

    #: :class:`~collections.OrderedDict` of active messages.
    #: *NOTE*: Can only be modified by the consuming thread.
    _not_yet_acked = None

    def __init__(self, channel, prefetch_count=0):
        self.channel = channel
        self.prefetch_count = prefetch_count or 0
        self._not_yet_acked = OrderedDict()

    def can_consume(self):
        """Return True if the Channel can consume more messages, else False.

        Used to ensure the client adheres to currently active prefetch
        limits.

        """
        pcount = self.prefetch_count
        return not pcount or len(self._not_yet_acked) < pcount

    def can_consume_max_estimate(self):
        """Return the remaining message capacity for the associated Channel.

        Returns an estimated number of outstanding messages that a Channel
        can accept without exceeding prefetch_count.

        returns:
            An integer >= 0
        """
        pcount = self.prefetch_count
        count = None
        if pcount:
            count = pcount - len(self._not_yet_acked)

        if count < 1:
            return 1

        return count

    def append(self, message, delivery_tag):
        """Append message to the list of unacked messages.

        Add a message, referenced by the integer delivery_tag, for ACKing,
        rejecting, or getting later. Messages are saved into an OrderedDict
        by delivery_tag.

        :param message: A received message that has not yet been acked
        :type message: qpid.messaging.Message
        :param delivery_tag: An integer number to refer to this message by
        upon receipt.
        :type delivery_tag: int
        """
        self._not_yet_acked[delivery_tag] = message

    def get(self, delivery_tag):
        """
        Get an un-ACKed message by delivery_tag.  If called with an invalid
        delivery_tag a KeyError is raised.

        :param delivery_tag: The delivery tag associated with the message
        to be returned.
        :type delivery_tag: int
        """
        return self._not_yet_acked[delivery_tag]

    def ack(self, delivery_tag):
        """Acknowledge a message by delivery_tag.

        Called asynchronously once the message has been handled and can be
        forgotten by the broker.

        :param delivery_tag: the delivery tag associated with the message
        to be acknowledged.
        :type delivery_tag: int
        """
        message = self._not_yet_acked.pop(delivery_tag)
        message._receiver.session.acknowledge(message=message)

    def reject(self, delivery_tag, requeue=False):
        """Reject a message by delivery_tag.

        Explicitly notify the broker that the Channel associated with this
        QoS object is rejecting the Message that was previously delivered.

        If requeue is False, then the message is not requeued for delivery
        to another consumer.  If requeue is True, then the message is
        requeued for delivery to another consumer.

        :param delivery_tag: The delivery tag associated with the message
        to be rejected.
        :type delivery_tag: int
        :param requeue: If True, the broker will be notified to requeue
        the message.  If False, the broker will be told to drop the message
        entirely.  In both cases, the message will be removed from this
        object.
        :type requeue: bool
        """
        message = self._not_yet_acked.pop(delivery_tag)
        if requeue:
            disposition = QpidDisposition(RELEASED)
        else:
            disposition = QpidDisposition(REJECTED)
        message._receiver.session.acknowledge(message=message,
                                              disposition=disposition)


class Message(base.Message):
    """Encodes message data in an organized way and serializes.

    Identical to the Message object used by Virtual Transports,
    and supports basic encoding/decoding and serialization.


    :param channel: The Channel associated with the message. A reference
    to Channel is needed to ensure serialization/encoding/decoding is
    supported by the Channel.
    :type channel: Channel
    :param payload: the payload of the message
    :type payload: dict
    """

    def __init__(self, channel, payload, **kwargs):
        self._raw = payload
        properties = payload['properties']
        body = payload.get('body')
        if body:
            body = channel.decode_body(body, properties.get('body_encoding'))
        kwargs.update({
            'body': body,
            'delivery_tag': properties['delivery_tag'],
            'content_type': payload.get('content-type'),
            'content_encoding': payload.get('content-encoding'),
            'headers': payload.get('headers'),
            'properties': properties,
            'delivery_info': properties.get('delivery_info'),
            'postencode': 'utf-8',
        })
        super(Message, self).__init__(channel, **kwdict(kwargs))

    def serializable(self):
        """Serialize the message.

        Serialize the message using encodings supported by the Channel that
        will send the message.
        """
        props = self.properties
        body, _ = self.channel.encode_body(self.body,
                                           props.get('body_encoding'))
        headers = dict(self.headers)
        # remove compression header
        headers.pop('compression', None)
        return {
            'body': body,
            'properties': props,
            'content-type': self.content_type,
            'content-encoding': self.content_encoding,
            'headers': headers,
        }


class Channel(base.StdChannel):
    """Supports broker configuration and messaging send and receive.

    A Channel object is designed to have method-parity with a Channel as
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
    broker, but provide a level of isolated communication with the broker
    while benefiting from a shared TCP connection.  The Channel is given
    its Connection object by the Transport that instantiates the Channel.

    This Channel inherits directly from base.StdChannel, which makes this a
    'native' Channel versus a 'virtual' Channel which would inherit from
    kombu.transports.virtual.

    Messages sent using this Channel are assigned a delivery_tag. The
    delivery_tag is generated for a message as they are prepared for sending
    by basic_publish().  The delivery_tag is unique per Channel instance
    using itertools.count.  The delivery_tag has no meaningful context in
    other objects, and is only maintained in the memory of this object,
    and the underlying objects that provide support (ie: QoS).

    Each Channel object instantiates exactly one QoS object for prefetch
    limiting, and asynchronous acking. The QoS object is lazily instantiated
    through a @property method qos.  The QoS object is a supporting object
    that should not be accessed directly except by the Channel itself.

    Synchronous reads on a queue are done using a call to basic_get() which
    uses _get() to perform the reading. These methods read immediately and
    do not accept some form of timeout. basic_get() reads synchronously and
    ACKs messages before returning them, or acking can be disable by the
    no_ack argument to basic_get().

    Asynchronous reads on a queue are done by starting a consumer using
    basic_consume().  Each call to basic_consume() will cause a thread to
    be started where a qpid.messaging.receiver will perform a blocking read
    on the requested queue. Typically a more efficient external I/O event
    notification system such as epoll or kqueue would allow the kernel to
    monitor many file descriptors for inbound data, but the qpid.messaging
    library does not allow an external epoll or kqueue loop to be used.
    Consumers are given a consumer tag by the caller of consumer_tag.
    Already started consumers can be cancelled using by their consumer_tag
    using basic_cancel().

    The Channel object handles thread creation of FDShimThread objects
    which provide asynchronous blocking reads.  FDShimThreads are given a
    Queue.Queue object to put messages into called delivery_queue.
    delivery_queue is provided by the creator of the Channel (typically a
    Transport object).  Cancellation of a consumer causes the consuming
    FDShimThread to be notified it is no longer needed.

    Asynchronous message acking is supported through the basic_ack
    function, and is referenced by delivery_tag. The Channel object uses
    its QoS object to perform the message acking.

    :param connection: A Connection object that this Channel can reference.
    Currently only used to access callbacks.
    :type connection: Connection
    :param transport: The Transport this Channel is associated with.
    :type transport: Transport
    :param delivery_queue: A threadsafe queue that asynchronous
    FDShimThread consumers should put arriving messages into.
    :type delivery_queue: Queue.Queue
    """

    #: A class reference that will be instantiated using the qos property.
    QoS = QoS

    #: A class reference that identifies the usage of Message as the message
    # type for this Channel
    Message = Message

    #: Default body encoding.
    #: NOTE: ``transport_options['body_encoding']`` will override this value.
    body_encoding = 'base64'

    #: Binary <-> ASCII codecs.
    codecs = {'base64': Base64()}

    #: counter used to generate delivery tags for this channel.
    _delivery_tags = count(1)

    def __init__(self, connection, transport, delivery_queue):
        self.connection = connection
        self.transport = transport
        self.delivery_queue = delivery_queue
        self._tag_to_queue = {}
        self._consumer_threads = {}
        qpid_connection = connection.create_qpid_connection()
        self._qpid_session = qpid_connection.session()
        self._broker = BrokerAgent(qpid_connection)
        self._qos = None
        self._consumers = set()
        self.closed = False

    def _get(self, queue):
        """Non-blocking, single-message read from a queue.

        An internal method to perform a non-blocking, single-message
        read from a queue by name. This method creates a receiver to read
        from the queue using the session referenced by _qpid_session.  The
        receiver is closed before the method exits. If a message is
        available, a qpid.messaging.Message object is returned.  If no
        message is available, a qpid.messaging.exception:Empty exception is
        raised.

        This is an internal method.  External calls for get functionality
        should be done using basic_get().

        :param queue: The queue name to get the message from
        :type queue: str
        """
        rx = self._qpid_session.receiver(queue)
        try:
            message = rx.fetch(timeout=0)
        finally:
            rx.close()
        return message

    def _put(self, routing_key, message, exchange=None, **kwargs):
        """Synchronous send of a single message onto a queue or exchange.

        An internal method which synchronously sends a single message onto
        a given queue or exchange.  If exchange is not specified,
        the message is sent directly to a queue specified by routing_key.
        If no queue is found by the name of routing_key while exchange is
        not specified an exception is raised.  If an exchange is specified,
        then the message is delivered onto the requested
        exchange using routing_key. Message sending is synchronous using
        sync=True because large messages in kombu funtests were not being
        fully sent before the receiver closed.

        This method creates a sender to send the message to the queue using
        the session referenced by _qpid_session.  The sender is closed
        before the method exits.

        This is an internal method. External calls for put functionality
        should be done using basic_publish().

        :param routing_key: If exchange is None, treated as the queue
        name to send the message to. If exchange is not None, treated as
        the routing_key to use as the message is submitted onto the exchange.
        :type routing_key: str
        :param message: The message to be sent as prepared by
        basic_publish().
        :type message: dict
        :param exchange: keyword parameter of the exchange this message
        should be sent on. If no exchange is specified, the message is sent
        directly to a queue specified by routing_key.
        :type exchange: str
        """
        if not exchange:
            address = '%s; {assert: always, node: {type: queue}}' % \
                      routing_key
            msg_subject = None
        else:
            address = '%s/%s; {assert: always, node: {type: topic}}' % (
                exchange, routing_key)
            msg_subject = str(routing_key)
        sender = self._qpid_session.sender(address)
        qpid_message = QpidMessage(content=message, subject=msg_subject)
        sender.send(qpid_message, sync=True)
        sender.close()

    def _purge(self, queue):
        """Purge all undelivered messages from a queue specified by name.

        An internal method to purge all undelivered messages from a queue
        specified by name.  The queue message depth is first checked,
        and then the broker is asked to purge that number of messages.  The
        integer number of messages requested to be purged is returned. The
        actual number of messages purged may be different than the
        requested number of messages to purge (see below).

        Delivered messages are asked to be purged, but are not.  This case
        fails silently, which is the correct behavior when a message that
        has been delivered to a different consumer, who has not acked the
        message, and still has an active session with the broker. Messages
        in that case are not safe for purging and will be retained by the
        broker.  The client is unable to change this delivery behavior.

        This is an internal method.  External calls for purge functionality
        should be done using queue_purge().

        :param queue: the name of the queue to be purged
        :type queue: str
        """
        queue_to_purge = self._broker.getQueue(queue)
        message_count = queue_to_purge.values['msgDepth']
        if message_count > 0:
            queue_to_purge.purge(message_count)
        return message_count

    def _size(self, queue):
        """Get the number of messages in a queue specified by name.

        An internal method to return the number of messages in a queue
        specified by name.  It returns an integer could of the number
        of messages currently in the queue.

        :param queue: The name of the queue to be inspected for the number
        of messages
        :type queue: str
        """
        queue_to_check = self._broker.getQueue(queue)
        message_depth = queue_to_check.values['msgDepth']
        return message_depth

    def _delete(self, queue, *args, **kwargs):
        """Delete a queue and all messages on that queue.

        An internal method to delete a queue specified by name and all the
        messages on it. First, all messages are purged from a queue using a
        call to _purge().  Second, the broker is asked to delete the queue.

        This is an internal method.  External calls for queue delete
        functionality should be done using queue_delete().

        :param queue: The name of the queue to be deleted.
        :type queue: str
        """
        self._purge(queue)
        self._broker.delQueue(queue)

    @QpidMessagingExceptionHandler('object already exists')
    def _new_queue(self, queue, **kwargs):
        """Create a new queue specified by name.

        An internal method to create a new queue specified by name. If the
        queue already exists, an exception is raise, which is caught and
        silenced by the @QpidMessagingExceptionHandler decorator.

        This is an internal method.  External calls for queue creation
        functionality should be done using queue_declare().

        This is an internal method,
        :param queue: the name of the queue to be created.
        :type queue: str
        """
        self._broker.addQueue(queue)

    def _has_queue(self, queue, **kwargs):
        """Determine if the broker has a queue specified by name.

        Returns True if the broker has a queue specified by name.  Returns
        False if the broker does not have a queue specified by name.

        :param queue: The queue name to check if the queue exists.
        :type queue: str
        """
        if self._broker.getQueue(queue):
            return True
        else:
            return False

    def queue_declare(self, queue, **kwargs):
        """Create a new queue specified by name.

        If a queue already exists, no action is taken and no exceptions are
        raised.  If the queue name is not specified, a queue name is
        generated using uuid(). This method uses _new_queue() internally.

        This method returns a named tuple with the name
        'queue_declare_ok_t' and the queue name as 'queue', message count
        on the queue as 'message_count', and the number of active consumers
        as 'consumer_count'.  The consumer count is assumed to be 0 since
        the queue was just created.  The named tuple values are ordered as
        queue, message_count, and consumer_count respectively.

        :param queue: the name of the queue to be created.
        :type queue: str
        """
        self._new_queue(queue, **kwargs)
        return queue_declare_ok_t(queue, self._size(queue), 0)

    def queue_delete(self, queue, if_unused=False, if_empty=False, **kwargs):
        """Delete a queue by name.

        Delete a queue specified by name.  Using the if_unused keyword
        argument, the delete can only occur if there are 0 consumers bound
        to it.  Using the if_empty keyword argument, the delete can only
        occur if there are 0 messages in the queue.

        This method returns None in all cases.

        :param queue: The name of the queue to be deleted.
        :type queue: str
        :param if_unused: If True, delete only if the queue has 0
        consumers.  If False, delete a queue even with consumers bound to it.
        :type if_unused: bool
        :param if_empty: If True, only delete the queue if it is empty.  If
        False, delete the queue if it is empty or not.
        :type if_empty: bool
        """
        if self._has_queue(queue):
            if if_empty and self._size(queue):
                return
            queue_obj = self._broker.getQueue(queue)
            consumer_count = queue_obj.getAttributes()['consumerCount']
            if if_unused and consumer_count > 0:
                return
            self._delete(queue)

    @QpidMessagingExceptionHandler('object already exists')
    def exchange_declare(self, exchange='', type='direct', durable=False,
                         **kwargs):
        """Create a new exchange.

        Create an exchange of a specific type, and optionally have the
        exchange be durable.  If an exchange of the requested name already
        exists, no action is taken and no exceptions are raised.  Durable
        exchanges will survive a broker restart, non-durable exchanges will
        not.

        Exchanges provide behaviors based on their type.  The expected
        behaviors are those defined in the AMQP 0-10 and prior
        specifications including 'direct', 'topic', and 'fanout'
        functionality.

        :param type: The exchange type. Valid values include 'direct',
        'topic', and 'fanout'.
        :type type: str
        :param exchange: The name of the exchange to be created.  If no
        exchange is specified, then a blank string will be used as the name.
        :type exchange: str
        :param durable: True if the exchange should be durable, or False
        otherwise.
        :type durable: bool
        """
        options = {'durable': durable}
        self._broker.addExchange(type, exchange, options)

    def exchange_delete(self, exchange_name, **kwargs):
        """Delete an exchange specified by name

        :param exchange_name: The name of the exchange to be deleted.
        :type exchange_name: str
        """
        self._broker.delExchange(exchange_name)

    @QpidMessagingExceptionHandler('queue in use')
    def after_reply_message_received(self, queue):
        self._delete(queue)

    def queue_bind(self, queue, exchange, routing_key, **kwargs):
        """Bind a queue to an exchange with a bind key.

        Bind a queue specified by name, to an exchange specified by name,
        with a specific bind key.  The queue and exchange must already
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
        name, that is already bound with a bind key.  The queue and
        exchange must already exist on the broker, and bound with the bind
        key for the operation to complete successfully.  Queues may be
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

        Purge all undelivered messages from a queue specified by name.  The
        queue message depth is first checked, and then the broker is asked
        to purge that number of messages.  The integer number of messages
        requested to be purged is returned. The actual number of messages
        purged may be different than the requested number of messages to
        purge.

        Delivered messages are asked to be purged, but are not.  This case
        fails silently, which is the correct behavior when a message that
        has been delivered to a different consumer, who has not acked the
        message, and still has an active session with the broker. Messages
        in that case are not safe for purging and will be retained by the
        broker.  The client is unable to change this delivery behavior.

        Internally, this method relies on _purge().

        :param queue: The name of the queue which should have all messages
        removed.
        :type queue: str
        """
        return self._purge(queue)

    def basic_get(self, queue, no_ack=False, **kwargs):
        """Non-blocking single message get and ack from a queue by name.

        Internally this method uses _get() to fetch the message.  If and
        Empty exception is raised by _get(), this method silences it and
        returns None.  If _get() does return a message, that message is
        acked according to the value of no_ack and returned.  If no_ack is
        True, the message is not acked, and if no_ack is False, By default,
        the message is acked.  This method never adds fetched Messages to the
        internal QoS object for asynchronous acking.

        This method converts the object type of the method as it passes
        through.  Fetching from the broker, _get() returns a qpid.messaging
        Message, but this method takes the payload of the qpid.messaging
        Message and instantiates a Message object based on the class
        setting of self.Message.

        :param queue: The queue name to fetch a message from.
        :type queue: str
        :param no_ack: If True, a message fetched will not be acked. If
        False, a message fetched will be acked.
        :type noack: bool
        """
        try:
            qpid_message = self._get(queue)
            raw_message = qpid_message.content
            message = self.Message(self, raw_message)
            if not no_ack:
                self._qpid_session.acknowledge(message=qpid_message)
            return message
        except Empty:
            pass

    def basic_ack(self, delivery_tag):
        """Acknowledge a message by delivery_tag.

        Acknowledges a message referenced by delivery_tag.  Messages can
        only be ack'ed using basic_ack() if they were acquired using
        basic_consume().  This is the acking portion of the asynchronous
        read behavior.

        Internally, this method uses the QoS object, which stores messages
        and is responsible for the ACKing.

        :param delivery_tag: The delivery tag associated with the message
        to be acknowledged.
        :type delivery_tag: int
        """
        self.qos.ack(delivery_tag)

    def basic_reject(self, delivery_tag, requeue=False):
        """Reject a message by delivery_tag.

        Rejects a message that has been received by the Channel, but not
        yet acknowledged.  Messages are referenced by their delivery_tag.

        If requeue is False, the rejected message will be dropped by the
        broker and not delivered to any other consumers.  If requeue is
        True, then the rejected message will be requeued for delivery to
        another consumer, potentially to the same consumer who rejected the
        message previously.

        :param delivery_tag: The delivery tag associated with the message
        to be rejected.
        :type delivery_tag: int
        :param requeue: If False, the rejected message will be dropped by
        the broker and not delivered to any other consumers.  If True,
        then the rejected message will be requeued for delivery to another
        consumer, potentially to the same consumer who rejected the message
        previously.
        :type requeue: bool

        """
        self.qos.reject(delivery_tag, requeue=requeue)

    def basic_consume(self, queue, no_ack, callback, consumer_tag, **kwargs):
        """Start an asynchronous consumer that reads from a queue.

        This method starts a consumer the reads messages from a queue
        specified by name until stopped by a call to basic_cancel(). Once a
        message is read, a call to the callback will occur with the message
        as the single argument.  The message passed to the callback is of
        type self.Message.  Each consumer is referenced by a consumer_tag,
        which is provided by the caller of this method.

        Consuming is done using a thread of type FDShimThread that is
        spawned when this method is called.  The child thread is marked as
        a daemon, indicating that if all non-daemon threads exit, the child
        consumer thread will also exit.  The child consumer thread performs
        an efficient blocking read, which wakes up regularly to see if it
        should exit.

        The child consumer thread does not call the callback directly.
        Instead, the child thread is given a threadsafe Queue.Queue object
        which it should deliver messages into.  This single queue
        aggregates all consumer messages, can be read through a call to
        drain_events() on the Transport object associated with this Channel
        object.  This method sets up the callback onto the self.connection
        object in a dict keyed by queue name.  drain_events() is
        responsible for calling that callback upon message receipt.

        Depending on the value of the no_ack parameter, the message that is
        received can be saved for asynchronous acking later after the
        message has been handled by the caller of drain_events(). Messages
        can be acked after being received through a call to basic_ack().
        If no_ack is True, then messages are not saved for acking later.
        If no_ack is False, then messages are saved for acking later.
        Internally the QoS object is used to store messages for acking later.

        basic_consume() transforms the message object type prior to calling
        the callback.  Initially the message comes in as a qpid.messaging
        Message.  This method unpacks the payload of the qpid.messaging
        Message and creates a new object of type self.Message.

        This method wraps the user delivered callback in a runtime-built
        function which provides the type transformation from qpid.messaging
        to self.Message, and adds the message to the qos object for
        asynchronous acking if necessary.

        :param queue: The name of the queue to consume messages from
        :type queue: str
        :param no_ack: If True, then messages will not be saved for
        acking later.  If False, then messages will be saved for acking
        later.
        :type no_ack: bool
        :param callback: a callable that will be called when messages
        arrive on the queue.
        :type callback: a callable object
        :param consumer_tag: a tag to reference the created consumer by.
        This consumer_tag is needed to cancel the consumer.
        :type consumer_tag: an immutable object
        """
        self._tag_to_queue[consumer_tag] = queue

        def _callback(qpid_message):
            raw_message = qpid_message.content
            message = self.Message(self, raw_message)
            if not no_ack:
                delivery_tag = message.delivery_tag
                self.qos.append(qpid_message, delivery_tag)
            return callback(message)

        self.connection._callbacks[queue] = _callback
        self._consumers.add(consumer_tag)
        my_thread = FDShimThread(self.connection.create_qpid_connection,
                                 queue, self.delivery_queue)
        self._consumer_threads[queue] = my_thread
        my_thread.daemon = True
        my_thread.start()

    def basic_cancel(self, consumer_tag):
        """Cancel consumer by consumer tag.

        Request the consumer stops reading messages from its queue. The
        consumer is a child thread, and it is told to stop by a call to
        the kill() method on the thread object.  Killing does not occur
        immediately, but will occur once the child completes its blocking
        read() and checks if it should die or not.  The thread is not
        waited on to die() because in practice there can be many consumers,
        and they are killed through a series of serial calls to
        basic_cancel() which takes a long time.

        This method also cleans up all lingering references of the consumer.

        :param consumer_tag: The tag which refers to the consumer to be
        cancelled.  Originally specified when the consumer was created as
        a parameter to basic_consume().
        :type consumer_tag: an immutable object
        """
        if consumer_tag in self._consumers:
            self._consumers.remove(consumer_tag)
            queue = self._tag_to_queue.pop(consumer_tag, None)
            consumer_thread = self._consumer_threads.pop(queue, None)
            if consumer_thread:
                consumer_thread.kill()
            self.connection._callbacks.pop(queue, None)

    def close(self):
        """Close Channel and all associated messages.

        This cancels all consumers by calling basic_cancel() for each known
        consumer_tag.  It also closes the self._qpid_session and
        self .broker_close() sessions.  Closing the sessions implicitly
        causes all outstanding, unacked messages to be considered
        undelivered by the broker.
        """
        if not self.closed:
            self.closed = True
            for consumer in list(self._consumers):
                self.basic_cancel(consumer)
            if self.connection is not None:
                self.connection.close_channel(self)
            self._qpid_session.close()
            self._broker.close()

    @property
    def qos(self):
        """:class:`QoS` manager for this channel.

        Lazily instantiates an object of type :class:`QoS` upon access to
        the self.qos attribute.
        """
        if self._qos is None:
            self._qos = self.QoS(self)
        return self._qos

    def basic_qos(self, prefetch_count, *args):
        """Change QoS settings for this channel.

        Set the number of messages this Channel can fetch and hold without
        acknowledging them.  For instance prefetch_count=3 will allow a
        maximum of 3 unacked messages to be received from the broker.

        :param prefetch_count: The number of outstanding, unacked messages
        this Channel is allowed to have.
        :type prefetch_count: int
        """
        self.qos.prefetch_count = prefetch_count

    def prepare_message(self, body, priority=None, content_type=None,
                        content_encoding=None, headers=None, properties=None):
        """Prepare message data for sending.

        Returns a dict object that encapsulates message attributes.  See
        parameters for more details on attributes that can be set.

        This message is typically called by kombu.messaging._publish() as a
        preparation step in message publication.

        :param body: The body of the message
        :type body: str
        :param priority: A number between 0 and 9 that sets the priority of
        the message.
        :type priority: int
        :param content_type: The content_type the message body should be
        treated as.  If this is unset, the qpid.messaging client tries to
        autodetect the content_type from the body.
        :type content_type: str
        :param content_encoding: The content_encoding the message body is
        encoded as.
        :type content_encoding: str
        :param headers: Additional Message headers that should be set.
        Passed in as a key-value pair.
        :type headers: dict
        :param properties: Message properties to be set on the message.
        :type properties: dict
        """
        properties = properties or {}
        info = properties.setdefault('delivery_info', {})
        info['priority'] = priority or 0

        return {'body': body,
                'content-encoding': content_encoding,
                'content-type': content_type,
                'headers': headers or {},
                'properties': properties or {}}

    def basic_publish(self, message, exchange, routing_key, **kwargs):
        """Publish message onto an exchange using a routing key.

        Publish a message onto an exchange specified by name using a
        routing key specified by routing_key.  Prepares the message in the
        following ways before sending:

        - encodes the body using self.encode_body()
        - wraps the body as a buffer object, so that the qpid.messaging
        uses a content type that can support arbitrarily large messages.
        - assigns a delivery_tag generated through self._delivery_tags
        - sets the exchange and routing_key info as delivery_info

        Internally uses _put() to send the message synchronously.  This
        message is typically called by kombu.messaging._publish() as the
        final step in message publication.

        :param message: A dict containing key value pairs with the message
        data.  A valid message dict can be generated using the
        prepare_message() method.
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
        message['body'] = buffer(message['body'])
        props = message['properties']
        props.update(
            body_encoding=body_encoding,
            delivery_tag=next(self._delivery_tags),
        )
        props['delivery_info'].update(
            exchange=exchange,
            routing_key=routing_key,
        )
        return self._put(routing_key, message, exchange, **kwargs)

    def encode_body(self, body, encoding=None):
        """Encode a body using an optionally specified encoding.

        The encoding can be specified by name, and is looked up in
        self.codecs.  self.codecs uses strings as its keys which specify
        the name of the encoding, and then the value is an instantiated
        object that can provide encoding/decoding of that type through
        calls to encode() and decode().

        Returns a tuple with the first position being the encoded body,
        and the second position the encoding used.

        If encoding is not specified, the body is passed through unchanged.

        :param body: The body to be encoded.
        :type body: str
        :param encoding: The encoding type to be used.  Must be a supported
        codec listed in self.codecs, which
        :type encoding: str
        """
        if encoding:
            return self.codecs.get(encoding).encode(body), encoding
        return body, encoding

    def decode_body(self, body, encoding=None):
        """Decode a body using an optionally specified encoding.

        The encoding can be specified by name, and is looked up in
        self.codecs.  self.codecs uses strings as its keys which specify
        the name of the encoding, and then the value is an instantiated
        object that can provide encoding/decoding of that type through
        calls to encode() and decode().

        Returns the decoded body.

        If encoding is not specified, the body is returned unchanged.

        :param body: The body to be encoded.
        :type body: str
        :param encoding: The encoding type to be used.  Must be a supported
        codec listed in self.codecs, which
        :type encoding: str
        """
        if encoding:
            return self.codecs.get(encoding).decode(body)
        return body

    def typeof(self, exchange, default='direct'):
        """Get the exchange type instance for `exchange`.

        Lookup and return the exchange type for an exchange specified by
        name.  Exchange types are expected to be 'direct', 'topic',
        and 'fanout', which correspond with exchange functionality as
        specified in AMQP 0-10 and earlier.  If the exchange cannot be
        found, the default exchange type is returned.

        :param exchange: The exchange to have its type lookup up.
        :type exchange: str
        :param default: The type of exchange to assume if the exchange does
        not exist.
        :type default: str
        """
        qpid_exchange = self._broker.getExchange(exchange)
        if qpid_exchange:
            qpid_exchange_attributes = qpid_exchange.getAttributes()
            return qpid_exchange_attributes["type"]
        else:
            return default


class FDShimThread(threading.Thread):
    """A consumer thread that reads and handles messages from a single queue.

    An instance of FDShimThread will asynchronously read messages from a
    single queue, and deliver each message read into a threadsafe
    Queue.Queue object delivery_queue.  The broker queue to read from is
    specified by name.  A separate thread of type FDShim is designed to
    receive and handle messages that FDShimThread object put into the
    delivery_queue.

    FDShimThread objects are designed to be efficient through a blocking
    read from the broker's queue.  Periodically the FDShimThread wakes up
    from the blocking read, and checks to see if it has been killed.  If it
    has not been killed it begins a new blocking read.  The blocking
    timeout is set through the class attribute block_timeout that contains
    the timeout in seconds.

    FDShimThread requires a function to be passed in that will allow the
    FDShimThread to generate a connection to the broker.  FDShimThread uses
    the passed in function to get the connection, start a session with the
    broker, and then create a _receiver to consuming messages from the
    named queue.

    An FDShimThread instance can be notified that they should die by a call
    to self.kill().  The thread may not exit immediately because it may
    be in a blocking read, but it will exit before entering the next
    blocking read.  When the thread exits properly, it gracefully closes the
    _receiver and _session objects that were created.

    FDShimThread objects are not designed to be used directly by objects
    other than Channel.  An FDShimThread is created by a call to
    Channel.basic_consume(), and destroyed through a call to Channel
    .basic_cancel().  The thread entry point is the run() method.
    Channel.basic_consume() daemonizes the thread before calling start()
    ensuring an FDShimThread will never keep the Python process alive if all
    other non-daemon threads have exited.  The Channel maintains references
    to the FDShimThread instances it creates for killing later.

    :param create_qpid_connection: A function that will return a valid
    qpid.messaging Connection when called with no arguments.
    :type create_qpid_connection: function
    :param queue: The name of the queue to consume messages from.
    :type queue: str
    :param delivery_queue: The threadsafe Queue.Queue object to deliver
    qpid.messaging Messages into once read from the broker.
    :type delivery_queue: Queue.Queue
    """
    # The timeout that blocking reads should occur for before waking up.
    block_timeout = 10

    def __init__(self, create_qpid_connection, queue, delivery_queue):
        self._session = create_qpid_connection().session()
        self._receiver = self._session.receiver(queue)
        self._queue = queue
        self._delivery_queue = delivery_queue
        self.is_killed = False
        super(FDShimThread, self).__init__()

    def run(self):
        """Thread entry point for FDShimThread instances"""
        while not self.is_killed:
            try:
                response = self._receiver.fetch(
                    timeout=FDShimThread.block_timeout)
            except QpidEmpty:
                pass
            else:
                queue = self._receiver.source
                response_bundle = (queue, response)
                self._delivery_queue.put(response_bundle)
        self._receiver.close()
        self._session.close()

    def kill(self):
        """Notify the thread that it should die at the earliest opportunity.

        The thread may not exit immediately because it may be in a blocking
        read.  It will exit gracefully before entering the next blocking read.
        """
        self.is_killed = True


class FDShim(object):
    """Monitor and handle messages from all consumer threads.

    The FDShim object monitors incoming messages from all consumers
    through a blocking read on the threadsafe queue that consumers
    deliver messages into, delivery_queue.  Once a message is received
    by FDShim, an externally monitorable file descriptor is set that data
    is ready for the transport, and the message is put into a separate
    threadsafe queue self.queue_from_fdshim.

    The FDShim object provides a read file descriptor named self.r which
    can be monitored by an external epoll-like event I/O notification
    system.  An external epoll loop would monitor self.r when it wants to
    be notified efficiently that the Transport associated with this FDShim
    has data available for reading.  The client library qpid.messaging does
    not make available read file descriptors for external monitoring,
    and so FDShim provides this functionality by creating os.pipe() based
    file descriptors that it writes into causing external epoll loops
    to efficiently "wake up" at the correct time.

    FDShim objects are designed to be used by a Transport, and should
    not be used by external objects directly.  Each Transport creates
    exactly one FDShim object to monitor and handle messages from all
    consumers associated with all Channels associated with the Transport.
    The thread entry point is monitor_consumers().  The transport daemonizes
    the thread before calling start() ensuring an FDShim will never keep the
    Python process alive if all other non-daemon threads have exited.

    :param queue_from_fdshim: The queue that that messages which are ready
    for reading are put into so that the Transport can drain them in
    Transport drain_events()
    :type queue_from_fdshim: Queue.Queue
    :param delivery_queue: The queue that FDShim performs a blocking
    read on to receive messages form all consumers associated with all
    Channels associated with the Transport that created FDShim.
    :type delivery_queue: Queue.Queue
    """
    def __init__(self, queue_from_fdshim, delivery_queue):
        self.queue_from_fdshim = queue_from_fdshim
        self.delivery_queue = delivery_queue
        self.r, self._w = os.pipe()
        self._is_killed = False

    def kill(self):
        """Notify the thread that it should die at the earliest opportunity.

        The thread may not exit immediately because it may be in a blocking
        read.  It will exit gracefully before entering the next blocking read.
        """
        self._is_killed = True

    def monitor_consumers(self):
        """The thread entry point.

        Do a blocking read call similar to what qpid.messaging does, and when
        something is finally received, set the pipe as being readable,
        and then put the message into the queue_from_fdshim object.

        Setting the pipe as being readable is done by writing a single '0'
        character into the pipe so that anything monitoring it will receive
        the ready for reading signal.
        """
        while not self._is_killed:
            try:
                response_bundle = self.delivery_queue.get(block=True)
            except Queue.Empty:
                pass
            else:
                self.queue_from_fdshim.put(response_bundle)
                os.write(self._w, '0')


class Connection(object):
    """Encapsulate a connection object for the Transport.

    A Connection object is created by a Transport during a call to
    Transport.establish_connection().  The Transport passes in Connection
    options that should be used for any Connections created by the broker.
    Each Transport creates exactly one Connection.

    Objects that use connections to the broker such as
    Channel, QoS, and FDShimThread objects need to have independent
    Connections generated.  Any part of this codebase can get a valid
    connection to the broker with parameters saved in this object by
    calling the bound create_qpid_connection() method.

    The Connection object is also responsible for maintaining the
    dictionary of reference to callbacks that should be called when
    messages are received.  These callbacks are saved in _callbacks,
    and keyed on the queue name associated with the received message.  The
    _callbacks are setup in Channel.basic_consume(), removed in
    Channel.basic_cancel(), and called in Transport.drain_events().

    The following keys are expected to be in the connection_options dict at
    a minimum:

      host: The host that connections should connect to.
      port: The port that connection should connect to.
      username: The username that connections should connect with.
      password: The password that connections should connect with.
      transport: The transport type that connections should use.  Either
                'tcp', or 'ssl' are expected as values.
      timeout: the timeout to use when a Connection connects to the broker.
      sasl_mechanisms: The sasl authentication mechanism type to use.
                       refer to SASL documentation for an explanation of
                       valid values.

    :param connection_options: A dict containing the connection options.
    This object expects a minimum number of keys in the dict which are
    explained in the documentation on the object itself.
    :type connection_options: dict
    """

    # A class reference to the Channel object
    Channel = Channel

    def __init__(self, **connection_options):
        self.connection_options = connection_options
        self.channels = []
        self._callbacks = {}

    def create_qpid_connection(self):
        """Create a qpid.messaging Connection with saved connection parameters

        Creates a qpid.messaging Connection object with the saved
        parameters that were passed into the Connection at instantiation
        time.
        """
        return QpidConnection.establish(**self.connection_options)

    def close_channel(self, channel):
        """Close a Channel.

        Close a channel specified by a reference to the Channel object.

        :param channel: Channel that should be closed.
        :type channel: Channel
        """
        try:
            self.channels.remove(channel)
        except ValueError:
            pass
        finally:
            channel.connection = None


class Transport(base.Transport):
    """Kombu native transport for a Qpid broker.

    Provide a native transport for Kombu that allows consumers and
    producers to read and write messages to/from a broker.  This Transport
    is capable of supporting both synchronous and asynchronous reading.
    All writes are synchronous through the Channel objects that support
    this Transport.

    Synchronous reads are done using a call to drain_events(),
    which synchronously reads events, and then handles them through
    calls to the callback handlers maintained on the Connection object.

    Asynchronous reads are done by monitoring the file descriptor
    self.fd_shim.r which will be sent the signal indicating it is ready for
    reading when messages are ready to be read.  Monitoring of this file
    descriptor should pair with on_readable() as the callback to call when
    the external loop is ready to read and handle messages that are
    associated with this Transport.

    The Transport also provides methods to establish and close a connection
    to the broker.  This Transport establishes a factory-like pattern that
    allows for lazy creation of Connections as needed.

    The Transport can create Channels to communicate with the broker with
    using the create_channel() method.

    :param client: A reference to the creator of the Transport.
    :type client: kombu.connection.Connection
    """
    # Reference to the class that should be used as the Connection object
    Connection = Connection

    # The default port
    default_port = DEFAULT_PORT

    # This Transport does not support polling as its primary fetching model.
    polling_interval = None

    # This Transport does support an asynchronous event model.
    supports_ev = True

    # Unused error classification to identify recoverable vs
    # non-recoverable errors on Channels and Connections.  These should be
    # set to a tuple instead of None if they are to be used.
    # channel_errors = None
    # recoverable_channel_errors = None
    # connection_errors = None
    # recoverable_connection_errors = None

    # The driver type and name for identification purposes.
    driver_type = 'qpid'
    driver_name = 'qpid'

    def __init__(self, client, **kwargs):
        self.client = client
        self.queue_from_fdshim = Queue.Queue()
        self.delivery_queue = Queue.Queue()
        self.fd_shim = FDShim(self.queue_from_fdshim, self.delivery_queue)
        fdshim_thread = threading.Thread(
            target=self.fd_shim.monitor_consumers)
        fdshim_thread.daemon = True
        fdshim_thread.start()

    def register_with_event_loop(self, connection, loop):
        """Register a file descriptor and callback with the loop

        Register the callback self.on_readable to be called when an
        external epoll loop sees that the file descriptor registered is
        ready for reading.  The file descriptor is created and updated by
        FDShim, which is created by the Transport at instantiation time.

        When supports_ev = True, Celery expects to call this method to give
        the Transport an opportunity to register a read file descriptor for
        external monitoring by celery using an Event I/O notification
        mechanism such as epoll.  A callback is also registered that is to
        be called once the external epoll loop is ready to handle the epoll
        event associated with messages that are ready to be handled for
        this Transport.

        The registration call is made exactly once per Transport after the
        Transport is finished instantiating.

        :param connection: A reference to the connection associated with
        this Transport.
        :type connection: Connection
        :param loop: A reference to the external loop.
        :type loop: kombu.async.hub.Hub
        """
        loop.add_reader(self.fd_shim.r, self.on_readable, connection, loop)

    def establish_connection(self):
        """Establish a Connection object.

        Determines the correct options to use when creating any Connections
        needed by this Transport, and create a Connection object which
        saves those values for Connections generated as they are needed.
        The options are a mixture of what is passed in through the creator
        of the Transport, and the defaults provided by
        self.default_connection_params.  Options cover broker network
        settings, timeout behaviors, authentication, and identity
        verification settings.

        The Connection object that is created is returned back to the
        caller of this function.
        """
        conninfo = self.client
        for name, default_value in items(self.default_connection_params):
            if not getattr(conninfo, name, None):
                setattr(conninfo, name, default_value)
        if conninfo.hostname == 'localhost':
            conninfo.hostname = '127.0.0.1'
        if conninfo.ssl:
            conninfo.qpid_transport = 'ssl'
            conninfo.transport_options['ssl_keyfile'] = conninfo.ssl[
                'keyfile']
            conninfo.transport_options['ssl_certfile'] = conninfo.ssl[
                'certfile']
            conninfo.transport_options['ssl_trustfile'] = conninfo.ssl[
                'ca_certs']
            if conninfo.ssl['cert_reqs'] == ssl.CERT_REQUIRED:
                conninfo.transport_options['ssl_skip_hostname_check'] = False
            else:
                conninfo.transport_options['ssl_skip_hostname_check'] = True
        else:
            conninfo.qpid_transport = 'tcp'
        opts = dict({'host': conninfo.hostname, 'port': conninfo.port,
                     'username': conninfo.userid,
                     'password': conninfo.password,
                     'transport': conninfo.qpid_transport,
                     'timeout': conninfo.connect_timeout,
                     'sasl_mechanisms': conninfo.sasl_mechanisms},
                    **conninfo.transport_options or {})
        conn = self.Connection(**opts)
        conn.client = self.client
        return conn

    def close_connection(self, connection):
        """Close the Connection object, and all associated Channels.

        Iterates through all Channels associated with the Connection,
        pops them from the list of channels, and call close() on each
        Channel.

        :param connection: The Connection that should be closed
        :type connection: Connection
        """
        for l in connection.channels:
            while l:
                try:
                    channel = l.pop()
                except (IndexError, KeyError):  # pragma: no cover
                    pass
                else:
                    channel.close()

    def drain_events(self, connection, timeout=0, **kwargs):
        """Handle and call callbacks for all ready Transport messages.

        Drains all events that are ready for consuming from FDShim.
        Messages must pass through FDShim so that an external read file
        descriptor can be marked as readable, to allow asynchronous I/O to
        properly occur.

        For each drained event, the message is called to the appropriate
        callback.  Callbacks are organized by queue name.  The object that
        is returned from queue_from_fdshim is a tuple containing the queue
        name, and the message, in that order.

        :param connection: The Connection that contains the callbacks,
        indexed by queue name, which will be called by this method.
        :type connection: Connection
        :param timeout: The timeout that limits how long drain_events() will
        run for.  The timeout could interrupt a blocking read that is
        waiting for a new message, or cause drain_events() to return before
        all messages are drained.
        :type timeout: int
        """
        start_time = clock()
        elapsed_time = -1
        while elapsed_time < timeout:
            try:
                queue, message = self.queue_from_fdshim.get(block=True,
                                                            timeout=timeout)
            except Queue.Empty:
                raise socket.timeout()
            else:
                connection._callbacks[queue](message)
            elapsed_time = clock() - start_time
        raise socket.timeout()

    def create_channel(self, connection):
        """Create and return a channel.

        Creates a new Channel, and append the Channel to the list of
        channels known by the Connection.  Once the new Channel is created,
        it is returned.

        :param connection: The connection that should back the new Channel.
        :type connection: Connection
        """
        channel = connection.Channel(connection, self, self.delivery_queue)
        connection.channels.append(channel)
        return channel

    def on_readable(self, connection, loop):
        """Handle any read events associated with this Transport.

        This method clears a single message from the externally monitored
        file descriptor by issuing a read call to the self.fd_shim pipe,
        which removes a single '0' character that was placed into the pipe
        by FDShim. Once a '0' is read, all available events are drained
        through a call to self.drain_events().

        Nothing is expected to be returned from drain_events() because
        drain_events() handles messages by calling callbacks that are
        maintained on the Connection object.  When drain_events() returns,
        all associated messages have been handled.

        This method reads as many messages that are available for this
        Transport, and then returns.  It blocks in the sense that reading
        and handling a large number of messages may take time, but it does
        not block waiting for a new message to arrive.  When drain_events()
        is called a timeout is not specified, which causes this behavior.

        One interesting behavior of note is where multiple messages are
        ready, and this method removes a single '0' character from
        fd_shim.r, but drain_events() may handle an arbitrary amount of
        messages.  In that case, extra '0' characters may be left on fd_shim
        to be read, where messages corresponding with those '0' characters
        have already been handled.  The external epoll loop will incorrectly
        think additional data is ready for reading, and will call
        on_readable unnecessarily, once for each '0' to be read. Additional
        calls to on_readable() produce no negative side effects, and will
        eventually clear out the fd_shim pipe of all symbols correctly.

        :param connection: The connection associated with the readable
        events, which contains the callbacks that need to be called for the
        readables.
        :type connection: Connection
        :param loop: The asynchronous loop object that contains epoll like
        functionality.
        :type loop: kombu.async.Hub
        """
        result = os.read(self.fd_shim.r, 1)
        if result == '0':
            try:
                self.drain_events(connection)
            except socket.timeout:
                pass

    @property
    def default_connection_params(self):
        """Return a dict with default connection parameters.

        These Connection parameters will be used whenever the creator of
        Transport does not specify a required parameter.
        """
        return {'userid': 'guest', 'password': 'guest',
                'port': self.default_port, 'virtual_host': '',
                'hostname': 'localhost', 'sasl_mechanisms': 'PLAIN'}