from __future__ import absolute_import

"""
kombu.transport.qpid
=======================

qpid transport.

"""

"""Kombu transport using the Django database as a message store."""

import os
import uuid
import base64
import threading
import Queue
import socket
import ssl
from time import clock
from itertools import count

from kombu.transport import virtual
from kombu.five import Empty, items
from kombu.utils import kwdict
from kombu.utils.compat import OrderedDict
from kombu.utils.encoding import str_to_bytes, bytes_to_str

from amqp.protocol import queue_declare_ok_t

from qpid.messaging import Connection as QpidConnection
from qpid.messaging import Message as QpidMessage
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
                    err = AssertionFailed(text="expected %s, got %s" % (expected, type))
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


class ProtonExceptionHandler(object):
    """An exception handling decorator that silences some exceptions.

    An exception handling class designed to silence specific exceptions
    that Proton raises as part of normal operation. Proton exceptions
    require string parsing, and are not machine consumable, This is
    designed to be used as a decorator, and accepts a whitelist string as
    an argument.

    Usage:
    @ProtonExceptionHandler('whitelist string goes here')

    :param allowed_exception_string: a string that, if present in the
    exception message, will be silenced.
    :type allowed_exception_string: str
    """

    def __init__(self, allowed_exception_string):
        self.allowed_exception_string = allowed_exception_string

    def __call__(self, original_func):
        """The decorator method.

        Method that wraps the actual function with exception silencing
        functionality. Any exception that contains the string self
        .allowed_exception_string in the message will be silenced.

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
                if decorator_self.allowed_exception_string not in error.message:
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

        :param s
        :type s: str
        """
        return bytes_to_str(base64.b64encode(str_to_bytes(s)))

    def decode(self, s):
        """Decode a string using Base64

        :param s
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
        """Return True if the Channel can consume more messages, false otherwise.

        Used to ensure the client adheres to currently active prefetch
        limits.

        """
        pcount = self.prefetch_count
        return not pcount or len(self._not_yet_acked) < pcount

    def can_consume_max_estimate(self):
        """Return the remaining message capacity for a Channel using this object.

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
        #TODO test behavior if delivery_tag is not valid
        """
        Get an un-ACKed message by delivery_tag.

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

        Explicitly notify the broker that this Channel is rejecting the
        Message.

        :param delivery_tag: The delivery tag associated with the message
        to be rejected.
        :type delivery_tag: int
        :param requeue: If True, the broker will be notified to requeue
        the message.  If False, the broker will be told to drop the message
        entirely.  In both cases, the message will be removed from this
        object.
        :type requeue: bool
        """
        #TODO this should forcibly reject the message by setting the message state to invalid
        #TODO proper support for requeue should be implemented
        #TODO add requeue to docstring
        self._not_yet_acked.pop(delivery_tag)


class Message(base.Message):
    """Encodes message data in an organized way and serializes.

    Identical to the Message object used by Virtual Transports,
    and supports basic encoding/decoding and serialization.


    :param channel: The Channel associated with the message. A reference
    to Channel is needed to ensure serialization/encoding/decoding is
    supported by the Channel.
    :type channel: Channel
    :param payload: the payload of the message
    :type payload: ???
    """
    #TODO determine the type of payload and add to docstring

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
        #TODO: consolidate these connections so the objects that need them instantiate them inline
        qpid_qmf_connection = connection.create_qpid_connection()
        qpid_publish_connection = connection.create_qpid_connection()
        self._qpid_session = qpid_publish_connection.session()
        self._broker = BrokerAgent(qpid_qmf_connection)
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
        #TODO properly close the receiver in the event that the Empty
        # exception is raised.
        rx = self._qpid_session.receiver(queue)
        message = rx.fetch(timeout=0)
        rx.close()
        return message

    def _put(self, queue, message, exchange=None, **kwargs):
        """Synchronous send of a single message onto a queue or exchange.

        An internal method which synchronously sends a single message onto
        a given queue or exchange.  If exchange is not specified,
        the message is sent directly to a queue specified by name.  If no
        queue is found an exception is raised.  If an exchange is
        specified, then the message is delivered onto the requested
        exchange and the queue name is used as the routing key. Message
        sending is synchronous using sync=True because large messages in
        kombu funtests were not being fully sent before the receiver closed.

        This method creates a sender to send the message to the queue using
        the session referenced by _qpid_session.  The sender is closed
        before the method exits.

        This is an internal method. External calls for put functionality
        should be done using basic_publish().

        :param queue: The queue name to get the message from
        :type queue: str
        :param message: The message to be sent
        :type message: ???
        :param exchange: keyword parameter of the exchange this message should be sent on. If not exchange is specified
        the message is sent directly to the queue name.
        :type exchange: str
        """
        #TODO determine type of message parameter
        if not exchange:
            address = '%s; {assert: always, node: {type: queue}}' % queue
            msg_subject = None
        else:
            address = '%s/%s; {assert: always, node: {type: topic}}' % (exchange, queue)
            msg_subject = str(queue)
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
        msgDepth = queue_to_check.values['msgDepth']
        return msgDepth

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

    @ProtonExceptionHandler('object already exists')
    def _new_queue(self, queue, **kwargs):
        """Create a new queue specified by name.

        An internal method to create a new queue specified by name. If the
        queue already exists, an exception is raise, which is caught and
        silenced by the @ProtonExceptionHandler decorator.

        This is an internal method.  External calls for queue creation
        functionality should be done using queue_declare().

        This is an internal method,
        :param queue: the name of the queue to be created.
        :type queue: str
        """
        self._broker.addQueue(queue)

    def _has_queue(self, queue, **kwargs):
        #TODO: implement me
        #TODO: write docstring
        raise NotImplementedError

    def _poll(self, cycle, timeout=None):
        #TODO: implement me
        #TODO: write docstring
        raise NotImplementedError('_poll Not Implemented')

    def queue_declare(self, queue=None, passive=False, **kwargs):
        """Create a new queue specified by name.

        If a queue already exists, no action is taken and no exceptions are
        raised.  If the queue name is not specified, a queue name is
        generated using uuid(). This method uses _new_queue() internally.

        :param queue: the name of the queue to be created.
        :type queue: str
        :param passive: ???
        :type passive: ???
        """
        #TODO: implement passive correctly and update docstring
        #TODO document return behavior
        queue = queue or 'amq.gen-%s' % uuid()
        if passive and not self._has_queue(queue, **kwargs):
            raise ChannelError(
                'NOT_FOUND - no queue {0!r} in vhost {1!r}'.format(
                    queue, self.connection.client.virtual_host or '/'),
                (50, 10), 'Channel.queue_declare', '404',
            )
        else:
            self._new_queue(queue, **kwargs)
        return queue_declare_ok_t(queue, self._size(queue), 0)

    #TODO add queue_delete(self, queue, if_unused=False, if_empty=False, **kwargs) method

    @ProtonExceptionHandler('object already exists')
    def exchange_declare(self, *args, **kwargs):
        """Create a new exchange.

        Create an exchange of a specific type, and optionally have the
        exchange be durable.  If an exchange of the requested name already
        exists, no action is taken and no exceptions are raised.  Durable
        exchanges will survive a broker restart, non-durable exchanges will not.

        Exchanges provide behaviors based on their type.  The expected
        behaviors are those defined in the AMQP 0-10 and prior
        specifications including 'direct', 'topic', and 'fanout'
        funcitonality.

        :param type: The exchange type. Valid values include 'direct',
        'topic', and 'fanout'.
        :type type: str
        :param exchange: The name of the exchange to be created.
        :type exchange: str
        :param durable: True if the exchange should be durable, or False
        otherwise.
        :type durable: bool
        """
        #TODO: update signature to not dynamically unpack kwargs
        #TODO: allow exchange name to be blank and update docstring
        e_type = kwargs['type']
        e_name = kwargs['exchange']
        e_durable = kwargs.get('durable', False)
        options = {'durable': e_durable}
        self._broker.addExchange(e_type, e_name, options)

    def exchange_delete(self, exchange_name, **kwargs):
        """Delete an exchange specified by name

        :param exchange_name: The name of the exchange to be deleted.
        :type exchange_name: str
        """
        self._broker.delExchange(exchange_name)

    def after_reply_message_received(self, queue):
        #TODO investigate the correct behavior of this message.  It should
        # not just return.  This is related to the event broadcast behavior
        # of celery control().
        #TODO write docstring
        return
        self._delete(queue)

    def queue_bind(self, *args, **kwargs):
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
        #TODO: update signature to not dynamically unpack kwargs
        queue = kwargs['queue']
        exchange = kwargs['exchange']
        key = kwargs['routing_key']
        self._broker.bind(exchange, queue, key)

    def queue_unbind(self, *args, **kwargs):
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
        queue = kwargs['queue']
        exchange = kwargs['exchange']
        key = kwargs['routing_key']
        self._broker.unbind(exchange, queue, key)

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

        Internally this method uses _get() to fetch the messsage.  If and
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
        self.qos.ack(delivery_tag)

    def basic_reject(self, delivery_tag, requeue=True):
        #TODO: verify that requeue=True is the right signature
        #TODO: implement me
        raise NotImplementedError('basic_reject is not implemented')

    def basic_consume(self, queue, no_ack, callback, consumer_tag, **kwargs):
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
        my_thread = FDShimThread(self.connection.create_qpid_connection, queue, self.delivery_queue)
        self._consumer_threads[queue] = my_thread
        my_thread.daemon = True
        my_thread.start()

    def basic_cancel(self, consumer_tag):
        """Cancel consumer by consumer tag."""
        if consumer_tag in self._consumers:
            self._consumers.remove(consumer_tag)
            queue = self._tag_to_queue.pop(consumer_tag, None)
            consumer_thread = self._consumer_threads.pop(queue, None)
            if consumer_thread:
                consumer_thread.kill()
            self.connection._callbacks.pop(queue, None)

    def close(self):
        """Close channel, cancel all consumers, and requeue unacked
        messages."""
        if not self.closed:
            self.closed = True
            for consumer in list(self._consumers):
                self.basic_cancel(consumer)
            if self.connection is not None:
                self.connection.close_channel(self)
            self._qpid_session.close()
            self._broker.close()

    def acquire(self, *arg, **kwargs):
        raise NotImplementedError('acquire Not Implemented')

    @property
    def qos(self):
        """:class:`QoS` manager for this channel."""
        if self._qos is None:
            self._qos = self.QoS(self)
        return self._qos

    def basic_qos(self, prefetch_size=0, prefetch_count=0,
                  apply_global=False):
        """Change QoS settings for this channel.

        Only `prefetch_count` is supported.

        """
        self.qos.prefetch_count = prefetch_count

    def prepare_message(self, body, priority=None, content_type=None,
                        content_encoding=None, headers=None, properties=None):
        """Prepare message data."""
        properties = properties or {}
        info = properties.setdefault('delivery_info', {})
        info['priority'] = priority or 0

        return {'body': body,
                'content-encoding': content_encoding,
                'content-type': content_type,
                'headers': headers or {},
                'properties': properties or {}}

    def basic_publish(self, message, exchange, routing_key, **kwargs):
        """Publish message."""
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
        if encoding:
            return self.codecs.get(encoding).encode(body), encoding
        return body, encoding

    def decode_body(self, body, encoding=None):
        if encoding:
            return self.codecs.get(encoding).decode(body)
        return body


    def typeof(self, exchange, default='direct'):
        """Get the exchange type instance for `exchange`."""
        qpid_exchange = self._broker.getExchange(exchange)
        if qpid_exchange:
            qpid_exchange_attributes = qpid_exchange.getAttributes()
            return qpid_exchange_attributes["type"]
        else:
            return default


class FDShimThread(threading.Thread):
    def __init__(self, create_qpid_connection, queue, delivery_queue):
        self._session = create_qpid_connection().session()
        self._receiver = self._session.receiver(queue)
        self._queue = queue
        self._delivery_queue = delivery_queue
        self.is_killed = False
        super(FDShimThread, self).__init__()

    def run(self):
        while not self.is_killed:
            try:
                response = self._receiver.fetch(timeout=10)
            except QpidEmpty:
                pass
            else:
                queue = self._receiver.source
                response_bundle = (queue, response)
                self._delivery_queue.put(response_bundle)
        self._receiver.close()
        self._session.close()

    def kill(self):
        self.is_killed = True


class FDShim(object):
    """
    The FDShim object is monitoring the Queue.Queue for incoming messages
    from all consumers.  Once a message is ready FDShim indicates a message is ready for reading on the file descriptor it makes available to anyone who wants to monitor the Transport for inbound messages.
    of
    consumers is handled by
    the and deletion of consumers is handled by a FDShim object, which itself runs in a separate thread
    and monitors all of the consumers.  Each Transport has exactly one FDShim object.  Each consumer is a FDShimThread
    object that is started or signalled to stop.  All signalling and message passing between threads is done using
    thread safe Queue.Queue objects.
    """

    def __init__(self, connection, queue_from_fdshim, delivery_queue):
        self.queue_from_fdshim = queue_from_fdshim
        self.delivery_queue = delivery_queue
        self.connection = connection
        self.r, self._w = os.pipe()
        self._is_killed = False

    def kill(self):
        self.is_killed = True

    def monitor_consumers(self):
        """
        Do a blocking read call similar to what qpid.messaging does, and when
        something is finally received, shove it into the pipe.
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
    Channel = Channel

    def __init__(self, **connection_options):
        self.connection_options = connection_options
        self.channels = []
        self._callbacks = {}

    def create_qpid_connection(self):
        return QpidConnection.establish(**self.connection_options)

    def close_channel(self, channel):
        try:
            self.channels.remove(channel)
        except ValueError:
            pass
        finally:
            channel.connection = None


class Transport(base.Transport):
    """
    Synchronous reads are done using a call to drain_events() which accepts a timeout is read for polling based
    usage.  Kombu uses drain_events() regularly.

    Asynchronous reads are done using a call to ..................................
    """
    Connection = Connection

    default_port = DEFAULT_PORT
    polling_interval = None
    supports_ev = True
    __reader = None

    #channel_errors = (
    #    virtual.Transport.channel_errors
    #)
    #import amqp
    #connection_errors = amqp.Connection.connection_errors
    #channel_errors = amqp.Connection.channel_errors
    #recoverable_connection_errors = \
    #    amqp.Connection.recoverable_connection_errors
    #recoverable_channel_errors = amqp.Connection.recoverable_channel_errors

    driver_type = 'qpid'
    driver_name = 'qpid'

    def __init__(self, client, **kwargs):
        self.client = client
        self.queue_from_fdshim = Queue.Queue()
        self.delivery_queue = Queue.Queue()
        self.fd_shim = FDShim(self, self.queue_from_fdshim, self.delivery_queue)
        fdshim_thread = threading.Thread(target=self.fd_shim.monitor_consumers)
        fdshim_thread.daemon = True
        fdshim_thread.start()

    def register_with_event_loop(self, connection, loop):
        loop.add_reader(self.fd_shim.r, self.on_readable, connection, loop)

    def establish_connection(self):
        conninfo = self.client
        for name, default_value in items(self.default_connection_params):
            if not getattr(conninfo, name, None):
                setattr(conninfo, name, default_value)
        if conninfo.hostname == 'localhost':
            conninfo.hostname = '127.0.0.1'
        if conninfo.ssl:
            conninfo.qpid_transport = 'ssl'
            conninfo.transport_options['ssl_keyfile'] = conninfo.ssl['keyfile']
            conninfo.transport_options['ssl_certfile'] = conninfo.ssl['certfile']
            conninfo.transport_options['ssl_trustfile'] = conninfo.ssl['ca_certs']
            if conninfo.ssl['cert_reqs'] == ssl.CERT_REQUIRED:
                conninfo.transport_options['ssl_skip_hostname_check'] = False
            else:
                conninfo.transport_options['ssl_skip_hostname_check'] = True
        else:
            conninfo.qpid_transport = 'tcp'
        opts = dict({
                        'host': conninfo.hostname,
                        'port': conninfo.port,
                        'username': conninfo.userid,
                        'password': conninfo.password,
                        'transport': conninfo.qpid_transport,
                        'timeout': conninfo.connect_timeout,
                        'sasl_mechanisms': conninfo.sasl_mechanisms
                    }, **conninfo.transport_options or {})
        conn = self.Connection(**opts)
        conn.client = self.client
        return conn

    def close_connection(self, connection):
        for l in connection.channels:
            while l:
                try:
                    channel = l.pop()
                except (IndexError, KeyError):  # pragma: no cover
                    pass
                else:
                    channel.close()

    def drain_events(self, connection, timeout=0, **kwargs):
        start_time = clock()
        elapsed_time = -1
        while elapsed_time < timeout:
            try:
                queue, message = self.queue_from_fdshim.get(block=True, timeout=timeout)
            except Queue.Empty:
                raise socket.timeout()
            else:
                connection._callbacks[queue](message)
            elapsed_time = clock() - start_time
        raise socket.timeout()

    def create_channel(self, connection):
        channel = connection.Channel(connection, self, self.delivery_queue)
        connection.channels.append(channel)
        return channel

    def on_readable(self, connection, loop):
        result = os.read(self.fd_shim.r, 1)
        if result == '0':
            try:
                self.drain_events(connection)
            except socket.timeout:
                pass

    @property
    def default_connection_params(self):
        return {'userid': 'guest', 'password': 'guest',
                'port': self.default_port, 'virtual_host': '',
                'hostname': 'localhost', 'sasl_mechanisms': 'PLAIN'}