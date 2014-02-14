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
from time import clock
from itertools import count


from kombu.transport import virtual
from kombu.five import Empty
from kombu.utils import kwdict
from kombu.utils.compat import OrderedDict
from kombu.utils.encoding import str_to_bytes, bytes_to_str

from amqp.protocol import queue_declare_ok_t

from qpid.messaging import Connection as QpidConnection
from qpid.messaging import Message as QpidMessage
from qpid.messaging.exceptions import Empty as QpidEmpty
from qpidtoollibs import BrokerAgent

from . import base

DEFAULT_PORT = 5672


VERSION = (1, 0, 0)
__version__ = '.'.join(map(str, VERSION))


class ProtonExceptionHandler(object):

    def __init__(self, allowed_exception_string):
        self.allowed_exception_string = allowed_exception_string

    def __call__(self, original_func):
        decorator_self = self

        def decorator(*args, **kwargs):
            try:
                original_func(*args, **kwargs)
            except Exception as error:
                if decorator_self.allowed_exception_string not in error.message:
                    raise
        return decorator


class Base64(object):

    def encode(self, s):
        return bytes_to_str(base64.b64encode(str_to_bytes(s)))

    def decode(self, s):
        return base64.b64decode(str_to_bytes(s))


class QoS(object):
    """Quality of Service guarantees.

    Only supports `prefetch_count` at this point.

    :param channel: AMQ Channel.
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
        self._qpid_session = self.channel.connection.fd_shim._qpid_session

    def can_consume(self):
        """Return true if the channel can be consumed from.

        Used to ensure the client adhers to currently active
        prefetch limits.

        """
        pcount = self.prefetch_count
        return True
        return not pcount or len(self._not_yet_acked) - len(self._dirty) < pcount

    def can_consume_max_estimate(self):
        """Returns the maximum number of messages allowed to be returned.

        Returns an estimated number of messages that a consumer may be allowed
        to consume at once from the broker. This is used for services where
        bulk 'get message' calls are preferred to many individual 'get message'
        calls - like SQS.

        returns:
            An integer > 0
        """
        pcount = self.prefetch_count
        count = None
        if pcount:
            count = pcount - (len(self._not_yet_acked) - len(self._dirty))

        if count < 1:
            return 1

        return count

    def append(self, message, delivery_tag):
        """Append message to transactional state."""
        self._not_yet_acked[delivery_tag] = message

    def get(self, delivery_tag):
        return self._not_yet_acked[delivery_tag]

    def ack(self, delivery_tag):
        """Acknowledge message and remove from transactional state."""
        message = self._not_yet_acked[delivery_tag]
        self._qpid_session.acknowledge(message=message)

    def reject(self, delivery_tag, requeue=False):
        """Remove from transactional state and requeue message."""
        self._not_yet_acked.pop(delivery_tag)


class Message(base.Message):

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

    QoS = QoS

    Message = Message

    #: Default body encoding.
    #: NOTE: ``transport_options['body_encoding']`` will override this value.
    body_encoding = 'base64'

    #: Binary <-> ASCII codecs.
    codecs = {'base64': Base64()}

    #: counter used to generate delivery tags for this channel.
    _delivery_tags = count(1)

    def __init__(self, connection):
        self.connection = connection
        self._tag_to_queue = {}
        qpid_qmf_connection = QpidConnection.establish('localhost')
        qpid_publish_connection = QpidConnection.establish('localhost')
        self._qpid_session = qpid_publish_connection.session()
        self._broker = BrokerAgent(qpid_qmf_connection)
        self._qos = None
        self._consumers = set()
        self.closed = False

    def _get(self, queue):
        rx = self._qpid_session.receiver(queue)
        return rx.fetch(timeout=0)

    def _put(self, queue, message, exchange=None, **kwargs):
        if not exchange:
            exchange = ''
        address = "%s/%s" % (exchange, queue)
        sender = self._qpid_session.sender(address)
        qpid_message = QpidMessage(message)
        sender.send(qpid_message, sync=True)

    def _purge(self, queue):
        queue_to_purge = self._broker.getQueue(queue)
        message_count = queue_to_purge.values['msgDepth']
        if message_count > 0:
            queue_to_purge.purge(message_count)
        return message_count

    def _size(self, queue):
        queue_to_check = self._broker.getQueue(queue)
        msgDepth = queue_to_check.values['msgDepth']
        return msgDepth

    def _delete(self, queue, *args, **kwargs):
        self._purge(queue)
        self._broker.delQueue(queue)

    @ProtonExceptionHandler('object already exists')
    def _new_queue(self, queue, **kwargs):
        self._broker.addQueue(queue)

    def _has_queue(self, queue, **kwargs):
        #TODO: implement me
        raise NotImplementedError

    def _poll(self, cycle, timeout=None):
        #TODO: implement me
        raise NotImplementedError('_poll Not Implemented')

    def queue_declare(self, queue=None, passive=False, **kwargs):
        """Declare queue."""
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

    @ProtonExceptionHandler('object already exists')
    def exchange_declare(self, *args, **kwargs):
        e_type = kwargs['type']
        e_name = kwargs['exchange']
        e_durable = kwargs.get('durable', False)
        options = {'durable': e_durable}
        self._broker.addExchange(e_type, e_name, options)

    def exchange_delete(self, exchange_name, **kwargs):
        self._broker.delExchange(exchange_name)

    def after_reply_message_received(self, *args, **kwargs):
        raise NotImplementedError('after_reply_message_received Not Implemented')

    def queue_bind(self, *args, **kwargs):
        queue = kwargs['queue']
        exchange = kwargs['exchange']
        key = kwargs['routing_key']
        self._broker.bind(exchange, queue, key)

    def queue_unbind(self, *args, **kwargs):
        queue = kwargs['queue']
        exchange = kwargs['exchange']
        key = kwargs['routing_key']
        self._broker.unbind(exchange, queue, key)

    def queue_purge(self, queue, **kwargs):
        """Remove all ready messages from queue."""
        return self._purge(queue)

    def basic_get(self, queue, no_ack=False, **kwargs):
        """Get message by direct access (synchronous)."""
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
        """Acknowledge message."""
        self.qos.ack(delivery_tag)

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
        self.connection.fd_shim.signaling_queue.put(['sub', queue])

    def basic_cancel(self, consumer_tag):
        """Cancel consumer by consumer tag."""
        if consumer_tag in self._consumers:
            self._consumers.remove(consumer_tag)
            queue = self._tag_to_queue.pop(consumer_tag, None)
            self.connection.fd_shim.signaling_queue.put(['kill', queue])
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

    def __init__(self, receiver, message_queue, *args, **kwargs):
        self.is_killed = False
        self._receiver = receiver
        self._message_queue = message_queue
        super(FDShimThread, self).__init__(*args, **kwargs)

    def run(self):
        while not self.is_killed:
            try:
                response = self._receiver.fetch(timeout=10)
            except QpidEmpty:
                pass
            else:
                self._message_queue.put(response)
        #TODO: handle _message_queue and _receiver cleanup here

    def kill(self):
        self.is_killed = True


class FDShim(object):
    """
    This is where the magic happens.
    """
    def __init__(self, connection, queue_from_fdshim):
        self.queue_from_fdshim = queue_from_fdshim
        self.connection = connection
        self.r, self._w = os.pipe()
        self._qpid_session = QpidConnection.establish('localhost').session()
        self.signaling_queue = Queue.Queue()
        self.message_queue = Queue.Queue()
        self._threads = {}
        self._is_killed = False

    def recv(self):
        while True:
            try:
                action, address = self.signaling_queue.get(False)
            except Queue.Empty:
                pass
            else:
                #signaling_queue event ready
                if action is 'sub':
                    if address not in self._threads:
                        receiver = self._qpid_session.receiver(address)
                        my_thread = FDShimThread(receiver, self.message_queue)
                        self._threads[address] = my_thread
                        my_thread.start()
                elif action is 'kill':
                    self._threads[address].kill()
                    del self._threads[address]
            try:
                child_message = self.message_queue.get(False)
            except Queue.Empty:
                pass
            else:
                #message from child ready
                return child_message

    def kill(self):
        self.is_killed = True

    def listen(self):
        """
        Do a blocking read call similar to what qpid.messaging does, and when
        something is finally received, shove it into the pipe.
        """
        while not self._is_killed:
            message = self.recv()
            self.queue_from_fdshim.put(message)
            os.write(self._w, 'ready')


class Transport(base.Transport):

    Channel = Channel

    default_port = DEFAULT_PORT
    polling_interval = None
    supports_ev = True
    __reader = None

    channel_errors = (
        virtual.Transport.channel_errors
    )

    driver_type = 'qpid'
    driver_name = 'qpid'

    def __init__(self, client, **kwargs):
        self.client = client
        self.channels = []
        self._avail_channels = []
        self._callbacks = {}
        self.fd_shim = None
        self.queue_from_fdshim = Queue.Queue()
        self.fd_shim = FDShim(self, self.queue_from_fdshim)
        fdshim_thread = threading.Thread(target=self.fd_shim.listen)
        fdshim_thread.daemon = True
        fdshim_thread.start()

    def register_with_event_loop(self, connection, loop):
        loop.add_reader(self.fd_shim.r, self.on_readable, connection, loop)

    def establish_connection(self):
        # creates channel to verify connection.
        # this channel is then used as the next requested channel.
        # (returned by ``create_channel``).
        self._avail_channels.append(self.create_channel(self))
        return self     # for drain events

    def close_connection(self, connection):
        for l in self._avail_channels, self.channels:
            while l:
                try:
                    channel = l.pop()
                except (IndexError, KeyError):  # pragma: no cover
                    pass
                else:
                    channel.close()
        self.fd_shim.kill()

    def drain_events(self, connection, timeout=0, **kwargs):
        start_time = clock()
        elapsed_time = 0
        while elapsed_time < timeout:
            try:
                message = self.queue_from_fdshim.get(block=True, timeout=timeout)
            except Queue.Empty:
                raise socket.timeout()
            else:
                queue = message.subject
                self._callbacks[queue](message)
            elapsed_time = clock() - start_time
        raise socket.timeout()

    def create_channel(self, connection):
        try:
            return self._avail_channels.pop()
        except IndexError:
            channel = self.Channel(connection)
            self.channels.append(channel)
            return channel

    def close_channel(self, channel):
        try:
            self.channels.remove(channel)
        except ValueError:
            pass
        finally:
            channel.connection = None

    def on_readable(self, connection, loop):
        self.drain_events(connection)