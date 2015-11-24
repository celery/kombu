from __future__ import absolute_import, unicode_literals

from amqp import ensure_promise, promise

from kombu.async import get_event_loop
from kombu.async.tornado import TornadoHub
from kombu.utils import cached_property
from kombu.utils.json import dumps, loads

from . import virtual

try:
    import nsq
except ImportError:  # pragma: no cover
    nsq = None  # noqa


class Writer(nsq.Writer):

    def __init__(self, hosts, on_connected=None, **kwargs):
        self.on_connected = ensure_promise(on_connected)
        super(Writer, self).__init__(hosts, **kwargs)

    def _on_connection_ready(self, conn, **kwargs):
        if conn.id not in self.conns:
            print('ON CONNECTED!')
            self.on_connected(conn, **kwargs)
        super(Writer, self)._on_connection_ready(conn, **kwargs)


class Channel(virtual.Channel):

    _noack_queues = set()

    def __init__(self, *args, **kwargs):
        if nsq is None:
            raise ImportError('pynsq is not installed')
        super(Channel, self).__init__(*args, **kwargs)
        self.hub = get_event_loop()
        if self.hub is None:
            self.hub = TornadoHub()
        self._max_in_flight = 1000
        self._readers = {}
        self._writer = None
        self._on_writer_connected = promise()
        self._no_ack_queues = set()

    def _put(self, queue, message, **kwargs):
        p = promise(args=(queue, message), callback=self._message_sent)
        self._on_writer_connected.then(
            promise(self._send_message, (p, self.writer, queue, message)),
        )
        return self.hub.run_until_ready(p)

    def _message_sent(self, *args, **kwargs):
        print('message sent: %r %r' % (args, kwargs))

    def create_writer(self):
        conninfo = self.conninfo
        return Writer(
            ['localhost:4150'],
            on_connected=self._on_writer_connected,
            io_loop=self.hub,
        )

    def _send_message(self, p, writer, queue, message, conn):
        writer.pub(queue, dumps(message), callback=p)
        return p


    def create_reader(self, queue):
        return nsq.Reader(
            topic=queue, channel=queue,
            max_in_flight=self._max_in_flight,
            nsqd_tcp_addresses=['localhost:4150'],
            message_handler=promise(self.on_message, (queue,)),
            #message_handler=self.on_message,
            io_loop=self.hub,
            lookupd_poll_interval=1,
        )

    def basic_qos(self, prefetch_size=0, prefetch_count=0,
            apply_global=False):
        self._max_in_flight = prefetch_count
        super(Channel, self).basic_qos(
            prefetch_size, prefetch_count, apply_global)
        for reader in self._readers.values():
            reader.set_max_in_flight(self._max_in_flight)

    def basic_consume(self, queue, no_ack, *args, **kwargs):
        if no_ack:
            self._no_ack_queues.add(queue)
        consumer_tag = super(Channel, self).basic_consume(
            queue, no_ack, *args, **kwargs)
        reader = self._readers[queue] = self.create_reader(queue)
        return consumer_tag

    def on_message(self, queue, message):
        if message:
            callbacks = self.connection._callbacks
            payload = loads(message.body)
            if queue in self._noack_queues:
                message.finish()
            else:
                payload['properties']['delivery_info'].update({
                    'nsq_message': message,
                })
                message.enable_async()
            payload['properties']['delivery_tag'] = message.id
            callbacks[queue](payload)
        return True

    def _get_message_by_tag(self, delivery_tag):
        delivery_info = self.qos.get(delivery_tag).delivery_info
        try:
            return delivery_info['nsq_message']
        except KeyError:
            pass

    def basic_ack(self, delivery_tag):
        message = self._get_message_by_tag(delivery_tag)
        if message:
            message.finish()
        super(Channel, self).basic_ack(delivery_tag)

    def basic_reject(self, delivery_tag, requeue=False):
        message = self._get_message_by_tag(delivery_tag)
        message.requeue() if requeue else message.finish()
        super(Channel, self).basic_reject(delivery_tag, requeue)

    @property
    def writer(self):
        if self._writer is None:
            self._writer = self.create_writer()
        return self._writer

    @property
    def conninfo(self):
        return self.connection.client

class Transport(virtual.Transport):
    Channel = Channel

    using_lookupd = True
    default_port = None
    connection_errors = (
        virtual.Transport.connection_errors +
        ()
    )
    channel_errors = (
        virtual.Transport.channel_errors,
        ()
    )
    driver_type = 'nsq'
    driver_name = 'nsq'

    implements = virtual.Transport.implements.extend(
        async=True,
        exchange_type={'direct'},
    )

    requires_hub = TornadoHub

    @cached_property
    def hub(self):
        return TornadoHub()

    def driver_version(self):
        return nsq.__version__


class TCPTransport(virtual.Transport):
    using_lookupd = False
