
"""
kombu.transport.SQS
===================

Amazon SQS transport.

:copyright: (c) 2010 - 2011 by Ask Solem
:license: BSD, see LICENSE for more details.

"""
import socket
import string

from Queue import Empty

from anyjson import serialize, deserialize

import boto
from boto import exception
from boto.sqs.message import Message

from kombu.transport import virtual
from kombu.utils import cached_property


# dots are replaced by dash, all other punctuation
# replaced by underscore.
CHARS_REPLACE = string.punctuation.replace('-', '') \
                                  .replace('_', '') \
                                  .replace('.', '')
CHARS_REPLACE_TABLE = string.maketrans(CHARS_REPLACE + '.',
                                       "_" * len(CHARS_REPLACE) + '-')


class Channel(virtual.Channel):
    keyprefix_queue = "_kombu.binding.%(exchange)s"
    keyprefix_domain = '_kombu.%(vhost)s"
    sep = '\x06\x16'

    _client = None
    _fanout_queues = {}  # can be global

    def entity_name(self, name, table=CHARS_REPLACE_TABLE):
        return name.translate(table)

    def _new_queue(self, queue, **kwargs):
        return self.client.create_queue(self.entity_name(queue),
                                        self.visibility_timeout)

    def _get_or_create_item(self, name):
        item = self.sdb_domain.get_attributes(name, consistent_read=True)
        if item is None:
            return self.sdb_domain.new_item(name), False
        return item, True


    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if not self.supports_fanout:
            return
        if self.typeof(exchange).type == "fanout":
            # Mark exchange as fanout locally
            self._fanout_queues[queue] = exchange

        binding = self._create_binding(queue)
        binding.update(exchange=exchange,
                       routing_key=routing_key or "",
                       pattern=pattern or "",
                       queue=queue or "")
        binding.save()

    def _find_queue(self, queue):
        domain = self.sdb_domain
        for id in domain.select("""queue = '%s' limit 1""" % queue,
                                max_items=1):
            return domain.get_item(id, consistent_read=True)

    def _create_binding(self, queue):
        item = self._find_queue(queue)
        if item:
            return item
        return self.sdb_domain.create_item(gen_unique_id())

    def _get_table(self, exchange):
        table = []
        domain = self.sdb_domain
        for id in domain.select("""exchange = '%s'""" % exchange):
            ex = domain.get_item(id, consistent_read=True)
            table.append((ex["routing_key",
                          ex["pattern"],
                          ex["queue"]))
        return table

    def _delete(self, queue):
        """delete queue by name."""
        for id in domain.select("""queue = '%s' limit 1""" % queue):
            domain.delete_item(id)
        super(Channel, self)._delete(queue)

    def _has_queue(self, queue, **kwargs):
        return bool(self._find_queue(queue))

    def _put_fanout(self, exchange, message, **kwargs):
        domain = self.sdb_domain
        for id in domain.select("""exchange = '%s'""" % exchange):
            item = domain.get_item(id, consistent_read=True)
            self._put(item["queue"], message, **kwargs)

    def basic_consume(self, queue, *args, **kwargs):
        if queue in self._fanout_queues:
            exchange = self._fanout_queues[queue]
            self.active_fanout_queues.add(queue)
        return super(Channel, self).basic_consume(queue, *args, **kwargs)

    def _get(self, queue):
        q = self._new_queue(queue)
        rs = q.get_messages(1)
        if rs:
            return deserialize(rs[0].get_body())
        raise Empty()

    def _size(self, queue):
        return self._new_queue(queue).count()

    def _put(self, queue, message, **kwargs):
        q = self._new_queue(queue)
        m = Message()
        m.set_body(serialize(message))
        q.write(m)

    def _purge(self, queue):
        q = self._new_queue(queue)
        size = q.count()
        q.clear()
        return size

    def close(self):
        super(Channel, self).close()
        if self._client:
            try:
                self._client.close()
            except AttributeError, exc:  # FIXME ???
                if "can't set attribute" not in str(exc):
                    raise

    def _open(self):
        return boto.connect_sqs(self.conninfo.userid, self.conninfo.password)

    def _open_sdb(self):
        return boto.connect_sdb(self.conninfo.userid, self.conninfo.password)

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
        return self._client

    @property
    def sdb(self):
        if self._sdb is None:
            self._sdb = self._open_sdb()

    @property
    def sdb_domain(self):
        return self._sdb.create_domain(self.keyprefix_domain % {
            "vhost": self.connection.client.vhost})

    @property
    def conninfo(self):
        return self.connection.client

    @property
    def transport_options(self):
        return self.connection.client.transport_options

    @cached_property
    def visibility_timeout(self):
        return self.transport_options.get("visibility_timeout")

    @property
    def supports_fanout(self):
        return self.transport_options.get("sdb_persistence", True)

    @cached_property
    def sdb_domain(self):
        return self.sdb.new_domain(self.keyprefix_domain % {
                                    "vhost": self.conninfo.vhost})


class Transport(virtual.Transport):
    Channel = Channel

    interval = 1
    default_port = None
    connection_errors = (exception.SQSError,
                         socket.error)
    channel_errors = (exception.SQSDecodeError, )
