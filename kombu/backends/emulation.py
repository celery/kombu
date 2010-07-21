from kombu.backends.base import BaseBackend, BaseMessage
from anyjson import deserialize, serialize
from itertools import count
from collections import OrderedDict
import sys
import time
import atexit

from Queue import Empty as QueueEmpty
from itertools import cycle


class QueueSet(object):
    """A set of queues that operates as one."""

    def __init__(self, backend, queues):
        self.backend = backend
        self.queues = queues

        # an infinite cycle through all the queues.
        self.cycle = cycle(self.queues)

        # A set of all the queue names, so we can match when we've
        # tried all of them.
        self.all = frozenset(self.queues)

    def get(self):
        """Get the next message avaiable in the queue.

        :returns: The message and the name of the queue it came from as
            a tuple.
        :raises Queue.Empty: If there are no more items in any of the queues.

        """

        # A set of queues we've already tried.
        tried = set()

        while True:
            # Get the next queue in the cycle, and try to get an item off it.
            queue = self.cycle.next()
            try:
                item = self.backend._get(queue)
            except QueueEmpty:
                # raises Empty when we've tried all of them.
                tried.add(queue)
                if tried == self.all:
                    raise
            else:
                return item, queue

    def __repr__(self):
        return "<QueueSet: %s>" % repr(self.queue_names)


class QualityOfService(object):

    def __init__(self, resource, prefetch_count=None, interval=None,
            do_restore=True):
        self.resource = resource
        self.prefetch_count = prefetch_count
        self.interval = interval
        self._delivered = OrderedDict()
        self.do_restore = do_restore
        self._restored_once = False
        atexit.register(self.restore_unacked_once)

    def can_consume(self):
        return len(self._delivered) > self.prefetch_count

    def append(self, message, queue_name, delivery_tag):
        self._delivered[delivery_tag] = message, queue_name

    def ack(self, delivery_tag):
        self._delivered.pop(delivery_tag, None)

    def restore_unacked(self):
        for message, queue_name in self._delivered.items():
            self.resource._put(queue_name, message)
        self._delivered = SortedDict()

    def requeue(self, delivery_tag):
        try:
            message, queue_name = self._delivered.pop(delivery_tag)
        except KeyError:
            pass
        self.resource.put(queue_name, message)

    def restore_unacked_once(self):
        if self.do_restore:
            if not self._restored_once:
                if self._delivered:
                    sys.stderr.write(
                        "Restoring unacknowledged messages: %s\n" % (
                            self._delivered))
                self.restore_unacked()
                if self._delivered:
                    sys.stderr.write("UNRESTORED MESSAGES: %s\n" % (
                        self._delivered))


class Message(BaseMessage):

    def __init__(self, channel, payload, **kwargs):
        properties = payload["properties"]
        kwargs["body"] = payload.get("body").encode("utf-8")
        kwargs["delivery_tag"] = properties["delivery_tag"]
        kwargs["content_type"] = payload.get("content-type")
        kwargs["content_encoding"] = payload.get("content-encoding")
        kwargs["priority"] = payload.get("priority")
        kwargs["headers"] = payload.get("headers")
        kwargs["properties"] = properties
        kwargs["delivery_info"] = properties.get("delivery_info")
        self.destination = payload.get("destination")

        super(Message, self).__init__(channel, **kwargs)

    def reject(self):
        raise NotImplementedError(
            "This backend does not implement basic.reject")


_exchanges = {}
_queues = {}
_consumers = {}
_callbacks = {}


class Channel(object):
    Message = Message

    interval = 1
    do_restore = True

    _next_delivery_tag = count(1).next
    _prefetch_count = None

    def __init__(self, connection, **kwargs):
        self.connection = connection
        self._consumers = set()
        self._qos_manager = None

    def _get(self, queue):
        raise NotImplementedError("Emulations must implement _get")

    def _put(self, queue, message):
        raise NotImplementedError("Emulations must implement _put")

    def _purge(self, queue):
        raise NotImplementedError("Emulations must implement _purge")

    def _new_queue(self, queue):
        raise NotImplementedError("Emulations must implement _new_queue")

    def exchange_declare(self, exchange, type="direct", durable=False,
            auto_delete=False, arguments=None):
        if exchange not in _exchanges:
            _exchanges[exchange] = {"type": type,
                                    "durable": durable,
                                    "auto_delete": auto_delete,
                                    "arguments": arguments or {},
                                    "table": {}}

    def queue_declare(self, queue, **kwargs):
        if queue not in _queues:
            _queues[queue] = self._new_queue(queue, **kwargs)

    def queue_bind(self, queue, exchange, routing_key, arguments=None):
        table = _exchanges[exchange].setdefault("table", {})
        table[routing_key] = queue

    def queue_purge(self, queue, **kwargs):
        return self._purge(queue, **kwargs)

    def flow(self, active=True):
        pass

    def basic_qos(self, prefetch_size, prefetch_count, apply_global=False):
        self._prefetch_count = prefetch_count

    def basic_get(self, queue, **kwargs):
        try:
            return self._get(queue)
        except QueueEmpty:
            pass

    def basic_ack(self, delivery_tag):
        self.qos_manager.ack(delivery_tag)

    def basic_reject(self, delivery_tag, requeue=False):
        if requeue:
            self.qos_manager.requeue(delivery_tag)

    def basic_consume(self, queue, no_ack, callback, consumer_tag,
                         **kwargs):
        _consumers[consumer_tag] = queue
        _callbacks[queue] = callback
        self._consumers.add(consumer_tag)

    def basic_publish(self, message, exchange, routing_key, **kwargs):
        message["destination"] = exchange
        message["properties"]["delivery_tag"] = self._next_delivery_tag()
        table = _exchanges[exchange]["table"]
        if routing_key in table:
            self._put(table[routing_key], message)

    def basic_cancel(self, consumer_tag):
        queue = _consumers.pop(consumer_tag, None)
        self._consumers.remove(consumer_tag)
        _callbacks.pop(queue, None)

    def message_to_python(self, raw_message):
        message = self.Message(self, payload=raw_message)
        self.qos_manager.append(message, message.destination,
                                message.delivery_tag)
        return message

    def prepare_message(self, message_data, priority=None,
            content_type=None, content_encoding=None, headers=None,
            properties=None):
        return {"body": message_data,
                "priority": priority or 0,
                "content-encoding": content_encoding,
                "content-type": content_type,
                "headers": headers or {},
                "properties": properties or {}}

    @property
    def qos_manager(self):
        if self._qos_manager is None:
            self._qos_manager = QualityOfService(self,
                                                 do_restore=self.do_restore)

        # Update prefetch count / interval
        self._qos_manager.prefetch_count = self._prefetch_count
        self._qos_manager.interval = self.interval

        return self._qos_manager

    def close(self):
        map(self.basic_cancel, self._consumers)


class EmulationBase(BaseBackend):
    Channel = Channel
    QueueSet = QueueSet

    default_port = None

    def __init__(self, connection, **kwargs):
        self.connection = connection

    def create_channel(self, connection):
        return self.Channel(connection)

    def establish_connection(self):
        return self # for drain events

    def close_connection(self, connection):
        pass

    def _poll(self, resource):
        while True:
            if self.qos_manager.can_consume():
                try:
                    return resource.get()
                except QueueEmpty:
                    pass
            time.sleep(self.interval)

    def drain_events(self, timeout=None):
        queueset = self.QueueSet(self._consumers.values())
        payload, queue = self._poll(queueset)

        if not queue or queue not in _callbacks:
            return

        _callbacks[queue](payload)
