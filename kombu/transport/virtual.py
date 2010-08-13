import atexit
import pickle
import sys
import tempfile

from time import sleep
from itertools import count, cycle
from Queue import Empty as QueueEmpty

from kombu.transport import base
from kombu.utils import OrderedDict


class FairCycle(object):
    """Consume from a set of resources, where each resource gets
    an equal chance to be consumed from."""

    def __init__(self, fun, resources, predicate=QueueEmpty):
        self.fun = fun
        self.resources = resources
        self.predicate = predicate

        # an infinite cycle through all the queues.
        self.cycle = cycle(self.resources)

        # A set of all the names, so we can match when we've
        # tried all of them.
        self.all = frozenset(self.resources)

    def get(self):
        # What we've already tried.
        tried = set()

        while True:
            # Get the next resource in the cycle,
            # and try to get an item off it.
            resource = self.cycle.next()
            try:
                return self.fun(resource), resource
            except self.predicate:
                tried.add(resource)
                if tried == self.all:
                    raise


class QualityOfService(object):

    def __init__(self, channel, prefetch_count=None, interval=None,
            do_restore=True):
        self.channel = channel
        self.prefetch_count = prefetch_count
        self.interval = interval
        self._delivered = OrderedDict()
        self.do_restore = do_restore
        self._restored_once = False
        if self.do_restore:
            atexit.register(self.restore_unacked_once)

    def can_consume(self):
        if not self.prefetch_count:
            return True
        return len(self._delivered) < self.prefetch_count

    def append(self, message, delivery_tag):
        self._delivered[delivery_tag] = message

    def ack(self, delivery_tag):
        self._delivered.pop(delivery_tag, None)

    def restore_unacked(self):
        for message in self._delivered.items():
            self.channel._restore(message)
        self._delivered.clear()

    def requeue(self, delivery_tag):
        try:
            message = self._delivered.pop(delivery_tag)
        except KeyError:
            pass
        self.channel._restore(message)

    def restore_unacked_once(self):
        if self.do_restore and not self._restored_once:
            if self._delivered:
                sys.stderr.write(
                    "Restoring unacknowledged messages: %s\n" % (
                    self._delivered))
            try:
                self.restore_unacked()
            except:
                pass
            if self._delivered:
                sys.stderr.write("UNABLE TO RESTORE %s MESSAGES\n" % (
                    len(self._delivered)))
                persist = tempfile.mktemp()
                sys.stderr.write(
                    "PERSISTING UNRESTORED MESSAGES TO FILE: %s\n" % persist)
                fh = open(persist, "w")
                try:
                    pickle.dump(self._delivered, fh, protocol=0)
                finally:
                    fh.flush()
                    fh.close()


class Message(base.Message):

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

        super(Message, self).__init__(channel, **kwargs)

    def reject(self):
        raise NotImplementedError(
            "This transport does not implement basic.reject")


_exchanges = {}
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
        raise NotImplementedError("Virtual channels must implement _get")

    def _put(self, queue, message):
        raise NotImplementedError("Virtual channels must implement _put")

    def _purge(self, queue):
        raise NotImplementedError("Virtual channels must implement _purge")

    def _size(self, queue):
        return 0

    def _delete(self, queue):
        self._purge(queue)

    def _new_queue(self, queue):
        pass

    def _lookup(self, exchange, routing_key, default="ae.undeliver"):
        try:
            return _exchanges[exchange]["table"][routing_key]
        except KeyError:
            self._new_queue(default)
            return default

    def _restore(self, message):
        delivery_info = message.delivery_info
        self._put(self._lookup(delivery_info["exchange"],
                               delivery_info["routing_key"]),
                  message)

    def _poll(self, queues):
        return FairCycle(self._get, queues, QueueEmpty).get()

    def drain_events(self, timeout=None):
        if self.qos_manager.can_consume():
            if hasattr(self, "_get_many"):
                return self._get_many(self._active_queues, timeout=timeout)
            return self._poll(self._active_queues)
        raise QueueEmpty()

    @property
    def _active_queues(self):
        return [_consumers[tag] for tag in self._consumers]

    def exchange_declare(self, exchange, type="direct", durable=False,
            auto_delete=False, arguments=None):
        if exchange not in _exchanges:
            _exchanges[exchange] = {"type": type,
                                    "durable": durable,
                                    "auto_delete": auto_delete,
                                    "arguments": arguments or {},
                                    "table": {}}

    def exchange_delete(self, exchange, if_unused=False):
        for rkey, queue in _exchanges[exchange]["table"].items():
            self._purge(queue)
        _exchanges.pop(exchange, None)

    def queue_declare(self, queue, **kwargs):
        self._new_queue(queue, **kwargs)
        return queue, self._size(queue), 0

    def queue_delete(self, queue, if_unusued=False, if_empty=False):
        if if_empty and self._size(queue):
            return
        self._delete(queue)

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

    def basic_recover(self, requeue=False):
        if requeue:
            return self.qos_manager.restore_unacked()
        raise NotImplementedError("Does not support recover(requeue=False)")

    def basic_reject(self, delivery_tag, requeue=False):
        if requeue:
            self.qos_manager.requeue(delivery_tag)

    def basic_consume(self, queue, no_ack, callback, consumer_tag,
                         **kwargs):
        _consumers[consumer_tag] = queue
        _callbacks[queue] = callback
        self._consumers.add(consumer_tag)

    def basic_publish(self, message, exchange, routing_key, **kwargs):
        message["properties"]["delivery_info"]["exchange"] = exchange
        message["properties"]["delivery_info"]["routing_key"] = routing_key
        message["properties"]["delivery_tag"] = self._next_delivery_tag()
        self._put(self._lookup(exchange, routing_key), message)

    def basic_cancel(self, consumer_tag):
        queue = _consumers.pop(consumer_tag, None)
        self._consumers.remove(consumer_tag)
        _callbacks.pop(queue, None)

    def message_to_python(self, raw_message):
        message = self.Message(self, payload=raw_message)
        self.qos_manager.append(message, message.delivery_tag)
        return message

    def prepare_message(self, message_data, priority=None,
            content_type=None, content_encoding=None, headers=None,
            properties=None):
        properties = properties or {}
        properties.setdefault("delivery_info", {})
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
        map(self.basic_cancel, list(self._consumers))
        self.connection.close_channel(self)


class Transport(base.Transport):
    Channel = Channel
    Cycle = FairCycle

    interval = 1
    default_port = None

    def __init__(self, client, **kwargs):
        self.client = client
        self._channels = set()

    def create_channel(self, connection):
        channel = self.Channel(connection)
        self._channels.add(channel)
        return channel

    def close_channel(self, channel):
        self._channels.discard(channel)

    def establish_connection(self):
        return self # for drain events

    def close_connection(self, connection):
        while self._channels:
            try:
                channel = self._channels.pop()
            except KeyError:
                pass
            else:
                channel.close()

    def _drain_channel(self, channel):
        return channel.drain_events(timeout=self.interval)

    def drain_events(self, connection, timeout=None):
        cycle = self.Cycle(self._drain_channel, self._channels, QueueEmpty)
        while True:
            try:
                item, channel = cycle.get()
                break
            except QueueEmpty:
                sleep(self.interval)

        message, queue = item

        if not queue or queue not in _callbacks:
            raise KeyError(
                "Received message for queue '%s' without consumers: %s" % (
                    queue, message))

        _callbacks[queue](message)
