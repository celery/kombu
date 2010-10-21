"""

Creating the applications Mailbox
=================================

::

    >>> mailbox = pidbox.Mailbox("celerybeat", type="direct")

    >>> @mailbox.handler
    >>> def reload_schedule(state, **kwargs):
    ...     state["beat"].reload_schedule()

    >>> @mailbox.handler
    >>> def connection_info(state, **kwargs):
    ...     return {"connection": state["connection"].info()}

Example Node
============

::
    >>> connection = kombu.BrokerConnection()
    >>> state = {"beat": beat,
                 "connection": connection}
    >>> consumer = mailbox(connection).Node(hostname).listen()
    >>> try:
    ...     while True:
    ...         connection.drain_events(timeout=1)
    ... finally:
    ...     consumer.cancel()

Example Client
==============

::
    >>> mailbox.cast("reload_schedule")   # cast is async.
    >>> info = celerybeat.call("connection_info", timeout=1)

"""

import socket
from copy import copy
from itertools import count

from kombu.entity import Exchange, Queue
from kombu.messaging import Consumer, Producer
from kombu.utils import gen_unique_id, kwdict


class Node(object):

    def __init__(self, hostname, state=None, channel=None, handlers=None,
            mailbox=None):
        self.channel = channel
        self.mailbox = mailbox
        self.hostname = hostname
        self.state = state
        if handlers is None:
            handlers = {}
        self.handlers = handlers

    def Consumer(self, channel=None, **options):
        options.setdefault("no_ack", True)
        return Consumer(channel or self.channel,
                        [self.mailbox.get_queue(self.hostname)],
                        **options)

    def listen(self, channel=None, callback=None):
        callback = callback or self.handle_message
        consumer = self.Consumer(channel=channel,
                                 callbacks=[callback or self.handle_message])
        consumer.consume()
        return consumer

    def reply(self, data, exchange, routing_key, **kwargs):
        self.mailbox._publish_reply(data, exchange, routing_key,
                                    channel=self.channel)

    def dispatch_from_message(self, message):
        message = dict(message)
        method = message["method"]
        destination = message.get("destination")
        reply_to = message.get("reply_to")
        arguments = message.get("arguments")
        if not destination or self.hostname in destination:
            return self.dispatch(method, arguments, reply_to)

    def dispatch(self, method, arguments=None, reply_to=None):
        arguments = arguments or {}
        handle = reply_to and self.handle_call or self.handle_cast
        try:
            reply = handle(method, kwdict(arguments))
        except SystemExit:
            raise
        except Exception, exc:
            reply = {"error": repr(exc)}

        if reply_to:
            self.reply({self.hostname: reply}, **reply_to)
        return reply

    def handle_call(self, method, arguments):
        return self.handle(method, arguments)

    def handle_cast(self, method, arguments):
        return self.handle(method, arguments)

    def handler(self, fun):
        self.handlers[fun.__name__] = fun
        return fun

    def handle(self, method, arguments={}):
        return self.handlers[method](self.state, **arguments)

    def handle_message(self, message_data, message):
        self.dispatch_from_message(message_data)


class Mailbox(object):
    node_cls = Node
    exchange_fmt = "%s.pidbox"
    reply_exchange_fmt = "reply.%s.pidbox"

    def __init__(self, namespace, type="direct", connection=None):
        self.namespace = namespace
        self.connection = connection
        self.type = type
        self.exchange = self._get_exchange(self.namespace, self.type)
        self.reply_exchange = self._get_reply_exchange(self.namespace)

    def __call__(self, connection):
        bound = copy(self)
        bound.connection = connection
        return bound

    def Node(self, hostname=None, state=None, channel=None, handlers=None):
        hostname = hostname or socket.gethostname()
        return self.node_cls(hostname, state, channel, handlers, mailbox=self)

    def call(self, destination, command, kwargs={}, timeout=None,
            callback=None, channel=None):
        return self._broadcast(command, kwargs, destination,
                               reply=True, timeout=timeout,
                               callback=callback,
                               channel=channel)

    def cast(self, destination, command, kwargs={}):
        return self._broadcast(command, kwargs, destination, reply=False)

    def abcast(self, command, kwargs={}):
        return self._broadcast(command, kwargs, reply=False)

    def multi_call(self, command, kwargs={}, timeout=1,
            limit=None, callback=None, channel=None):
        return self._broadcast(command, kwargs, reply=True,
                               timeout=timeout, limit=limit,
                               callback=callback,
                               channel=channel)

    def get_reply_queue(self, ticket):
        return Queue("%s.%s" % (ticket, self.reply_exchange.name),
                     exchange=self.reply_exchange,
                     routing_key=ticket,
                     durable=False,
                     auto_delete=True)

    def get_queue(self, hostname):
        return Queue("%s.%s.pidbox" % (hostname, self.namespace),
                     exchange=self.exchange)


    def _publish_reply(self, reply, exchange, routing_key, channel=None):
        chan = channel or self.connection.channel()
        try:
            exchange = Exchange(exchange, exchange_type="direct",
                                          delivery_mode="transient",
                                          durable=False,
                                          auto_delete=True)
            producer = Producer(chan, exchange=exchange)
            producer.publish(reply, routing_key=routing_key)
        finally:
            channel or chan.close()

    def _publish(self, type, arguments, destination=None, reply_ticket=None,
            channel=None):
        message = {"method": type,
                   "arguments": arguments,
                   "destination": destination}
        if reply_ticket:
            message["reply_to"] = {"exchange": self.reply_exchange.name,
                                   "routing_key": reply_ticket}
        chan = channel or self.connection.channel()
        producer = Producer(chan, exchange=self.exchange)
        try:
            producer.publish(message)
        finally:
            channel or chan.close()

    def _broadcast(self, command, arguments=None, destination=None,
            reply=False, timeout=1, limit=None, callback=None, channel=None):
        arguments = arguments or {}
        reply_ticket = reply and gen_unique_id() or None

        if destination is not None and \
                not isinstance(destination, (list, tuple)):
            raise ValueError("destination must be a list/tuple not %s" % (
                    type(destination)))

        # Set reply limit to number of destinations (if specificed)
        if limit is None and destination:
            limit = destination and len(destination) or None

        chan = channel or self.connection.channel()
        try:
            if reply_ticket:
                self.get_reply_queue(reply_ticket)(chan).declare()

            self._publish(command, arguments, destination=destination,
                                              reply_ticket=reply_ticket,
                                              channel=chan)

            if reply_ticket:
                return self._collect(reply_ticket, limit=limit,
                                                   timeout=timeout,
                                                   callback=callback,
                                                   channel=chan)
        finally:
            channel or chan.close()

    def _collect(self, ticket, limit=None, timeout=1,
            callback=None, channel=None):
        chan = channel or self.connection.channel()
        queue = self.get_reply_queue(ticket)
        consumer = Consumer(channel, [queue], no_ack=True)
        responses = []

        def on_message(message_data, message):
            if callback:
                callback(message_data)
            responses.append(message_data)

        try:
            consumer.register_callback(on_message)
            consumer.consume()
            for i in limit and range(limit) or count():
                try:
                    self.connection.drain_events(timeout=timeout)
                except socket.timeout:
                    break
            return responses
        finally:
            channel or chan.close()

    def _get_exchange(self, namespace, type):
        return Exchange(self.exchange_fmt % namespace,
                        type=type,
                        durable=False,
                        auto_delete=True,
                        delivery_mode="transient")

    def _get_reply_exchange(self, namespace):
        return Exchange(self.reply_exchange_fmt % namespace,
                        type="direct",
                        durable=False,
                        auto_delete=True,
                        delivery_mode="transient")
