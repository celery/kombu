"""
kombu.pidbox
===============

Generic process mailbox.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import socket

from copy import copy
from itertools import count

from .entity import Exchange, Queue
from .messaging import Consumer, Producer
from .utils import kwdict, uuid

REPLY_QUEUE_EXPIRES = 10

__all__ = ['Node', 'Mailbox']


class Node(object):

    #: hostname of the node.
    hostname = None

    #: the :class:`Mailbox` this is a node for.
    mailbox = None

    #: map of method name/handlers.
    handlers = None

    #: current context (passed on to handlers)
    state = None

    #: current channel.
    channel = None

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
        options.setdefault('no_ack', True)
        return Consumer(channel or self.channel,
                        [self.mailbox.get_queue(self.hostname)],
                        **options)

    def handler(self, fun):
        self.handlers[fun.__name__] = fun
        return fun

    def listen(self, channel=None, callback=None):
        callback = callback or self.handle_message
        consumer = self.Consumer(channel=channel,
                                 callbacks=[callback or self.handle_message])
        consumer.consume()
        return consumer

    def dispatch(self, method, arguments=None, reply_to=None):
        arguments = arguments or {}
        handle = reply_to and self.handle_call or self.handle_cast
        try:
            reply = handle(method, kwdict(arguments))
        except SystemExit:
            raise
        except Exception, exc:
            reply = {'error': repr(exc)}

        if reply_to:
            self.reply({self.hostname: reply},
                       exchange=reply_to['exchange'],
                       routing_key=reply_to['routing_key'])
        return reply

    def handle(self, method, arguments={}):
        return self.handlers[method](self.state, **arguments)

    def handle_call(self, method, arguments):
        return self.handle(method, arguments)

    def handle_cast(self, method, arguments):
        return self.handle(method, arguments)

    def handle_message(self, body, message=None):
        method = body['method']
        destination = body.get('destination')
        reply_to = body.get('reply_to')
        arguments = body.get('arguments')
        if not destination or self.hostname in destination:
            return self.dispatch(method, arguments, reply_to)
    dispatch_from_message = handle_message

    def reply(self, data, exchange, routing_key, **kwargs):
        self.mailbox._publish_reply(data, exchange, routing_key,
                                    channel=self.channel)


class Mailbox(object):
    node_cls = Node
    exchange_fmt = '%s.pidbox'
    reply_exchange_fmt = 'reply.%s.pidbox'

    #: Name of application.
    namespace = None

    #: Connection (if bound).
    connection = None

    #: Exchange type (usually direct, or fanout for broadcast).
    type = 'direct'

    #: mailbox exchange (init by constructor).
    exchange = None

    #: exchange to send replies to.
    reply_exchange = None

    def __init__(self, namespace, type='direct', connection=None):
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
        return Queue('%s.%s' % (ticket, self.reply_exchange.name),
                     exchange=self.reply_exchange,
                     routing_key=ticket,
                     durable=False,
                     auto_delete=True,
                     queue_arguments={
                         'x-expires': int(REPLY_QUEUE_EXPIRES * 1000),
                     })

    def get_queue(self, hostname):
        return Queue('%s.%s.pidbox' % (hostname, self.namespace),
                     exchange=self.exchange,
                     durable=False,
                     auto_delete=True)

    def broadcast(self, command, arguments=None, destination=None,
            reply=False, timeout=1, channel=None):
        if destination is not None and \
                not isinstance(destination, (list, tuple)):
            raise ValueError('destination must be a list/tuple not %s' % (
                    type(destination)))

        arguments = arguments or {}
        reply_ticket = reply and uuid() or None
        chan = channel or self.connection.default_channel

        if reply_ticket:
            self.get_reply_queue(reply_ticket)(chan).declare()

        self._publish(command, arguments, destination=destination,
                                          reply_ticket=reply_ticket,
                                          channel=chan)

        return reply_ticket

    def collect(self, tickets, limit=None, timeout=1,
            callback=None, channel=None):
        queues = map(self.get_reply_queue, tickets)
        consumer = Consumer(channel, queues, no_ack=True)
        responses = []

        def on_message(body, message):
            if callback:
                callback(body)
            responses.append(body)

        consumer.register_callback(on_message)
        with consumer:
            for i in limit and range(limit) or count():
                try:
                    self.connection.drain_events(timeout=timeout)
                except socket.timeout:
                    break
            return responses

    def cleanup(self, tickets, channel=None):
        chan = channel or self.connection.default_channel
        for queue in map(self.get_reply_queue, tickets):
            chan.after_reply_message_received(queue.name)

    def _publish_reply(self, reply, exchange, routing_key, channel=None):
        chan = channel or self.connection.default_channel
        exchange = Exchange(exchange, exchange_type='direct',
                                      delivery_mode='transient',
                                      durable=False)
        producer = Producer(chan, exchange=exchange,
                                  auto_declare=True)
        producer.publish(reply, routing_key=routing_key)

    def _publish(self, type, arguments, destination=None, reply_ticket=None,
            channel=None):
        message = {'method': type,
                   'arguments': arguments,
                   'destination': destination}
        if reply_ticket:
            message['reply_to'] = {'exchange': self.reply_exchange.name,
                                   'routing_key': reply_ticket}
        chan = channel or self.connection.default_channel
        producer = Producer(chan, exchange=self.exchange)
        producer.publish(message)

    def _broadcast(self, command, arguments=None, destination=None,
            reply=False, timeout=1, limit=None, callback=None, channel=None):
        reply_ticket = self.broadcast(command,
                arguments=arguments, destination=destination,
                reply=reply, channel=channel)

        # Set reply limit to number of destinations (if specified)
        if limit is None and destination:
            limit = destination and len(destination) or None

        if reply_ticket:
            try:
                return self.collect([reply_ticket], limit=limit,
                                                    timeout=timeout,
                                                    callback=callback,
                                                    channel=channel)
            finally:
                self.cleanup([reply_ticket], channel)

    def _get_exchange(self, namespace, type):
        return Exchange(self.exchange_fmt % namespace,
                        type=type,
                        durable=False,
                        delivery_mode='transient')

    def _get_reply_exchange(self, namespace):
        return Exchange(self.reply_exchange_fmt % namespace,
                        type='direct',
                        durable=False,
                        delivery_mode='transient')
