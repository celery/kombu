"""
kombu.transport.rest
====================

REST transport.

"""
from __future__ import absolute_import

__author__ = """\
Tal Liron <tal.liron@threecrickets.com>
"""

from Queue import Empty

import anyjson

from kombu.exceptions import StdConnectionError, StdChannelError
from kombu.utils.rest import rest_request, REST_DRIVER, RESTError, RawCodec, ReverseJSONCodec, register_raw

from . import virtual

DEFAULT_PROTOCOL = 'http'
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 80

QUEUE_URL =          '%(base)s/queue/%(queue)s/'
QUEUE_MESSAGE_URL =  '%(base)s/queue/%(queue)s/message/'
QUEUE_COUNT_URL =    '%(base)s/queue/%(queue)s/count/'
QUEUES_MESSAGE_URL = '%(base)s/queues/%(group)s/message/'
ROUTES_URL =         '%(base)s/routes/%(group)s/'


class Channel(virtual.Channel):
    """
    Works with the Prudence-based RESTful messaging backend.
    
    Supports two additional values for 'body_encoding':
    
    'raw' will send the body as-is without changes. In Celery, it can be used in
    conjunction with the 'raw' (or None) Celery task serializer.

    Note that this is slightly different from a None encoding: 'raw' will also
    register a serializer named 'raw' and make it the default (instead of 'json').
    This is necessary in order to support raw encoding for fanouts, which use a
    different code path that relies on the default serializer (see Mailbox._publish
    in pidbox.py).
    
    'json' actually uses a *reverse*-JSON encoding, which is useful when the body
    is sent as a JSON-encoded string, but you want a final 'raw' encoding. In
    Celery, it can be used in conjunction with the 'json' Celery task serializer.
    """
    supports_fanout = True

    def __init__(self, *vargs, **kwargs):
        super(Channel, self).__init__(*vargs, **kwargs)
        self._base_url = None
        if self.body_encoding == 'raw':
            register_raw(make_default=True)

    def _new_queue(self, queue, **kwargs):
        print 'broker._new_queue %s' % queue
        # No action needed
        pass

    def _get(self, queue):
        message = rest_request(QUEUE_MESSAGE_URL, base=self.base_url, queue=queue)
        print 'broker._get %s -> %s' % (queue, message)
        if message is None:
            raise Empty()
        return message

    def _put(self, queue, message, **kwargs):
        print 'broker._put %s, %s' % (queue, message)
        rest_request(QUEUE_MESSAGE_URL, base=self.base_url, queue=queue,
            payload=message)

    def _purge(self, queue):
        print 'broker._purge %s' % queue
        # Note: this is the same as queue_delete()
        rest_request(QUEUE_URL, base=self.base_url, queue=queue,
            method='DELETE')

    def _size(self, queue):
        print 'broker._size %s' % queue
        count = rest_request(QUEUE_COUNT_URL, base=self.base_url, queue=queue)
        if count is None:
            return 0
        else:
            return count['count'] if 'count' in count else 0

    def get_table(self, exchange):
        routes = rest_request(ROUTES_URL, base=self.base_url, group=exchange)
        print 'broker.get_table %s -> %s' % (exchange, routes)
        if routes is not None and 'routes' in routes:
            routes = frozenset((r['routing_key'], r['pattern'], r['queue']) for r in routes['routes'])
        else:
            routes = []

        # Merge routes received from broker with local routes
        return routes | frozenset(self.state.exchanges[exchange]['table'])

    def _put_fanout(self, exchange, message, **kwargs):
        print 'broker._put_fanout %s, %s' % (exchange, message)
        rest_request(QUEUES_MESSAGE_URL, base=self.base_url, group=exchange,
            payload=message)

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        print 'broker._queue_bind %s, %s, %s, %s' % (exchange, routing_key, pattern, queue)
        rest_request(ROUTES_URL, base=self.base_url, group=exchange,
            payload={'queue': queue, 'routing_key': routing_key, 'pattern': pattern})

    def queue_delete(self, queue, **kwargs):
        print 'broker.queue_delete %s' % queue
        rest_request(QUEUE_URL, base=self.base_url, queue=queue,
            method='DELETE')
        
    def exchange_delete(self, exchange):
        print 'broker.exchange_delete %s' % exchange
        rest_request(ROUTES_URL, base=self.base_url, group=exchange,
            method='DELETE')
    
    @property
    def base_url(self):
        if self._base_url is None:
            client = self.connection.client
            protocol = client.transport_options.get('protocol') or DEFAULT_PROTOCOL
            hostname = client.hostname or DEFAULT_HOST
            self._base_url = protocol + '://' + hostname + ':' + str(client.port) + '/' + client.virtual_host
        return self._base_url

Channel.codecs['raw'] = RawCodec()
Channel.codecs['json'] = ReverseJSONCodec()


class Transport(virtual.Transport):
    Channel = Channel

    polling_interval = 1
    default_port = DEFAULT_PORT
    connection_errors = (StdConnectionError, RESTError)
    channel_errors = (StdChannelError, RESTError)
    driver_type = 'rest'
    driver_name = REST_DRIVER

    def driver_version(self):
        return '1.0'
