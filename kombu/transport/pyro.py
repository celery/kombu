"""
kombu.transport.pyro
======================

Pyro transport.

"""
from __future__ import absolute_import

import Pyro4
from Pyro4.errors import NamingError
from Queue import Queue

from . import virtual

DEFAULT_PORT = 9090

class Channel(virtual.Channel):



    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)
        transport = args[0]
        self.shared_queues = transport.shared_queues

    def queues(self):
        return self.shared_queues.get_queue_names()

    def _new_queue(self, queue, **kwargs):
        if queue not in self.queues():
            self.shared_queues.new_queue(queue)

    def _get(self, queue, timeout=None):
        queue = self._queue_for(queue)
        msg = self.shared_queues._get(queue)
        return msg

    def _queue_for(self, queue):
        if queue not in self.queues():
            self.shared_queues.new_queue(queue)
        return queue

    def _put(self, queue, message, **kwargs):
        queue = self._queue_for(queue)
        self.shared_queues._put(queue, message)

    def _size(self, queue):
        return self.shared_queues._size(queue)

    def _delete(self, queue, *args):
        self.shared_queues._delete(queue)

    def _purge(self, queue):
        return self.shared_queues._purge(queue)

    def after_reply_message_received(self, queue):
        pass


class Transport(virtual.Transport):
    Channel = Channel

    #: memory backend state is global.
    state = virtual.BrokerState()

    default_port = DEFAULT_PORT

    driver_type = 'pyro'
    driver_name = 'pyro'

    def __init__(self, client, **kwargs):
        super(Transport, self).__init__(client)
        self.client = client
        self.default_port = kwargs.get("default_port") or self.default_port
        self.shared_queues = None 

        conninfo = self.client
        for name, default_value in self.default_connection_params.items():
            if not getattr(conninfo, name, None):
                setattr(conninfo, name, default_value)

        if conninfo.hostname == 'localhost':
            conninfo.hostname = '127.0.0.1'

        Pyro4.config.HMAC_KEY=conninfo.virtual_host
        try:
            nameserver = Pyro4.locateNS(host=conninfo.hostname, port=self.default_port)
            uri = nameserver.lookup(conninfo.virtual_host) # name of registered pyro object
            self.shared_queues = Pyro4.Proxy(uri)
        except NamingError as ex:
            err = "Unable to locate pyro nameserver (%s) on host %s" % (conninfo.virtual_host, conninfo.hostname)
            raise NamingError(err)

    def driver_version(self):
        return 'N/A'
