"""
Consul Kombu Transport

It uses Consul.io's Key/Value store to transport messages in Queues

It uses python-consul for talking to Consul's HTTP API
"""
from __future__ import absolute_import, unicode_literals

import uuid
import socket

from . import virtual
from kombu.exceptions import ChannelError
from kombu.log import get_logger
from kombu.five import Empty, monotonic
from kombu.utils.json import loads, dumps

try:
    import consul
except ImportError:
    consul = None

LOGGER = get_logger('kombu.transport.consul')

DEFAULT_PORT = 8500
DEFAULT_HOST = 'localhost'


class Channel(virtual.Channel):
    """
    Consul Channel class which talks to the Consul Key/Value store
    """
    prefix = 'kombu'
    index = None
    timeout = '10s'
    session_ttl = 30
    lock_name = '%s' % socket.gethostname()
    queues = {}

    def __init__(self, *args, **kwargs):
        if consul is None:
            raise ImportError('Missing python-consul library')

        super(Channel, self).__init__(*args, **kwargs)

        port = self.connection.client.port or self.connection.default_port
        host = self.connection.client.hostname or DEFAULT_HOST

        LOGGER.debug('Host: %s Port: %d Timeout: %s', host, port, self.timeout)

        self.client = consul.Consul(host=host, port=int(port))

    def _lock_key(self, queue):
        return '%s/%s.lock' % (self.prefix, queue)

    def _key_prefix(self, queue):
        return '%s/%s' % (self.prefix, queue)

    def _get_or_create_session(self, queue):
        """
        Try to renew the session if it exists, otherwise create a new session
        in Consul

        This session is used to obtain a lock inside Consul so that we achieve
        read-consistency between the nodes

        :param queue: The name of the Queue
        :return: The ID of the session
        """

        session_id = None
        try:
            session_id = self.queues[queue]['session_id']
        except KeyError:
            pass

        if session_id is not None:
            LOGGER.debug('Trying to renew existing session %s',
                         self.queues[queue]['session_id'])
            session = self.client.session.renew(session_id=session_id)
            session_id = None
            try:
                session_id = session['ID']
            except KeyError:
                pass

        if session_id is None:
            LOGGER.debug('Creating session %s with TTL %d', self.lock_name,
                         self.session_ttl)
            session_id = self.client.session.create(name=self.lock_name,
                                                    ttl=self.session_ttl)

            LOGGER.debug('Created session %s with id %s', self.lock_name,
                         session_id)

        return session_id

    def _obtain_lock(self, queue):
        """
        Try to obtain a lock on the Queue

        It does so by creating a object called 'lock' which is locked by the
        current session.

        This way other nodes are not able to write to the lock object which
        means that they have to wait before the lock is released

        :param queue: The name of the Queue
        :return: True on success, False otherwise
        """
        session_id = self._get_or_create_session(queue)
        lock_key = self._lock_key(queue)

        LOGGER.debug('Trying to create lock object %s with session %s',
                     lock_key, session_id)

        if self.client.kv.put(key=lock_key,
                              acquire=session_id,
                              value=self.lock_name) is False:
            LOGGER.info('Could not obtain a lock on key %s', lock_key)
            return False

        self.queues[queue]['session_id'] = session_id

        return True

    def _release_lock(self, queue):
        """
        Try to release a lock. It does so by simply removing the lock key in
        Consul.

        :param queue: The name of the queue we want to release the lock from
        :return: None
        """
        LOGGER.debug('Removing lock key %s', self._lock_key(queue))
        self.client.kv.delete(key=self._lock_key(queue))

    def _destroy_session(self, queue):
        """
        Destroy a previously created Consul session and release all locks
        it still might hold
        :param queue: The name of the Queue
        :return: None
        """
        LOGGER.debug('Destroying session %s', self.queues[queue]['session_id'])
        self.client.session.destroy(self.queues[queue]['session_id'])

    def _new_queue(self, queue, **_):
        self.queues[queue] = {'session_id': None}
        return self.client.kv.put(key=self._key_prefix(queue), value=None)

    def _delete(self, queue, *args, **_):
        self._destroy_session(queue)
        del self.queues[queue]
        self._purge(queue)

    def _put(self, queue, payload, **_):
        """
        Put `message` onto `queue`.

        This simply writes a key to the K/V store of Consul
        """
        key = '%s/msg/%d_%s' % (self._key_prefix(queue),
                                int(round(monotonic() * 1000)),
                                uuid.uuid4())

        if self.client.kv.put(key=key, value=dumps(payload), cas=0) is False:
            raise ChannelError('Cannot add key {0!r} to consul'.format(key))

    def _get(self, queue, timeout=None):
        """
        Get the first available message from the queue

        Before it does so it obtains a lock on the Key/Value store so
        only one node reads at the same time. This is for read consistency
        """
        if self._obtain_lock(queue) is False:
            raise Empty

        key = '%s/msg/' % self._key_prefix(queue)
        LOGGER.debug('Fetching key %s with index %s', key, self.index)
        self.index, data = self.client.kv.get(key=key, recurse=True,
                                              index=self.index,
                                              wait=self.timeout)

        try:
            if data is None:
                raise Empty

            LOGGER.debug('Removing key %s with modifyindex %s',
                         data[0]['Key'], data[0]['ModifyIndex'])

            self.client.kv.delete(key=data[0]['Key'],
                                  cas=data[0]['ModifyIndex'])

            return loads(data[0]['Value'])
        except TypeError:
            pass
        finally:
            self._release_lock(queue)

        raise Empty

    def _purge(self, queue):
        self._destroy_session(queue)
        return self.client.kv.delete(key='%s/msg/' % self._key_prefix(queue),
                                     recurse=True)

    def _size(self, queue):
        size = 0
        try:
            key = '%s/msg/' % self._key_prefix(queue)
            LOGGER.debug('Fetching key recursively %s with index %s', key,
                         self.index)
            self.index, data = self.client.kv.get(key=key, recurse=True,
                                                  index=self.index,
                                                  wait=self.timeout)
            size = len(data)
        except TypeError:
            pass

        LOGGER.debug('Found %d keys under %s with index %s', size, key,
                     self.index)
        return size


class Transport(virtual.Transport):
    """
    Consul K/V storage Transport for Kombu
    """
    Channel = Channel

    default_port = DEFAULT_PORT
    driver_type = 'consul'
    driver_name = 'consul'

    def __init__(self, *args, **kwargs):
        if consul is None:
            raise ImportError('Missing python-consul library')

        super(Transport, self).__init__(*args, **kwargs)

        self.connection_errors = (
            virtual.Transport.connection_errors + (
                consul.ConsulException, consul.base.ConsulException
            )
        )

        self.channel_errors = (
            virtual.Transport.channel_errors + (
                consul.ConsulException, consul.base.ConsulException
            )
        )

    def verify_connection(self, connection):
        port = connection.client.port or self.default_port
        host = connection.client.hostname or DEFAULT_HOST

        LOGGER.debug('Verify Consul connection to %s:%d', host, port)

        try:
            client = consul.Consul(host=host, port=int(port))
            client.agent.self()
            return True
        except ValueError:
            pass

        return False

    def driver_version(self):
        return consul.__version__
