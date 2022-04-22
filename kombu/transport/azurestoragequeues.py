"""Azure Storage Queues transport module for kombu.

More information about Azure Storage Queues:
https://azure.microsoft.com/en-us/services/storage/queues/

Features
========
* Type: Virtual
* Supports Direct: *Unreviewed*
* Supports Topic: *Unreviewed*
* Supports Fanout: *Unreviewed*
* Supports Priority: *Unreviewed*
* Supports TTL: *Unreviewed*

Connection String
=================

Connection string has the following format:

.. code-block::

    azurestoragequeues://STORAGE_ACCOUNT_ACCESS_KEY@STORAGE_ACCOUNT_URL
    azurestoragequeues://SAS_TOKEN@STORAGE_ACCOUNT_URL

Note that if the access key for the storage account contains a slash, it will
have to be regenerated before it can be used in the connection URL.

Transport Options
=================

* ``queue_name_prefix``
"""

from __future__ import annotations

import string
from queue import Empty

from azure.core.exceptions import ResourceExistsError

from kombu.utils.encoding import safe_str
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property

from . import virtual

try:
    from azure.storage.queue import QueueServiceClient
except ImportError:  # pragma: no cover
    QueueServiceClient = None

# Azure storage queues allow only alphanumeric and dashes
# so, replace everything with a dash
CHARS_REPLACE_TABLE = {
    ord(c): 0x2d for c in string.punctuation
}


class Channel(virtual.Channel):
    """Azure Storage Queues channel."""

    domain_format = 'kombu%(vhost)s'
    _queue_service = None
    _queue_name_cache = {}
    no_ack = True
    _noack_queues = set()

    def __init__(self, *args, **kwargs):
        if QueueServiceClient is None:
            raise ImportError('Azure Storage Queues transport requires the '
                              'azure-storage-queue library')

        super().__init__(*args, **kwargs)

        self._credential, self._url = Transport.parse_uri(
            self.conninfo.hostname
        )

        for queue in self.queue_service.list_queues():
            self._queue_name_cache[queue['name']] = queue

    def basic_consume(self, queue, no_ack, *args, **kwargs):
        if no_ack:
            self._noack_queues.add(queue)

        return super().basic_consume(queue, no_ack,
                                     *args, **kwargs)

    def entity_name(self, name, table=CHARS_REPLACE_TABLE):
        """Format AMQP queue name into a valid Azure Storage Queue name."""
        return str(safe_str(name)).translate(table)

    def _ensure_queue(self, queue):
        """Ensure a queue exists."""
        queue = self.entity_name(self.queue_name_prefix + queue)
        try:
            q = self._queue_service.get_queue_client(
                queue=self._queue_name_cache[queue]
            )
        except KeyError:
            try:
                q = self.queue_service.create_queue(queue)
            except ResourceExistsError:
                q = self._queue_service.get_queue_client(queue=queue)

            self._queue_name_cache[queue] = q
        return q

    def _delete(self, queue, *args, **kwargs):
        """Delete queue by name."""
        queue_name = self.entity_name(queue)
        self._queue_name_cache.pop(queue_name, None)
        self.queue_service.delete_queue(queue_name)

    def _put(self, queue, message, **kwargs):
        """Put message onto queue."""
        q = self._ensure_queue(queue)
        encoded_message = dumps(message)
        q.send_message(encoded_message)

    def _get(self, queue, timeout=None):
        """Try to retrieve a single message off ``queue``."""
        q = self._ensure_queue(queue)

        messages = q.receive_messages(messages_per_page=1, timeout=timeout)
        try:
            message = next(messages)
        except StopIteration:
            raise Empty()

        content = loads(message.content)

        q.delete_message(message=message)

        return content

    def _size(self, queue):
        """Return the number of messages in a queue."""
        q = self._ensure_queue(queue)
        return q.get_queue_properties().approximate_message_count

    def _purge(self, queue):
        """Delete all current messages in a queue."""
        q = self._ensure_queue(queue)
        n = self._size(q.queue_name)
        q.clear_messages()
        return n

    @property
    def queue_service(self):
        if self._queue_service is None:
            self._queue_service = QueueServiceClient(
                account_url=self._url, credential=self._credential
            )

        return self._queue_service

    @property
    def conninfo(self):
        return self.connection.client

    @property
    def transport_options(self):
        return self.connection.client.transport_options

    @cached_property
    def queue_name_prefix(self):
        return self.transport_options.get('queue_name_prefix', '')


class Transport(virtual.Transport):
    """Azure Storage Queues transport."""

    Channel = Channel

    polling_interval = 1
    default_port = None
    can_parse_url = True

    @staticmethod
    def parse_uri(uri: str) -> tuple[str, str]:
        # URL like:
        #  azurestoragequeues://STORAGE_ACCOUNT_ACCESS_KEY@STORAGE_ACCOUNT_URL
        #  azurestoragequeues://SAS_TOKEN@STORAGE_ACCOUNT_URL

        # urllib parse does not work as the sas key could contain a slash
        # e.g.: azurestoragequeues://some/key@someurl

        try:
            # > 'some/key@url'
            uri = uri.replace('azurestoragequeues://', '')
            # > 'some/key',  'url'
            credential, url = uri.rsplit('@', 1)

            # Validate parameters
            assert all([credential, url])
        except Exception:
            raise ValueError(
                'Need a URI like '
                'azurestoragequeues://{SAS or access key}@{URL}'
            )

        return credential, url

    @classmethod
    def as_uri(cls, uri: str, include_password=False, mask='**') -> str:
        credential, url = cls.parse_uri(uri)
        return 'azurestoragequeues://{}@{}'.format(
            credential if include_password else mask,
            url
        )
