"""Azure Service Bus Message Queue transport module for kombu.

Note that the Shared Access Policy used to connect to Azure Service Bus
requires Manage, Send and Listen claims since the broker will create new
queues and delete old queues as required.

Note that if the SAS key for the Service Bus account contains a slash, it will
have to be regenerated before it can be used in the connection URL.

More information about Azure Service Bus:
https://azure.microsoft.com/en-us/services/service-bus/

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

    azureservicebus://SAS_POLICY_NAME:SAS_KEY@SERVICE_BUSNAMESPACE

Transport Options
=================

* ``visibility_timeout``
* ``queue_name_prefix``
* ``wait_time_seconds``
* ``peek_lock``
* ``peek_lock_seconds``
"""

import string
from queue import Empty
from typing import Dict, Any, Optional, Union, Set

from kombu.utils.encoding import bytes_to_str, safe_str
from kombu.utils.json import loads, dumps
from kombu.utils.objects import cached_property

import azure.core.exceptions
import azure.servicebus.exceptions
import isodate
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceiver, ServiceBusSender, \
    ServiceBusReceiveMode
from azure.servicebus.management import ServiceBusAdministrationClient

from . import virtual

# dots are replaced by dash, all other punctuation replaced by underscore.
CHARS_REPLACE_TABLE = {
    ord(c): 0x5f for c in string.punctuation if c not in '_'
}


class SendReceive(object):
    def __init__(self, receiver: Optional[ServiceBusReceiver] = None, sender: Optional[ServiceBusSender] = None):
        self.receiver = receiver  # type: ServiceBusReceiver
        self.sender = sender  # type: ServiceBusSender

    def close(self) -> None:
        if self.receiver:
            self.receiver.close()
            self.receiver = None
        if self.sender:
            self.sender.close()
            self.sender = None


class Channel(virtual.Channel):
    """Azure Service Bus channel."""

    default_wait_time_seconds = 5  # in seconds
    default_peek_lock_seconds = 60  # in seconds (default 60, max 300)
    domain_format = 'kombu%(vhost)s'
    _queue_service = None  # type: ServiceBusClient
    _queue_mgmt_service = None  # type: ServiceBusAdministrationClient
    _queue_cache = {}  # type: Dict[str, SendReceive]
    _noack_queues = set()  # type: Set[str]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Validate ASB connection string
        if not all([self.conninfo.hostname, self.conninfo.userid, self.conninfo.password]):
            raise ValueError('Need an URI like azureservicebus://{SAS policy name}:{SAS key}@{ServiceBus Namespace}')

        self.qos.restore_at_shutdown = False

    def basic_consume(self, queue, no_ack, *args, **kwargs):
        if no_ack:
            self._noack_queues.add(queue)
        return super().basic_consume(
            queue, no_ack, *args, **kwargs
        )

    def basic_cancel(self, consumer_tag):
        if consumer_tag in self._consumers:
            queue = self._tag_to_queue[consumer_tag]
            self._noack_queues.discard(queue)
        return super().basic_cancel(consumer_tag)

    def _add_queue_to_cache(self,
                            name: str,
                            receiver: Optional[ServiceBusReceiver] = None,
                            sender: Optional[ServiceBusSender] = None) -> SendReceive:
        if name in self._queue_cache:
            obj = self._queue_cache[name]
            obj.sender = obj.sender or sender
            obj.receiver = obj.receiver or receiver
        else:
            obj = SendReceive(receiver, sender)
            self._queue_cache[name] = obj
        return obj

    def get_connection_string(self) -> str:
        # Generating endpoint string saves us importing internal class ServiceBusSharedKeyCredential
        # Convert endpoint into fqdn
        endpoint = 'sb://' + self.conninfo.hostname
        if not endpoint.endswith('.net'):
            endpoint += '.servicebus.windows.net'

        conn_dict = {
            'Endpoint': endpoint,
            'SharedAccessKeyName': self.conninfo.userid,
            'SharedAccessKey': self.conninfo.password,
        }
        conn_str = ';'.join([key + '=' + value for key, value in conn_dict.items()])

        return conn_str

    def entity_name(self, name: str, table: Optional[Dict[int, int]] = None) -> str:
        """Format AMQP queue name into a valid ServiceBus queue name."""
        return str(safe_str(name)).translate(table or CHARS_REPLACE_TABLE)

    def _restore(self, message: virtual.base.Message) -> None:
        # Not be needed as ASB handles unacked messages
        # Remove 'azure_message' as its not JSON serializable
        # message.delivery_info.pop('azure_message', None)
        # super()._restore(message)
        pass

    def _new_queue(self, queue: str, **kwargs) -> SendReceive:
        """Ensure a queue exists in ServiceBus."""
        queue = self.entity_name(self.queue_name_prefix + queue)

        try:
            return self._queue_cache[queue]
        except KeyError:
            # Converts seconds into ISO8601 duration format ie 66seconds = P1M6S
            lock_duration = isodate.duration_isoformat(isodate.Duration(seconds=self.peek_lock_seconds))
            try:
                self.queue_mgmt_service.create_queue(queue_name=queue, lock_duration=lock_duration)
            except azure.core.exceptions.ResourceExistsError:
                pass
            return self._add_queue_to_cache(queue)

    def _delete(self, queue: str, *args, **kwargs) -> None:
        """Delete queue by name."""
        queue = self.entity_name(self.queue_name_prefix + queue)

        self._queue_mgmt_service.delete_queue(queue)
        send_receive_obj = self._queue_cache.pop(queue, None)
        if send_receive_obj:
            send_receive_obj.close()

    def _put(self, queue: str, message, **kwargs) -> None:
        """Put message onto queue."""
        queue = self.entity_name(self.queue_name_prefix + queue)
        msg = ServiceBusMessage(dumps(message))

        queue_obj = self._queue_cache.get(queue, None)
        if queue_obj is None or queue_obj.sender is None:
            sender = self.queue_service.get_queue_sender(queue)
            queue_obj = self._add_queue_to_cache(queue, sender=sender)

        queue_obj.sender.send_messages(msg)

    def _get(self, queue: str, timeout: Optional[Union[float, int]] = None) -> Dict[str, Any]:
        """Try to retrieve a single message off ``queue``."""
        # If we're not ack'ing for this queue, just change receive_mode
        recv_mode = ServiceBusReceiveMode.RECEIVE_AND_DELETE if queue in self._noack_queues else \
            ServiceBusReceiveMode.PEEK_LOCK

        queue = self.entity_name(self.queue_name_prefix + queue)

        queue_obj = self._queue_cache.get(queue, None)
        if queue_obj is None or queue_obj.receiver is None:
            receiver = self.queue_service.get_queue_receiver(queue_name=queue, receive_mode=recv_mode)
            queue_obj = self._add_queue_to_cache(queue, receiver=receiver)

        messages = queue_obj.receiver.receive_messages(max_message_count=1,
                                                       max_wait_time=timeout or self.wait_time_seconds)

        if not messages:
            raise Empty()

        # message.body is either byte or generator[bytes]
        message = messages[0]
        if not isinstance(message.body, bytes):
            body = b''.join(message.body)
        else:
            body = message.body

        msg = loads(bytes_to_str(body))
        msg['properties']['delivery_info']['azure_message'] = message

        return msg

    def basic_ack(self, delivery_tag: str, multiple: bool = False) -> None:
        delivery_info = self.qos.get(delivery_tag).delivery_info

        if delivery_info['exchange'] in self._noack_queues:
            return super().basic_ack(delivery_tag)

        queue = self.entity_name(self.queue_name_prefix + delivery_info['exchange'])
        queue_obj = self._queue_cache.get(queue, None)
        if queue_obj is None or queue_obj.receiver is None:
            receiver = self.queue_service.get_queue_receiver(queue)
            queue_obj = self._add_queue_to_cache(queue, receiver=receiver)

        try:
            queue_obj.receiver.complete_message(delivery_info['azure_message'])
        except azure.servicebus.exceptions.MessageAlreadySettled:
            super().basic_ack(delivery_tag)
        except Exception:
            super().basic_reject(delivery_tag)
        else:
            super().basic_ack(delivery_tag)

    def _size(self, queue: str) -> int:
        """Return the number of messages in a queue."""
        queue = self.entity_name(self.queue_name_prefix + queue)
        props = self.queue_mgmt_service.get_queue_runtime_properties(queue)

        return props.total_message_count

    def _purge(self, queue):
        """Delete all current messages in a queue."""
        # Azure doesn't provide a purge api yet
        n = 0
        max_purge_count = 10
        queue = self.entity_name(self.queue_name_prefix + queue)

        # By default all the receivers will be in PEEK_LOCK receive mode
        queue_obj = self._queue_cache.get(queue, None)
        if queue not in self._noack_queues or queue_obj is None or queue_obj.receiver is None:
            receiver = self.queue_service.get_queue_receiver(queue_name=queue, receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE)
            queue_obj = self._add_queue_to_cache('purge_' + queue, receiver=receiver)

        while True:
            messages = queue_obj.receiver.receive_messages(max_message_count=max_purge_count,
                                                           max_wait_time=0.2)
            n += len(messages)

            if len(messages) < max_purge_count:
                break

        return n

    def close(self) -> None:
        # receivers and senders spawn threads so clean them up
        if not self.closed:
            self.closed = True
            for queue_obj in self._queue_cache.values():
                queue_obj.close()
            self._queue_cache.clear()

            if self.connection is not None:
                self.connection.close_channel(self)

    @property
    def queue_service(self) -> ServiceBusClient:
        if self._queue_service is None:
            self._queue_service = ServiceBusClient.from_connection_string(self.get_connection_string())
        return self._queue_service

    @property
    def queue_mgmt_service(self) -> ServiceBusAdministrationClient:
        if self._queue_mgmt_service is None:
            self._queue_mgmt_service = ServiceBusAdministrationClient.from_connection_string(self.get_connection_string())
        return self._queue_mgmt_service

    @property
    def conninfo(self):
        return self.connection.client

    @property
    def transport_options(self):
        return self.connection.client.transport_options

    @cached_property
    def queue_name_prefix(self) -> str:
        return self.transport_options.get('queue_name_prefix', '')

    @cached_property
    def wait_time_seconds(self) -> int:
        return self.transport_options.get('wait_time_seconds',
                                          self.default_wait_time_seconds)

    @cached_property
    def peek_lock_seconds(self) -> int:
        return min(self.transport_options.get('peek_lock_seconds',
                                              self.default_peek_lock_seconds),
                   300)  # Limit upper bounds to 300


class Transport(virtual.Transport):
    """Azure Service Bus transport."""

    Channel = Channel

    polling_interval = 1
    default_port = None
