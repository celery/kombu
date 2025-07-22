"""Amazon SQS transport module for Kombu.

This package implements an AMQP-like interface on top of Amazons SQS service,
with the goal of being optimized for high performance and reliability.

The default settings for this module are focused now on high performance in
task queue situations where tasks are small, idempotent and run very fast.

SQS Features supported by this transport
========================================
Long Polling
------------
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html

Long polling is enabled by setting the `wait_time_seconds` transport
option to a number > 1.  Amazon supports up to 20 seconds.  This is
enabled with 10 seconds by default.

Batch API Actions
-----------------
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-batch-api.html

The default behavior of the SQS Channel.drain_events() method is to
request up to the 'prefetch_count' messages on every request to SQS.
These messages are stored locally in a deque object and passed back
to the Transport until the deque is empty, before triggering a new
API call to Amazon.

This behavior dramatically speeds up the rate that you can pull tasks
from SQS when you have short-running tasks (or a large number of workers).

When a Celery worker has multiple queues to monitor, it will pull down
up to 'prefetch_count' messages from queueA and work on them all before
moving on to queueB.  If queueB is empty, it will wait up until
'polling_interval' expires before moving back and checking on queueA.

Message Attributes
-----------------
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html

SQS supports sending message attributes along with the message body.
To use this feature, you can pass a 'message_attributes' as keyword argument
to `basic_publish` method.

Other Features supported by this transport
==========================================
Predefined Queues
-----------------
The default behavior of this transport is to use a single AWS credential
pair in order to manage all SQS queues (e.g. listing queues, creating
queues, polling queues, deleting messages).

If it is preferable for your environment to use multiple AWS credentials, you
can use the 'predefined_queues' setting inside the 'transport_options' map.
This setting allows you to specify the SQS queue URL and AWS credentials for
each of your queues. For example, if you have two queues which both already
exist in AWS) you can tell this transport about them as follows:

.. code-block:: python

    transport_options = {
      'predefined_queues': {
        'queue-1': {
          'url': 'https://sqs.us-east-1.amazonaws.com/xxx/aaa',
          'access_key_id': 'a',
          'secret_access_key': 'b',
          'backoff_policy': {1: 10, 2: 20, 3: 40, 4: 80, 5: 320, 6: 640}, # optional
          'backoff_tasks': ['svc.tasks.tasks.task1'] # optional
        },
        'queue-2.fifo': {
          'url': 'https://sqs.us-east-1.amazonaws.com/xxx/bbb.fifo',
          'access_key_id': 'c',
          'secret_access_key': 'd',
          'backoff_policy': {1: 10, 2: 20, 3: 40, 4: 80, 5: 320, 6: 640}, # optional
          'backoff_tasks': ['svc.tasks.tasks.task2'] # optional
        },
      }
    'sts_role_arn': 'arn:aws:iam::<xxx>:role/STSTest', # optional
    'sts_token_timeout': 900, # optional
    'sts_token_buffer_time': 0, # optional, added in 5.6.0
    }

Note that FIFO and standard queues must be named accordingly (the name of
a FIFO queue must end with the .fifo suffix).

backoff_policy & backoff_tasks are optional arguments. These arguments
automatically change the message visibility timeout, in order to have
different times between specific task retries. This would apply after
task failure.

AWS STS authentication is supported, by using sts_role_arn, and
sts_token_timeout. sts_role_arn is the assumed IAM role ARN we are trying
to access with. sts_token_timeout is the token timeout, defaults (and minimum)
to 900 seconds. After the mentioned period, a new token will be created.

--------------------
Predefined Exchanges
--------------------
When using a fanout exchange with this transport, messages are sent to an AWS SNS, which then forwards the messages
to all subscribed queues.

The default behavior of this transport is to create the SNS topic when the exchange is first declared.
However, it is also possible to use a predefined SNS topic instead of letting the transport create it.

.. code-block:: python

    transport_options = {
      'predefined_exchanges': {
        'exchange-1': {
          'arn': 'arn:aws:sns:us-east-1:xxx:exchange-1',
          'access_key_id': 'a',
          'secret_access_key': 'b',
        },
        'exchange-2': {
          'arn': 'arn:aws:sns:us-east-1:xxx:exchange-2',
          'access_key_id': 'c',
          'secret_access_key': 'd',
        },
      }
    }

.. versionadded:: 5.6.0
    sts_token_buffer_time (seconds) is the time by which you want to refresh your token
    earlier than its actual expiration time, defaults to 0 (no time buffer will be added),
    should be less than sts_token_timeout.



If you authenticate using Okta_ (e.g. calling |gac|_), you can also specify
a 'session_token' to connect to a queue. Note that those tokens have a
limited lifetime and are therefore only suited for short-lived tests.

.. _Okta: https://www.okta.com/
.. _gac: https://github.com/Nike-Inc/gimme-aws-creds#readme
.. |gac| replace:: ``gimme-aws-creds``


Client config
-------------
In some cases you may need to override the botocore config. You can do it
as follows:

.. code-block:: python

    transport_option = {
      'client-config': {
          'connect_timeout': 5,
       },
    }

For a complete list of settings you can adjust using this option see
https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html

Features
========
* Type: Virtual
* Supports Direct: Yes
* Supports Topic: Yes
* Supports Fanout: Yes
* Supports Priority: No
* Supports TTL: No
"""

from __future__ import annotations

import base64
import binascii
import re
import socket
import string
import threading
import uuid
from datetime import datetime, timedelta
from json import JSONDecodeError
from queue import Empty
from typing import Any, Literal

from botocore.client import BaseClient, Config
from botocore.exceptions import ClientError
from vine import ensure_promise, promise, transform

from kombu import Exchange
from kombu.asynchronous import get_event_loop
from kombu.asynchronous.aws.ext import boto3, exceptions
from kombu.asynchronous.aws.sqs.connection import AsyncSQSConnection
from kombu.asynchronous.aws.sqs.message import AsyncMessage
from kombu.log import get_logger
from kombu.transport import virtual
from kombu.utils import json, scheduling
from kombu.utils.encoding import bytes_to_str, safe_str
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property

logger = get_logger(__name__)

# dots are replaced by dash, dash remains dash, all other punctuation
# replaced by underscore.
CHARS_REPLACE_TABLE = {
    ord(c): 0x5f for c in string.punctuation if c not in '-_.'
}
CHARS_REPLACE_TABLE[0x2e] = 0x2d  # '.' -> '-'

#: SQS bulk get supports a maximum of 10 messages at a time.
SQS_MAX_MESSAGES = 10

_SUPPORTED_BOTO_SERVICES = Literal["sqs", "sns"]


def maybe_int(x):
    """Try to convert x' to int, or return x' if that fails."""
    try:
        return int(x)
    except ValueError:
        return x


class UndefinedQueueException(Exception):
    """Predefined queues are being used and an undefined queue was used."""


class UndefinedExchangeException(Exception):
    """Predefined exchanges are being used and an undefined exchange/SNS topic was used."""


class InvalidQueueException(Exception):
    """Predefined queues are being used and configuration is not valid."""


class AccessDeniedQueueException(Exception):
    """Raised when access to the AWS queue is denied.

    This may occur if the permissions are not correctly set or the
    credentials are invalid.
    """


class DoesNotExistQueueException(Exception):
    """The specified queue doesn't exist."""


class QoS(virtual.QoS):
    """Quality of Service guarantees implementation for SQS."""

    def reject(self, delivery_tag, requeue=False):
        super().reject(delivery_tag, requeue=requeue)
        routing_key, message, backoff_tasks, backoff_policy = \
            self._extract_backoff_policy_configuration_and_message(
                delivery_tag)
        if routing_key and message and backoff_tasks and backoff_policy:
            self.apply_backoff_policy(
                routing_key, delivery_tag, backoff_policy, backoff_tasks)

    def _extract_backoff_policy_configuration_and_message(self, delivery_tag):
        try:
            message = self._delivered[delivery_tag]
            routing_key = message.delivery_info['routing_key']
        except KeyError:
            return None, None, None, None
        if not routing_key or not message:
            return None, None, None, None
        queue_config = self.channel.predefined_queues.get(routing_key, {})
        backoff_tasks = queue_config.get('backoff_tasks')
        backoff_policy = queue_config.get('backoff_policy')
        return routing_key, message, backoff_tasks, backoff_policy

    def apply_backoff_policy(self, routing_key, delivery_tag,
                             backoff_policy, backoff_tasks):
        queue_url = self.channel._queue_cache[routing_key]
        task_name, number_of_retries = \
            self.extract_task_name_and_number_of_retries(delivery_tag)
        if not task_name or not number_of_retries:
            return None
        policy_value = backoff_policy.get(number_of_retries)
        if task_name in backoff_tasks and policy_value is not None:
            c = self.channel.sqs(routing_key)
            c.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=delivery_tag,
                VisibilityTimeout=policy_value
            )

    def extract_task_name_and_number_of_retries(self, delivery_tag):
        message = self._delivered[delivery_tag]
        message_headers = message.headers
        task_name = message_headers['task']
        number_of_retries = int(
            message.properties['delivery_info']['sqs_message']
            ['Attributes']['ApproximateReceiveCount'])
        return task_name, number_of_retries


class Channel(virtual.Channel):
    """SQS Channel."""

    default_region = 'us-east-1'
    default_visibility_timeout = 1800  # 30 minutes.
    default_wait_time_seconds = 10  # up to 20 seconds max
    domain_format = 'kombu%(vhost)s'
    _asynsqs = None
    _predefined_queue_async_clients = {}  # A client for each predefined queue
    _sqs = None
    _sns = None
    _predefined_queue_clients = {}  # A client for each predefined queue
    _queue_cache = {}  # SQS queue name => SQS queue URL
    _noack_queues = set()

    QoS = QoS
    # https://stackoverflow.com/questions/475074/regex-to-parse-or-validate-base64-data
    B64_REGEX = re.compile(rb'^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$')

    def __init__(self, *args, **kwargs):
        if boto3 is None:
            raise ImportError('boto3 is not installed')
        super().__init__(*args, **kwargs)
        self._validate_predifined_queues()

        # SQS blows up if you try to create a new queue when one already
        # exists but with a different visibility_timeout.  This prepopulates
        # the queue_cache to protect us from recreating
        # queues that are known to already exist.
        self._update_queue_cache(self.queue_name_prefix)

        self.hub = kwargs.get('hub') or get_event_loop()

    def _validate_predifined_queues(self):
        """Check that standard and FIFO queues are named properly.

        AWS requires FIFO queues to have a name
        that ends with the .fifo suffix.
        """
        for queue_name, q in self.predefined_queues.items():
            fifo_url = q['url'].endswith('.fifo')
            fifo_name = queue_name.endswith('.fifo')
            if fifo_url and not fifo_name:
                raise InvalidQueueException(
                    "Queue with url '{}' must have a name "
                    "ending with .fifo".format(q['url'])
                )
            elif not fifo_url and fifo_name:
                raise InvalidQueueException(
                    "Queue with name '{}' is not a FIFO queue: "
                    "'{}'".format(queue_name, q['url'])
                )

    def _update_queue_cache(self, queue_name_prefix):
        if self.predefined_queues:
            for queue_name, q in self.predefined_queues.items():
                self._queue_cache[queue_name] = q['url']
            return

        resp = self.sqs().list_queues(QueueNamePrefix=queue_name_prefix)
        for url in resp.get('QueueUrls', []):
            queue_name = url.split('/')[-1]
            self._queue_cache[queue_name] = url

    def basic_consume(self, queue, no_ack, *args, **kwargs):
        if no_ack:
            self._noack_queues.add(queue)
        if self.hub:
            self._loop1(queue)
        return super().basic_consume(
            queue, no_ack, *args, **kwargs
        )

    def basic_cancel(self, consumer_tag):
        if consumer_tag in self._consumers:
            queue = self._tag_to_queue[consumer_tag]
            self._noack_queues.discard(queue)
        return super().basic_cancel(consumer_tag)

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        # If the exchange is a fanout exchange, initialise the SNS topic
        if (self.state.exchanges[exchange].get("type") == "fanout" and self.supports_fanout
        ):
            self.sns().initialise_exchange(exchange)
            self.sns().subscribe_queue_to_topic(queue_name=queue, exchange_name=exchange)

    def drain_events(self, timeout=None, callback=None, **kwargs):
        """Return a single payload message from one of our queues.

        Raises
        ------
            Queue.Empty: if no messages available.
        """
        # If we're not allowed to consume or have no consumers, raise Empty
        if not self._consumers or not self.qos.can_consume():
            raise Empty()

        # At this point, go and get more messages from SQS
        self._poll(self.cycle, callback, timeout=timeout)

    def _reset_cycle(self):
        """Reset the consume cycle.

        Returns
        -------
            FairCycle: object that points to our _get_bulk() method
                rather than the standard _get() method.  This allows for
                multiple messages to be returned at once from SQS (
                based on the prefetch limit).
        """
        self._cycle = scheduling.FairCycle(
            self._get_bulk, self._active_queues, Empty,
        )

    def entity_name(self, name, table=CHARS_REPLACE_TABLE):
        """Format AMQP queue name into a legal SQS queue name."""
        if name.endswith('.fifo'):
            partial = name[:-len('.fifo')]
            partial = str(safe_str(partial)).translate(table)
            return partial + '.fifo'
        else:
            return str(safe_str(name)).translate(table)

    def canonical_queue_name(self, queue_name):
        return self.entity_name(self.queue_name_prefix + queue_name)

    def _resolve_queue_url(self, queue):
        """Try to retrieve the SQS queue URL for a given queue name."""
        # Translate to SQS name for consistency with initial
        # _queue_cache population.
        sqs_qname = self.canonical_queue_name(queue)

        # The SQS ListQueues method only returns 1000 queues.  When you have
        # so many queues, it's possible that the queue you are looking for is
        # not cached.  In this case, we could update the cache with the exact
        # queue name first.
        if sqs_qname not in self._queue_cache:
            self._update_queue_cache(sqs_qname)
        try:
            return self._queue_cache[sqs_qname]
        except KeyError:
            if self.predefined_queues:
                raise UndefinedQueueException((
                                                  "Queue with name '{}' must be "
                                                  "defined in 'predefined_queues'."
                                              ).format(sqs_qname))

            raise DoesNotExistQueueException(
                f"Queue with name '{sqs_qname}' doesn't exist in SQS"
            )

    def _new_queue(self, queue, **kwargs):
        """Ensure a queue with given name exists in SQS.

        Arguments:
        ---------
            queue (str): the AMQP queue name
        Returns
            str: the SQS queue URL
        """
        try:
            return self._resolve_queue_url(queue)
        except DoesNotExistQueueException:
            sqs_qname = self.canonical_queue_name(queue)
            attributes = {'VisibilityTimeout': str(self.visibility_timeout)}
            if sqs_qname.endswith('.fifo'):
                attributes['FifoQueue'] = 'true'

            resp = self._create_queue(sqs_qname, attributes)
            queue_url = self._queue_cache[sqs_qname] = resp['QueueUrl']
            return queue_url

    def _create_queue(self, queue_name, attributes):
        """Create an SQS queue with a given name and nominal attributes."""
        # Allow specifying additional boto create_queue Attributes
        # via transport options
        if self.predefined_queues:
            return None

        attributes.update(
            self.transport_options.get('sqs-creation-attributes') or {},
        )

        queue_tags = self.transport_options.get('queue_tags')

        create_params = {
            'QueueName':  queue_name,
            'Attributes': attributes,
        }

        if queue_tags:
            create_params['tags'] = queue_tags

        return self.sqs(queue=queue_name).create_queue(**create_params)

    def _delete(self, queue, exchange, *args, **kwargs):
        """Delete queue by name."""
        if self.predefined_queues:
            return

        q_url = self._resolve_queue_url(queue)
        self.sqs().delete_queue(
            QueueUrl=q_url,
        )

        # If the exchange is a fanout exchange, unsubscribe the queue to the SNS topic
        exchange_info = self.state.exchanges.get(exchange)
        if exchange_info.get("type", "") == 'fanout':
            self.sns().unsubscribe_queue_from_topic(queue)

        self._queue_cache.pop(queue, None)

    def _put(self, queue, message, **kwargs):
        """Put message onto queue."""
        q_url = self._new_queue(queue)
        kwargs = {'QueueUrl': q_url}
        if 'properties' in message:
            if 'message_attributes' in message['properties']:
                # we don't want to want to have the attribute in the body
                kwargs['MessageAttributes'] = \
                    message['properties'].pop('message_attributes')
            if queue.endswith('.fifo'):
                if 'MessageGroupId' in message['properties']:
                    kwargs['MessageGroupId'] = \
                        message['properties']['MessageGroupId']
                else:
                    kwargs['MessageGroupId'] = 'default'
                if 'MessageDeduplicationId' in message['properties']:
                    kwargs['MessageDeduplicationId'] = \
                        message['properties']['MessageDeduplicationId']
                else:
                    kwargs['MessageDeduplicationId'] = str(uuid.uuid4())
            else:
                if "DelaySeconds" in message['properties']:
                    kwargs['DelaySeconds'] = \
                        message['properties']['DelaySeconds']

        if self.sqs_base64_encoding:
            body = AsyncMessage().encode(dumps(message))
        else:
            body = dumps(message)
        kwargs['MessageBody'] = body

        c = self.sqs(queue=self.canonical_queue_name(queue))
        if message.get('redelivered'):
            c.change_message_visibility(
                QueueUrl=q_url,
                ReceiptHandle=message['properties']['delivery_tag'],
                VisibilityTimeout=self.wait_time_seconds
            )
        else:
            c.send_message(**kwargs)

    def _message_to_python(self, message, queue_name, q_url):
        raw_msg_body = message['Body']
        decoded_bytes = self._decode_python_message_body(raw_msg_body)
        text = bytes_to_str(decoded_bytes)

        payload = self._prepare_json_payload(text)

        # handle no-ack queues immediately
        if queue_name in self._noack_queues:
            self._delete_message(queue_name, message)
            return payload

        return self._envelope_payload(payload, text, message, q_url)

    def _messages_to_python(self, messages, queue):
        """Convert a list of SQS Message objects into Payloads.

        This method handles converting SQS Message objects into
        Payloads, and appropriately updating the queue depending on
        the 'ack' settings for that queue.

        Arguments:
        ---------
            messages (SQSMessage): A list of SQS Message objects.
            queue (str): Name representing the queue they came from.

        Returns
        -------
            List: A list of Payload objects
        """
        q_url = self._new_queue(queue)
        return [self._message_to_python(m, queue, q_url) for m in messages]

    def _receive_message(
        self,
        queue: str,
        max_number_of_messages: int = 1,
        wait_time_seconds: int | None = None
    ):
        """Unified receive_message wrapper for SQS (boto3.client.SQS) with full attribute support.

        :param queue: The queue as a string
        :param max_number_of_messages: Int of max number of messages to receive.
        :param wait_time_seconds: Int of sqs wait time in seconds.
        :return: SQS client recieve_message
        """
        q_url: str = self._new_queue(queue)
        client = self.sqs(queue=queue)

        message_system_attribute_names = self.get_message_attributes.get(
            'MessageSystemAttributeNames')
        message_attribute_names = self.get_message_attributes.get(
            'MessageAttributeNames')

        params: dict[str, Any] = {
            'QueueUrl':                    q_url,
            'MaxNumberOfMessages':         max_number_of_messages,
            'WaitTimeSeconds':             wait_time_seconds or self.wait_time_seconds,
            'MessageAttributeNames':       message_attribute_names,
            'MessageSystemAttributeNames': message_system_attribute_names
        }

        return client.receive_message(**params)

    def _get_bulk(self, queue,
                  max_if_unlimited=SQS_MAX_MESSAGES, callback=None):
        """Try to retrieve multiple messages off ``queue``.

        Where :meth:`_get` returns a single Payload object, this method
        returns a list of Payload objects.  The number of objects returned
        is determined by the total number of messages available in the queue
        and the number of messages the QoS object allows (based on the
        prefetch_count).

        Note:
        ----
            Ignores QoS limits so caller is responsible for checking
            that we are allowed to consume at least one message from the
            queue.  get_bulk will then ask QoS for an estimate of
            the number of extra messages that we can consume.

        Arguments:
        ---------
            queue (str): The queue name to pull from.

        Returns
        -------
            List[Message]
        """
        # drain_events calls `can_consume` first, consuming
        # a token, so we know that we are allowed to consume at least
        # one message.

        # Note: ignoring max_messages for SQS with boto3
        max_count = self._get_message_estimate()
        if max_count:
            resp = self._receive_message(
                queue=queue,
                wait_time_seconds=self.wait_time_seconds,
                max_number_of_messages=max_count
            )

            if messages := resp.get('Messages'):
                for m in messages:
                    m['Body'] = AsyncMessage(body=m['Body']).decode()
                for msg in self._messages_to_python(messages, queue):
                    self.connection._deliver(msg, queue)
                return

        raise Empty()

    def _get(self, queue):
        """Try to retrieve a single message off ``queue``."""
        resp = self._receive_message(
            queue=queue,
            wait_time_seconds=self.wait_time_seconds,
            max_number_of_messages=1
        )

        if messages := resp.get('Messages'):
            body = AsyncMessage(body=messages[0]['Body']).decode()
            messages[0]['Body'] = body
            return self._messages_to_python(messages, queue)[0]
        raise Empty()

    def _loop1(self, queue, _=None):
        self.hub.call_soon(self._schedule_queue, queue)

    def _schedule_queue(self, queue):
        if queue in self._active_queues:
            if self.qos.can_consume():
                self._get_bulk_async(
                    queue, callback=promise(self._loop1, (queue,)),
                )
            else:
                self._loop1(queue)

    def _get_message_estimate(self, max_if_unlimited=SQS_MAX_MESSAGES):
        maxcount = self.qos.can_consume_max_estimate()
        return min(
            max_if_unlimited if maxcount is None else max(maxcount, 1),
            max_if_unlimited,
        )

    def _get_bulk_async(self, queue, callback=None):
        maxcount = self._get_message_estimate()
        if maxcount:
            return self._get_async(queue, maxcount, callback=callback)
        # Not allowed to consume, make sure to notify callback..
        callback = ensure_promise(callback)
        callback([])
        return callback

    def _get_async(self, queue, count=1, callback=None):
        q_url = self._new_queue(queue)
        qname = self.canonical_queue_name(queue)
        return self._get_from_sqs(
            queue_name=qname, queue_url=q_url, count=count,
            connection=self.asynsqs(queue=qname),
            callback=transform(
                self._on_messages_ready, callback, q_url, queue
            ),
        )

    def _on_messages_ready(self, queue, qname, messages):
        if 'Messages' in messages and messages['Messages']:
            callbacks = self.connection._callbacks
            for msg in messages['Messages']:
                msg_parsed = self._message_to_python(msg, qname, queue)
                callbacks[qname](msg_parsed)

    def _get_from_sqs(self, queue_name, queue_url,
                      connection, count=1, callback=None):
        """Retrieve and handle messages from SQS.

        Uses long polling and returns :class:`~vine.promises.promise`.
        """
        return connection.receive_message(
            queue_name, queue_url, number_messages=count,
            wait_time_seconds=self.wait_time_seconds,
            callback=callback,
        )

    def _restore(self, message,
                 unwanted_delivery_info=('sqs_message', 'sqs_queue')):
        for unwanted_key in unwanted_delivery_info:
            # Remove objects that aren't JSON serializable (Issue #1108).
            message.delivery_info.pop(unwanted_key, None)
        return super()._restore(message)

    def basic_ack(self, delivery_tag, multiple=False):
        try:
            message = self.qos.get(delivery_tag).delivery_info
            sqs_message = message['sqs_message']
        except KeyError:
            super().basic_ack(delivery_tag)
        else:
            queue = None
            if 'routing_key' in message:
                queue = self.canonical_queue_name(message['routing_key'])

            try:
                self.sqs(queue=queue).delete_message(
                    QueueUrl=message['sqs_queue'],
                    ReceiptHandle=sqs_message['ReceiptHandle']
                )
            except ClientError as exception:
                if exception.response['Error']['Code'] == 'AccessDenied':
                    raise AccessDeniedQueueException(
                        exception.response["Error"]["Message"]
                    )
                super().basic_reject(delivery_tag)
            else:
                super().basic_ack(delivery_tag)

    def _size(self, queue):
        """Return the number of messages in a queue."""
        q_url = self._new_queue(queue)
        c = self.sqs(queue=self.canonical_queue_name(queue))
        resp = c.get_queue_attributes(
            QueueUrl=q_url,
            AttributeNames=['ApproximateNumberOfMessages'])
        return int(resp['Attributes']['ApproximateNumberOfMessages'])

    def _purge(self, queue):
        """Delete all current messages in a queue."""
        q_url = self._new_queue(queue)
        # SQS is slow at registering messages, so run for a few
        # iterations to ensure messages are detected and deleted.
        size = 0
        for i in range(10):
            size += int(self._size(queue))
            if not size:
                break
        self.sqs(queue=queue).purge_queue(QueueUrl=q_url)
        return size

    def close(self):
        super().close()
        # if self._asynsqs:
        #     try:
        #         self.asynsqs().close()
        #     except AttributeError as exc:  # FIXME ???
        #         if "can't set attribute" not in str(exc):
        #             raise

    def new_sqs_client(self, region, access_key_id, secret_access_key, session_token=None):
        """Create a new SQS client.

        :param region: The AWS region to use.
        :param access_key_id: The AWS access key ID for authenticating with boto.
        :param secret_access_key: The AWS secret access key for authenticating with boto.
        :param session_token: The AWS session token for authenticating with boto, if required.
        :returns: A Boto SQS client.
        """
        return self._new_boto_client(
            service='sqs', region=region, access_key_id=access_key_id, secret_access_key=secret_access_key,
            session_token=session_token
        )

    def _new_boto_client(self, service: _SUPPORTED_BOTO_SERVICES, region, access_key_id, secret_access_key,
                         session_token=None):
        session = boto3.session.Session(
            region_name=region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token,
        )
        is_secure = self.is_secure if self.is_secure is not None else True
        client_kwargs = {
            'use_ssl': is_secure
        }
        if self.endpoint_url is not None:
            client_kwargs['endpoint_url'] = self.endpoint_url
        client_config = self.transport_options.get('client-config') or {}
        config = Config(**client_config)
        return session.client(service, config=config, **client_kwargs)

    def sqs(self, queue=None):
        # If a queue has been provided, check if the queue has been defined already. Re-use it's client if possible.
        if queue is not None and self.predefined_queues:
            # Raise if queue is not defined
            if queue not in self.predefined_queues:
                raise UndefinedQueueException(f"Queue with name '{queue}' must be defined in 'predefined_queues'.")

            q = self.predefined_queues[queue]

            # Handle authenticating boto client with tokens
            if self.transport_options.get('sts_role_arn'):
                return self._handle_sts_session(queue, q)

            # If the queue has already been defined, then return the client for the queue
            if queue in self._predefined_queue_clients:
                return self._predefined_queue_clients[queue]

            # Create client, add it to the queue map and return
            c = self._predefined_queue_clients[queue] = self.new_sqs_client(
                region=q.get('region', self.region),
                access_key_id=q.get('access_key_id', self.conninfo.userid),
                secret_access_key=q.get('secret_access_key', self.conninfo.password)
            )
            return c

        # If SQS client has been initialised, return it
        if self._sqs is not None:
            return self._sqs

        # Initialise a new SQS client and return it
        c = self._sqs = self.new_sqs_client(
            region=self.region,
            access_key_id=self.conninfo.userid,
            secret_access_key=self.conninfo.password,
        )
        return c

    def sns(self) -> _SnsFanout:
        # If an SNS class has not been initialised, then initialise it
        if not self._sns:
            self._sns = _SnsFanout(channel=self)

        # Return the existing SNS class instance
        return self._sns

    def remove_stale_sns_subscriptions(self, exchange_name: str) -> None:
        """Removes any stale SNS topic subscriptions

        This method will check that any SQS subscriptions on the SNS topic are associated with SQS queues. If not,
        it will remove the stale subscription. This method will only work if the 'supports_fanout' property is True.

        :param exchange_name: The exchange to check for stale subscriptions
        :return: None
        """
        if self.supports_fanout:
            return self.sns().remove_stale_sns_subscriptions(exchange_name)
        return None

    def _handle_sts_session(self, queue: str, q):
        """Checks if the STS token needs renewing for SQS

        :param queue: The queue name
        :param q: The queue object
        :returns: The SQS client with a refreshed STS token
        """
        region = q.get('region', self.region)

        # Check if a token refresh is needed
        if self.is_sts_token_refresh_required(
            item=q, client_map=self._predefined_queue_clients, expire_time=getattr(self, "sts_expiration", None)
        ):
            return self._new_predefined_queue_client_with_sts_session(queue, region)

        # If token refresh is not required, return existing client
        return self._predefined_queue_clients[queue]

    @staticmethod
    def is_sts_token_refresh_required(
        item: Any, client_map: dict[str, BaseClient], expire_time: datetime | None = None
    ) -> bool:
        """Checks if the STS token needs renewing

        This method will check different STS expiry times depending on the service the token was used for.

        :param item: Either the queue or the exchange object
        :param client_map: Map of client names to boto3 clients. Either the queue or exchange map
        :param expire_time: The datetime when the token expires.
        :returns: True if the token needs renewing, False otherwise.
        """
        # Get the expiry time of the STS token depending on the service
        if not expire_time:  # STS token - token init
            return True
        elif expire_time.replace(tzinfo=None) < datetime.utcnow():  # STS token - refresh if expired
            return True
        elif item not in client_map:  # STS token = refresh if exchange or queue is not in client map
            return True
        else:  # STS token - ruse existing
            return False

    def generate_sts_session_token_with_buffer(self, role_arn, token_expiry_seconds, token_buffer_seconds=0):
        """Generate STS session credentials with an optional expiration buffer.

        The buffer is only applied if it is less than `token_expiry_seconds` to prevent an expired token.
        """
        credentials = self.generate_sts_session_token(role_arn, token_expiry_seconds)
        if token_buffer_seconds and 0 < token_buffer_seconds < token_expiry_seconds:
            credentials["Expiration"] -= timedelta(seconds=token_buffer_seconds)
        return credentials

    def _new_predefined_queue_client_with_sts_session(self, queue, region):
        # Handle STS token refresh
        sts_creds = self.get_sts_credentials()
        self.sts_expiration = sts_creds['Expiration']

        # Get new client and return it
        c = self._predefined_queue_clients[queue] = self.new_sqs_client(
            region=region,
            access_key_id=sts_creds['AccessKeyId'],
            secret_access_key=sts_creds['SecretAccessKey'],
            session_token=sts_creds['SessionToken'],
        )
        return c

    def get_sts_credentials(self):
        return self.generate_sts_session_token_with_buffer(
            self.transport_options.get('sts_role_arn'),
            self.transport_options.get('sts_token_timeout', 900),
            self.transport_options.get('sts_token_buffer_time', 0),
        )

    @staticmethod
    def generate_sts_session_token(role_arn: str, token_expiry_seconds: int):
        sts_client = boto3.client('sts')
        sts_policy = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName='Celery',
            DurationSeconds=token_expiry_seconds
        )
        return sts_policy['Credentials']

    def asynsqs(self, queue=None):
        message_system_attribute_names = self.get_message_attributes.get(
            'MessageSystemAttributeNames')
        message_attribute_names = self.get_message_attributes.get(
            'MessageAttributeNames')

        if queue is not None and self.predefined_queues:
            if queue in self._predefined_queue_async_clients and \
                not hasattr(self, 'sts_expiration'):
                return self._predefined_queue_async_clients[queue]
            if queue not in self.predefined_queues:
                raise UndefinedQueueException(f"Queue with name '{queue}' must be defined in 'predefined_queues'.")

            q = self.predefined_queues[queue]
            c = self._predefined_queue_async_clients[queue] = \
                AsyncSQSConnection(
                    sqs_connection=self.sqs(queue=queue),
                    region=q.get('region', self.region),
                    message_system_attribute_names=message_system_attribute_names,
                    message_attribute_names=message_attribute_names
                )
            return c

        if self._asynsqs is not None:
            return self._asynsqs

        c = self._asynsqs = AsyncSQSConnection(
            sqs_connection=self.sqs(queue=queue),
            region=self.region,
            message_system_attribute_names=message_system_attribute_names,
            message_attribute_names=message_attribute_names
        )
        return c

    @property
    def conninfo(self):
        return self.connection.client

    @property
    def transport_options(self):
        return self.connection.client.transport_options

    @cached_property
    def visibility_timeout(self):
        return (self.transport_options.get('visibility_timeout') or
                self.default_visibility_timeout)

    @cached_property
    def predefined_queues(self):
        """Map of queue_name to predefined queue settings."""
        return self.transport_options.get('predefined_queues', {})

    @cached_property
    def predefined_exchanges(self):
        """Map of exchange_name to predefined SNS client."""
        return self.transport_options.get('predefined_exchanges', {})

    @cached_property
    def queue_name_prefix(self):
        return self.transport_options.get('queue_name_prefix', '')

    @cached_property
    def supports_fanout(self):
        return self.transport_options.get('supports_fanout', False)

    @cached_property
    def region(self):
        return (self.transport_options.get('region') or
                boto3.Session().region_name or
                self.default_region)

    @cached_property
    def regioninfo(self):
        return self.transport_options.get('regioninfo')

    @cached_property
    def is_secure(self):
        return self.transport_options.get('is_secure')

    @cached_property
    def port(self):
        return self.transport_options.get('port')

    @cached_property
    def endpoint_url(self):
        if self.conninfo.hostname is not None:
            scheme = 'https' if self.is_secure else 'http'
            if self.conninfo.port is not None:
                port = f':{self.conninfo.port}'
            else:
                port = ''
            return '{}://{}{}'.format(
                scheme,
                self.conninfo.hostname,
                port
            )

    @cached_property
    def wait_time_seconds(self) -> int:
        return self.transport_options.get('wait_time_seconds',
                                          self.default_wait_time_seconds)

    @cached_property
    def sqs_base64_encoding(self):
        return self.transport_options.get('sqs_base64_encoding', True)

    @cached_property
    def fetch_message_attributes(self):
        return self.transport_options.get('fetch_message_attributes', None)

    @property
    def get_message_attributes(self) -> dict[str, Any]:
        """Get the message attributes to be fetched from SQS.

        Ensures 'ApproximateReceiveCount' is included in system attributes if list is provided.
        - The number of retries is managed by SQS /
            (specifically by the ``ApproximateReceiveCount`` message attribute)
        - See: class QoS(virtual.QoS):
            (method) def extract_task_name_and_number_of_retries

        :return: A dictionary with SQS message attribute fetch config.
        """
        APPROXIMATE_RECEIVE_COUNT = 'ApproximateReceiveCount'
        fetch = self.fetch_message_attributes
        message_system_attrs = None
        message_attrs = None

        if fetch is None or isinstance(fetch, str):
            return {
                'MessageAttributeNames':       [],
                'MessageSystemAttributeNames': [APPROXIMATE_RECEIVE_COUNT],
            }

        if isinstance(fetch, list):
            message_system_attrs = ['ALL'] if 'ALL'.lower() in [s.lower() for s in fetch] else (
                list(set(fetch + [APPROXIMATE_RECEIVE_COUNT]))
            )

        elif isinstance(fetch, dict):
            system = fetch.get('MessageSystemAttributeNames', [])
            attrs = fetch.get('MessageAttributeNames', None)

            if isinstance(system, list):
                message_system_attrs = ['ALL'] if 'ALL'.lower() in [s.lower() for s in system] else (
                    list(set(system + [APPROXIMATE_RECEIVE_COUNT]))
                )

            if isinstance(attrs, list) and attrs:
                message_attrs = ['ALL'] if 'ALL'.lower() in [s.lower() for s in attrs] else (
                    list(set(attrs))
                )

        return {
            'MessageAttributeNames':       sorted(message_attrs) if message_attrs else [],
            'MessageSystemAttributeNames': (
                sorted(message_system_attrs) if message_system_attrs else [APPROXIMATE_RECEIVE_COUNT]
            )
        }

    def _put_fanout(self, exchange: str, message: dict, routing_key, **kwargs):
        """Add a message to fanout queues by adding a notification to an SNS topic, with subscribed SQS queues.

        :param exchange: The name of the exchange to add the notification to.
        :param message: The message to be added.
        :param routing_key: The routing key to use for the notification.
        :param kwargs: Additional parameters
        :return: None
        """
        # Extract properties from message for SNS parameters
        sns_params = {}
        if properties := message.get("properties"):
            if message_attrs := properties.get('message_attributes'):
                sns_params['MessageAttributes'] = message_attrs

            # If the exchange is a FIFO topic, then add required MessageGroupId and MessageDeduplicationId attributes
            if exchange.endswith('.fifo'):
                sns_params['MessageGroupId'] = properties.get('MessageGroupId', "default")
                sns_params['MessageDeduplicationId'] = properties.get('MessageDeduplicationId', str(uuid.uuid4()))

        # Add the message to the SNS topic
        self.sns().publish(exchange, message, routing_key, sns_params)

    # —————————————————————————————————————————————————————————————
    # _message_to_python helper methods (extracted for testing/readability)
    # —————————————————————————————————————————————————————————————

    def _optional_b64_decode(self, raw: bytes) -> bytes:
        """Optionally decode a base64 encoded string.

        :param raw: The raw bytes object to decode.
        :return: Bytes of the optionally decoded raw input.
        """
        candidate = raw.strip()

        if self.B64_REGEX.fullmatch(candidate) is None:
            return raw

        try:
            decoded = base64.b64decode(candidate, validate=True)
        except (binascii.Error, ValueError):
            return raw

        reencoded = base64.b64encode(decoded).rstrip(b'=')
        if reencoded != candidate.rstrip(b'='):
            return raw

        try:
            decoded.decode('utf-8')
        except UnicodeDecodeError:
            return raw

        return decoded

    def _decode_python_message_body(self, raw_body):
        """Decode the message body when needed.

        raw_body: bytes or str
        returns: bytes (decoded Base64 if it looks like Base64, otherwise raw bytes)
        """
        b = raw_body.encode() if isinstance(raw_body, str) else raw_body
        return self._optional_b64_decode(b)

    def _prepare_json_payload(self, text):
        """Try to JSON-decode text into a dict; on failure return {}."""
        try:
            data = loads(text)
            return data if isinstance(data, dict) else {}
        except (JSONDecodeError, TypeError):
            return {}

    def _delete_message(self, queue_name, message):
        """Move the message over to the new queue URL and delete it."""
        new_q = self._new_queue(queue_name)
        self.asynsqs(queue=queue_name).delete_message(
            new_q, message['ReceiptHandle']
        )

    def _envelope_payload(self, payload, raw_text, message, q_url):
        """Prepare the payload envelope.

        Ensure we have a dict with 'body' and 'properties.delivery_info',
        then stamp on SQS-specific metadata.

        :param payload: The payload as an object
        :param raw_text: Text that will be set as the payload body.
        :param message: A kombu Message.
        :param q_url: The SQS queue URL.

        :return: Payload object.
        """
        # if payload wasn’t already a Kombu JSON dict, wrap it
        if 'properties' not in payload:
            payload = {
                'body':       raw_text,
                'properties': {'delivery_info': {}},
            }

        props = payload.setdefault('properties', {})
        di = props.setdefault('delivery_info', {})

        # add SQS metadata
        di.update({
            'sqs_message': message,
            'sqs_queue':   q_url,
        })
        props['delivery_tag'] = message['ReceiptHandle']

        return payload


class _SnsFanout:
    """A class to manage AWS Simple Notification Service (SNS) for fanout exchanges. This class maintains caches of
    SNS subscriptions, clients, topic ARNs etc to enable efficient management of SNS topics and subscriptions.
    """
    _predefined_clients = {}  # A client for each predefined queue
    _topic_arn_cache: dict[str: str] = {}  # SNS topic name => Topic queue URL
    _queue_arn_cache: dict[str, str] = {}  # SQS queue URL => Queue ARN
    _subscription_arn_cache: dict[str, str] = {}  # Queue => Subscription ARN
    _exchange_topic_cache: dict[str, str] = {}  # Exchange name => SNS topic ARN

    _lock = threading.Lock()

    def __init__(self, channel: Channel):
        self._chan = channel
        self._client = None

    def initialise_exchange(self, exchange_name: str) -> None:
        """Initialise SNS topic for a fanout exchange.

        This method will create the SNS topic if it doesn't exist, and check for any SNS topic subscriptions
        that no longer exist.

        :param exchange_name: The name of the exchange.
        :returns: None
        """
        # Clear any old subscriptions
        self.remove_stale_sns_subscriptions(exchange_name)

        # If topic has already been initialised, then do nothing
        if self._topic_arn_cache.get(exchange_name):
            return None

        # If predefined_exchanges are set, then do not try to create an SNS topic
        if self._chan.predefined_exchanges:
            logger.debug(f"'predefined_exchanges' has been specified, so SNS topics will not be created.")
            return

        with self._lock:
            # Create the topic and cache the ARN
            self._topic_arn_cache[exchange_name] = self._create_sns_topic(exchange_name)
            return None

    def publish(self, exchange_name: str, message: dict, routing_key, sns_params: dict = None) -> None:
        """Send a notification to AWS Simple Notification Service (SNS).

        :param exchange_name: The name of the exchange.
        :param message: The message to be sent.
        :param routing_key: The routing key for the notification.
        :param sns_params: Additional parameters for SNS notification.
        :return: None
        """
        # Get topic ARN for the given exchange
        topic_arn = self._get_topic_arn(exchange_name)

        # Build request args for boto
        request_args = {
            "TopicArn": topic_arn,
            "Message":  dumps(message),
        }
        request_args.update(sns_params or {})

        # Serialise message attributes into SNS format
        if serialised_attrs := self._serialise_message_attributes(request_args.pop("MessageAttributes", {})):
            request_args["MessageAttributes"] = serialised_attrs

        # Send event to topic
        response = self._get_client(exchange_name).publish(**request_args)
        if (status_code := response["ResponseMetadata"]["HTTPStatusCode"]) != 200:
            raise UndefinedExchangeException(
                f"Unable to send message to topic '{topic_arn}': status code was {status_code}")

    def _get_topic_arn(self, exchange_name: str) -> str:
        """Get the SNS topic ARN

        If the topic ARN is not in the cache, then create it
        :param exchange_name: The exchange to create the SNS topic for
        :return: The SNS topic ARN
        """
        # If topic ARN is in the cache, then return it
        if topic_arn := self._topic_arn_cache.get(exchange_name):
            return topic_arn

        # If predefined-exchanges are used, then do not create a new topic and raise an exception
        if self._chan.predefined_exchanges:
            with self._lock:
                # Try and get the topic ARN from the predefined_exchanges and add it to the cache
                topic_arn = self._topic_arn_cache[exchange_name] = self._chan.predefined_exchanges.get(exchange_name)
                if topic_arn:
                    return topic_arn

            # If pre-defined exchanges do not have the exchange, then raise an exception
            raise UndefinedExchangeException(
                f"Exchange with name '{exchange_name}' must be defined in 'predefined_exchanges'."
            )

        # If predefined_caches are not used, then create a new SNS topic/retrieve the ARN from AWS SNS and cache it
        with self._lock:
            arn = self._topic_arn_cache[exceptions] = self._create_sns_topic(exchange_name)
            return arn

    def _create_sns_topic(self, exchange_name: str) -> str:
        """Creates an AWS SNS topic

        If the topic already exists, AWS will return it's ARN without creating a new one.

        :param exchange_name: The exchange to create the SNS topic for
        :return: Topic ARN
        """
        # Create the SNS topic/Retrieve the SNS topic ARN
        topic_name = self._chan.canonical_queue_name(exchange_name)

        logger.debug(f"Creating SNS topic '{topic_name}'")

        # Call SNS API to create the topic
        response = self._get_client().create_topic(
            Name=topic_name,
            Attributes={
                "FifoTopic": str(topic_name.endswith(".fifo")),

            },
            Tags=[
                {"Key": "ManagedBy", "Value": "Celery/Kombu"},
                {"Key":   "Description",
                 "Value": "This SNS topic is used by Kombu to enable Fanout support for AWS SQS."},
            ],
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise UndefinedExchangeException("Unable to create SNS topic")

        # Extract the ARN from the response
        arn = response["TopicArn"]
        logger.info(f"Created SNS topic '{topic_name}' with ARN '{arn}'")

        return arn

    def _set_permission_on_sqs_queue(self, topic_arn: str, queue_name: str, queue_arn: str):
        """Sets the permissions on an AWS SQS queue to enable the SNS topic to publish to the queue.

        :param topic_arn: The ARN of the SNS topic
        :param queue_name: The queue name to set permissions for
        :param queue_arn: The ARN of the SQS queue
        :return: None
        """
        self._chan.sqs().set_queue_attributes(
            QueueUrl=self._chan._resolve_queue_url(queue_name),
            Attributes={
                "Policy": json.dumps(
                    {
                        "Version":   "2012-10-17",
                        "Statement": [
                            {
                                "Sid":       "KombuManaged",
                                "Effect":    "Allow",
                                "Principal": {"Service": "sns.amazonaws.com"},
                                "Action":    "SQS:SendMessage",
                                "Resource":  queue_arn,
                                "Condition": {"ArnLike": {"aws:SourceArn": topic_arn}},
                            }
                        ],
                    }
                )
            },
        )
        logger.debug(f"Set permissions on SNS topic '{topic_arn}'")

    @staticmethod
    def _serialise_message_attributes(message_attributes: dict) -> dict:
        """Serialises SQS message attributes into SNS format.

        :param message_attributes: A dictionary of message attributes
        :returns: A dictionary of serialised message attributes in SNS format.
        """
        if not message_attributes:
            return {}

        attrs = {}
        for key, value in message_attributes.items():
            attrs[key] = {
                "DataType":    "String",
                "StringValue": str(value),
            }

        return attrs

    def _get_client(self, exchange_name: str | None = None):
        """Get or create a Boto SNS client.

        If an SNS client has already been initialised for this Channel instance, return it. If not, create a new SNS
        client, add it to this Channel instance and return it.

        If the exchange is defined in the predefined_exchanges, then return the client for the exchange and handle
        any STS token renewal.

        :param exchange_name: The name of the exchange
        :returns: A Boto SNS client.
        """
        # Attempt to get pre-=defined client for exchange if it has been provided
        if exchange_name is not None and self._chan.predefined_exchanges:
            # Raise if queue is not defined
            if not (e := self._chan.predefined_exchanges.get(exchange_name)):
                raise UndefinedExchangeException(
                    f"Exchange with name '{exchange_name}' must be defined in 'predefined_exchanges'.")

            # Handle authenticating boto client with tokens
            if self._chan.transport_options.get('sts_role_arn'):
                return self._handle_sts_session(exchange_name, e)

            # If the queue has already been defined, then return the client for the queue
            if exchange_name in self._predefined_clients:
                return self._predefined_clients[exchange_name]

            # Create client, add it to the queue map and return
            c = self._predefined_clients[exchange_name] = self._create_boto_client(
                region=e.get('region', self._chan.region),
                access_key_id=e.get('access_key_id', self._chan.conninfo.userid),
                secret_access_key=e.get('secret_access_key', self._chan.conninfo.password)
            )
            return c

        # If SQS client has been initialised, return it
        if self._client is not None:
            return self._client

        # Initialise a new SQS client and return it
        c = self._client = self._create_boto_client(
            region=self._chan.region,
            access_key_id=self._chan.conninfo.userid,
            secret_access_key=self._chan.conninfo.password,
        )
        return c

    def _handle_sts_session(self, exchange_name: str, e):
        """Checks if the STS token needs renewing for SNS

        :param exchange_name: The exchange name
        :param e: The exchange object
        :returns: The SNS client with a refreshed STS token
        """
        # Check if a token refresh is needed
        if self._chan.is_sts_token_refresh_required(
            item=e, client_map=self._predefined_clients, expire_time=getattr(self, "sts_expiration", None)
        ):
            return self._create_boto_client_with_sts_session(e, region=e.get('region', self._chan.region))

        # If token refresh is not required, return existing client
        return self._predefined_clients[exchange_name]

    def _create_boto_client_with_sts_session(self, exchange: Exchange, region: str):
        # Handle STS token refresh
        sts_creds = self._chan.get_sts_credentials()
        self.sts_expiration = sts_creds['Expiration']

        # Get new client and return it
        c = self._predefined_clients[exchange] = self._create_boto_client(
            region=region,
            access_key_id=sts_creds['AccessKeyId'],
            secret_access_key=sts_creds['SecretAccessKey'],
            session_token=sts_creds['SessionToken'],
        )
        return c

    def _create_boto_client(self, region, access_key_id, secret_access_key, session_token=None):
        """Create a new SNS client.

        :param region: The AWS region to use.
        :param access_key_id: The AWS access key ID for authenticating with boto.
        :param secret_access_key: The AWS secret access key for authenticating with boto.
        :param session_token: The AWS session token for authenticating with boto, if required.
        :returns: A Boto SNS client.
        """
        return self._chan._new_boto_client(
            service='sns', region=region, access_key_id=access_key_id, secret_access_key=secret_access_key,
            session_token=session_token
        )

    def __get_queue_arn(self, queue_name: str) -> str:
        """Returns the ARN of the SQS queue associated with the given queue

        This method will return the ARN from the cache if it exists, otherwise it will fetch it from SQS.

        :param queue_name: The queue to get the ARN for
        """
        # Check if the queue ARN is already cached, and return if it exists
        if arn := self._queue_arn_cache.get(queue_name):
            return arn

        queue_url = self._chan._resolve_queue_url(queue_name)

        # Get the ARN for the SQS queue
        response = self._chan.sqs().get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise UndefinedExchangeException("Unable to get ARN for SQS queue")

        # Update queue ARN cache
        with self._lock:
            arn = self._queue_arn_cache[queue_name] = response["Attributes"]["QueueArn"]

        return arn

    """
    ---------------------------------
    SNS topic subscription management
    ---------------------------------
    """

    def subscribe_queue_to_topic(self, queue_name: str, exchange_name: str) -> str:
        """Subscribes a queue to an AWS SNS topic

        :param queue_name: The queue to subscribe
        :param exchange_name: The exchange to subscribe to the queue, if not provided
        :raises: UndefinedExchangeException if exchange is not defined.
        :return: The subscription ARN
        """
        # Get exchange from Queue and raise if not defined
        cache_key = f"{exchange_name}:{queue_name}"

        # If the subscription ARN is already cached, return it
        if subscription_arn := self._subscription_arn_cache.get(cache_key):
            return subscription_arn

        queue_arn = self.__get_queue_arn(queue_name)
        topic_arn = self._get_topic_arn(exchange_name)

        logger.debug(f"Subscribing queue '{queue_arn}' to SNS topic '{topic_arn}'")

        # Request SNS client to subscribe the queue to the topic
        response = self._get_client().subscribe(
            TopicArn=topic_arn,
            Protocol="sqs",
            Endpoint=queue_arn,
            Attributes={
                "RawMessageDelivery": "true"
            },
            ReturnSubscriptionArn=True
        )
        if (status_code := response["ResponseMetadata"]["HTTPStatusCode"]) != 200:
            raise Exception(f"Unable to subscribe queue: status code was {status_code}")

        # Update subscription ARN cache
        with self._lock:
            subscription_arn = self._subscription_arn_cache[cache_key] = response["SubscriptionArn"]
            logger.info(
                f"Create subscription '{subscription_arn}' for SQS queue '{queue_arn}' to SNS topic '{topic_arn}'")

        # Setup permissions for the queue to receive messages from the topic
        self._set_permission_on_sqs_queue(topic_arn=topic_arn, queue_arn=queue_arn, queue_name=queue_name)

        return subscription_arn

    def unsubscribe_queue_from_topic(self, queue_name: str) -> None:
        """Unsubscribes a queue from an AWS SNS topic

        :param queue_name: The queue to unsubscribe
        :return: None
        """
        # Get subscription ARN from cache if it exists, and return if it exists
        if not (subscription_arn := self._subscription_arn_cache.get(queue_name)):
            return

        # Unsubscribe the SQS queue from the SNS topic
        self._unsubscribe_sns_subscription(subscription_arn)
        logger.info(f"Unsubscribed subscription '{subscription_arn}' for SQS queue '{queue_name}'")

    def remove_stale_sns_subscriptions(self, exchange_name: str) -> None:
        """Removes any stale SNS topic subscriptions

        This method will check that any SQS subscriptions on the SNS topic are associated with SQS queues. If not,
        it will remove the stale subscription.

        :param exchange_name: The exchange to check for stale subscriptions
        :return: None
        """
        # If predefined_exchanges are set, then do not try to remove subscriptions
        if self._chan.predefined_exchanges:
            logger.debug(
                f"'predefined_exchanges' has been specified, so stale SNS subscription cleanup will be skipped.")
            return

        logger.debug(f"Checking for stale SNS subscriptions for exchange '{exchange_name}'")

        # Get subscriptions to check
        topic_arn = self._get_topic_arn(exchange_name)

        # Iterate through the subscriptions and remove any that are not associated with SQS queues
        for subscription_arn in self._get_invalid_sns_subscriptions(topic_arn):
            # Unsubscribe the SQS queue from the SNS topic
            try:
                self._unsubscribe_sns_subscription(subscription_arn)
                logger.info(f"Removed stale subscription '{subscription_arn}' for SNS topic '{topic_arn}'")

            # Report any failures to remove the subscription and continue to the next as this is not a critical error
            except Exception as e:
                logger.warning(
                    f"Failed to remove stale subscription '{subscription_arn}' for SNS topic '{topic_arn}': {e}")

    def _unsubscribe_sns_subscription(self, subscription_arn: str) -> None:
        """Unsubscribes a subscription from an AWS SNS topic

        :param subscription_arn: The ARN of the subscription to unsubscribe
        :return: None
        """
        response = self._get_client().unsubscribe(SubscriptionArn=subscription_arn)
        if (status_code := response["ResponseMetadata"]["HTTPStatusCode"]) != 200:
            logger.error(f"Unable to unsubscribe queue: status code was {status_code}")
            return

    def _get_invalid_sns_subscriptions(self, sns_topic_arn: str) -> list[str]:
        """Get a list of all invalid SQS subscriptions associated with a given SNS topic

        :param sns_topic_arn: The SNS topic ARN to check
        :return: A list of SNS subscription ARNs that are invalid
        """
        paginator = self._get_client().get_paginator("list_subscriptions_by_topic")
        req_args = {
            "TopicArn": sns_topic_arn,
        }

        # Iterate through the paginated subscriptions and build a list of subscriptions to check
        invalid_subscription_arns = []
        for response in paginator.paginate(**req_args):
            invalid_subscription_arns.extend(
                self._filter_sns_subscription_response(response.get("Subscriptions"))
            )

        return invalid_subscription_arns

    def _filter_sns_subscription_response(self, subscriptions: list[dict]) -> list[str]:
        """Returns a list of SNS subscription ARNs that are not associated with a SQS queue

        :param subscriptions: A list of subscriptions for an SNS topic
        :return: A list of subscription ARNs that are dead
        """
        subscription_arns = []
        for subscription in subscriptions:
            # Skip subscription if it is not for SQS
            if not subscription.get("Protocol", "").lower() == "sqs":
                continue

            # Extract the SQS queue ARN from the subscription endpoint
            queue_name = subscription["Endpoint"].split(":")[-1]

            # Check if the queue has been removed by calling the get queue URL method. Note: listing the queues sometimes
            # results in a valid queue not being returned (due to eventual consistency in SQS), so calling this method
            # helps to mitigate this.
            try:
                self._chan.sqs().get_queue_url(QueueName=queue_name)
            except ClientError as e:
                queue_missing_errs = ["QueueDoesNotExist", "NonExistentQueue"]
                # If one of the errors above has been raised, then the queue has been removed and the subscription should
                # be removed too.
                if any(err in str(e) for err in queue_missing_errs):
                    subscription_arns.append(subscription["SubscriptionArn"])
                else:
                    raise
        return subscription_arns


class Transport(virtual.Transport):
    """SQS Transport.

    Additional queue attributes can be supplied to SQS during queue
    creation by passing an ``sqs-creation-attributes`` key in
    transport_options. ``sqs-creation-attributes`` must be a dict whose
    key-value pairs correspond with Attributes in the
    `CreateQueue SQS API`_.

    For example, to have SQS queues created with server-side encryption
    enabled using the default Amazon Managed Customer Master Key, you
    can set ``KmsMasterKeyId`` Attribute. When the queue is initially
    created by Kombu, encryption will be enabled.

    .. code-block:: python

        from kombu.transport.SQS import Transport

        transport = Transport(
            ...,
            transport_options={
                'sqs-creation-attributes': {
                    'KmsMasterKeyId': 'alias/aws/sqs',
                },
            }
        )

    .. _CreateQueue SQS API: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html#API_CreateQueue_RequestParameters

    .. versionadded:: 5.6
    Queue tags can be applied to SQS queues during creation by passing an
    ``queue_tags`` key in transport_options. ``queue_tags`` must be
    a dict of tag key-value pairs.

    .. code-block:: python

        from kombu.transport.SQS import Transport

        transport = Transport(
            ...,
            transport_options={
                'queue_tags': {
                    'Environment': 'production',
                    'Team': 'backend',
                },
            }
        )

    The ``ApproximateReceiveCount`` message attribute is fetched by this
    transport by default. Requested message attributes can be changed by
    setting ``fetch_message_attributes`` in the transport options.

    .. code-block:: python

        from kombu.transport.SQS import Transport

        transport = Transport(
            ...,
            transport_options={
                'fetch_message_attributes': ["All"],  # Get all of the MessageSystemAttributeNames (formerly AttributeNames)
            }
        )
        # Preferred - A dict specifying system and custom message attributes
        transport = Transport(
            ...,
            transport_options={
                'fetch_message_attributes': {
                    'MessageSystemAttributeNames': ["SenderId", "SentTimestamp"],
                    'MessageAttributeNames': ['S3MessageBodyKey']
                },
            }
        )
    .. _Message Attributes: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#SQS-ReceiveMessage-request-AttributeNames

    """  # noqa: E501

    Channel = Channel

    polling_interval = 1
    wait_time_seconds = 0
    default_port = None
    connection_errors = (
        virtual.Transport.connection_errors +
        (exceptions.BotoCoreError, socket.error)
    )
    channel_errors = (
        virtual.Transport.channel_errors + (exceptions.BotoCoreError,)
    )
    driver_type = 'sqs'
    driver_name = 'sqs'

    implements = virtual.Transport.implements.extend(
        asynchronous=True,
        exchange_type=frozenset(['direct']),
    )

    @property
    def default_connection_params(self):
        return {'port': self.default_port}
