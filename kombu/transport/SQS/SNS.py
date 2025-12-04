"""Amazon SNS fanout support for the AWS SQS transport module for Kombu.

This module provides a `SNS` class that can be used to manage SNS topics and subscriptions.
It's primarily used to provide fanout support via AWS Simple Notification Service (SNS)
topics and subscriptions. The module also provides methods for handling the lifecycle
of these topics.
"""
from __future__ import annotations

import json
import threading
from datetime import datetime
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from kombu.exceptions import KombuError
from kombu.log import get_logger

from .exceptions import UndefinedExchangeException

# pragma: no branch
if TYPE_CHECKING:
    from . import Channel

logger = get_logger(__name__)


class SNS:
    """A class to manage AWS Simple Notification Service (SNS) for fanout exchanges.

    This class maintains caches of SNS subscriptions, clients, topic ARNs etc to
    enable efficient management of SNS topics and subscriptions.
    """
    def __init__(self, channel: Channel):
        self.channel = channel
        self._client = None
        self.subscriptions = _SnsSubscription(self)
        self._predefined_clients = {}  # A client for each predefined queue
        self._topic_arn_cache: dict[str, str] = {}  # SNS topic name => Topic ARN
        self._exchange_topic_cache: dict[str, str] = {}  # Exchange name => SNS topic ARN
        self.sts_expiration: datetime | None = None  # Cached STS expiration time
        self._lock = threading.Lock()

    def initialise_exchange(self, exchange_name: str) -> None:
        """Initialise SNS topic for a fanout exchange.

        This method will create the SNS topic if it doesn't exist, and check for any SNS topic subscriptions
        that no longer exist.

        :param exchange_name: The name of the exchange.
        :returns: None
        """
        # Clear any old subscriptions
        self.subscriptions.cleanup(exchange_name)

        # If topic has already been initialised, then do nothing
        if self._topic_arn_cache.get(exchange_name):
            return None

        # If predefined_exchanges are set, then do not try to create an SNS topic
        if self.channel.predefined_exchanges:
            logger.debug(
                "'predefined_exchanges' has been specified, so SNS topics will"
                " not be created."
            )
            return

        with self._lock:
            # Create the topic and cache the ARN
            self._topic_arn_cache[exchange_name] = self._create_sns_topic(exchange_name)
            return None

    def publish(
        self,
        exchange_name: str,
        message: str,
        message_attributes: dict = None,
        request_params: dict = None,
    ) -> None:
        """Send a notification to AWS Simple Notification Service (SNS).

        :param exchange_name: The name of the exchange.
        :param message: The message to be sent as a JSON string
        :param message_attributes: Attributes for the message.
        :param request_params: Additional parameters for SNS notification.
        :return: None
        """
        # Get topic ARN for the given exchange
        topic_arn = self._get_topic_arn(exchange_name)

        # Build request args for boto
        request_args: dict[str, str | dict] = {
            "TopicArn": topic_arn,
            "Message":  message,
        }
        request_args.update(request_params or {})

        # Serialise message attributes into SNS format
        if serialised_attrs := self.serialise_message_attributes(message_attributes):
            request_args["MessageAttributes"] = serialised_attrs

        # Send event to topic
        response = self.get_client(exchange_name).publish(**request_args)
        if (status_code := response["ResponseMetadata"]["HTTPStatusCode"]) != 200:
            raise UndefinedExchangeException(
                f"Unable to send message to topic '{topic_arn}': status code was {status_code}"
            )

    def _get_topic_arn(self, exchange_name: str) -> str:
        """Get the SNS topic ARN.

        If the topic ARN is not in the cache, then create it
        :param exchange_name: The exchange to create the SNS topic for
        :return: The SNS topic ARN
        """
        # If topic ARN is in the cache, then return it
        if topic_arn := self._topic_arn_cache.get(exchange_name):
            return topic_arn

        # If predefined-exchanges are used, then do not create a new topic and raise an exception
        if self.channel.predefined_exchanges:
            with self._lock:
                # Try and get the topic ARN from the predefined_exchanges and add it to the cache
                topic_arn = self._topic_arn_cache[exchange_name] = (
                    self.channel.predefined_exchanges.get(exchange_name, {}).get("arn")
                )
                if topic_arn:
                    return topic_arn

            # If pre-defined exchanges do not have the exchange, then raise an exception
            raise UndefinedExchangeException(
                f"Exchange with name '{exchange_name}' must be defined in 'predefined_exchanges'."
            )

        # If predefined_caches are not used, then create a new SNS topic/retrieve the ARN from AWS SNS and cache it
        with self._lock:
            arn = self._topic_arn_cache[exchange_name] = self._create_sns_topic(
                exchange_name
            )
            return arn

    def _create_sns_topic(self, exchange_name: str) -> str:
        """Creates an AWS SNS topic.

        If the topic already exists, AWS will return it's ARN without creating a new one.

        :param exchange_name: The exchange to create the SNS topic for
        :return: Topic ARN
        """
        # Create the SNS topic/Retrieve the SNS topic ARN
        topic_name = self.channel.canonical_queue_name(exchange_name)

        logger.debug(f"Creating SNS topic '{topic_name}'")

        # Call SNS API to create the topic
        response = self.get_client().create_topic(
            Name=topic_name,
            Attributes={
                "FifoTopic": str(topic_name.endswith(".fifo")),
            },
            Tags=[
                {"Key": "ManagedBy", "Value": "Celery/Kombu"},
                {
                    "Key":   "Description",
                    "Value": "This SNS topic is used by Kombu to enable Fanout support for AWS SQS.",
                },
            ],
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise UndefinedExchangeException("Unable to create SNS topic")

        # Extract the ARN from the response
        arn = response["TopicArn"]
        logger.info(f"Created SNS topic '{topic_name}' with ARN '{arn}'")

        return arn

    @staticmethod
    def serialise_message_attributes(message_attributes: dict) -> dict:
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

    def get_client(self, exchange_name: str | None = None):
        """Get or create a Boto SNS client.

        If an SNS client has already been initialised for this Channel instance, return it. If not, create a new SNS
        client, add it to this Channel instance and return it.

        If the exchange is defined in the predefined_exchanges, then return the client for the exchange and handle
        any STS token renewal.

        :param exchange_name: The name of the exchange
        :returns: A Boto SNS client.
        """
        # Attempt to get predefined client for exchange if it has been provided
        if exchange_name is not None and self.channel.predefined_exchanges:
            # Raise if queue is not defined
            if not (e := self.channel.predefined_exchanges.get(exchange_name)):
                raise UndefinedExchangeException(
                    f"Exchange with name '{exchange_name}' must be defined in 'predefined_exchanges'."
                )

            # Handle authenticating boto client with tokens
            if self.channel.transport_options.get("sts_role_arn"):
                return self._handle_sts_session(exchange_name, e)

            # If the queue has already been defined, then return the client for the queue
            if c := self._predefined_clients.get(exchange_name):
                return c

            # Create client, add it to the queue map and return
            c = self._predefined_clients[exchange_name] = self._create_boto_client(
                region=e.get("region", self.channel.region),
                access_key_id=e.get("access_key_id", self.channel.conninfo.userid),
                secret_access_key=e.get(
                    "secret_access_key", self.channel.conninfo.password
                ),
            )
            return c

        # If SQS client has been initialised, return it
        if self._client is not None:
            return self._client

        # Initialise a new SQS client and return it
        c = self._client = self._create_boto_client(
            region=self.channel.region,
            access_key_id=self.channel.conninfo.userid,
            secret_access_key=self.channel.conninfo.password,
        )
        return c

    def _handle_sts_session(self, exchange_name: str, e: dict):
        """Checks if the STS token needs renewing for SNS.

        :param exchange_name: The exchange name
        :param e: The exchange object
        :returns: The SNS client with a refreshed STS token
        """
        # Check if a token refresh is needed
        if self.channel.is_sts_token_refresh_required(
            name=exchange_name,
            client_map=self._predefined_clients,
            expire_time=self.sts_expiration,
        ):
            return self._create_boto_client_with_sts_session(
                exchange_name, region=e.get("region", self.channel.region)
            )

        # If token refresh is not required, return existing client
        return self._predefined_clients[exchange_name]

    def _create_boto_client_with_sts_session(self, exchange_name: str, region: str):
        """Creates a new SNS client with a refreshed STS token.

        :param exchange_name: The exchange name
        :param region: The AWS region to use.
        :returns: The SNS client with a refreshed STS token.
        """
        # Handle STS token refresh
        sts_creds = self.channel.get_sts_credentials()
        self.sts_expiration = sts_creds["Expiration"]

        # Get new client and return it
        c = self._predefined_clients[exchange_name] = self._create_boto_client(
            region=region,
            access_key_id=sts_creds["AccessKeyId"],
            secret_access_key=sts_creds["SecretAccessKey"],
            session_token=sts_creds["SessionToken"],
        )
        return c

    def _create_boto_client(
        self, region, access_key_id, secret_access_key, session_token=None
    ):
        """Create a new SNS client.

        :param region: The AWS region to use.
        :param access_key_id: The AWS access key ID for authenticating with boto.
        :param secret_access_key: The AWS secret access key for authenticating with boto.
        :param session_token: The AWS session token for authenticating with boto, if required.
        :returns: A Boto SNS client.
        """
        return self.channel._new_boto_client(
            service="sns",
            region=region,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
        )

    # ---------------------------------
    # SNS topic subscription management
    # ---------------------------------


class _SnsSubscription:
    _queue_arn_cache: dict[str, str] = {}  # SQS queue URL => Queue ARN
    _subscription_arn_cache: dict[str, str] = {}  # Queue => Subscription ARN

    _lock = threading.Lock()

    def __init__(self, sns_fanout: SNS):
        self.sns = sns_fanout

    def subscribe_queue(self, queue_name: str, exchange_name: str) -> str:
        """Subscribes a queue to an AWS SNS topic.

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

        # Get ARNs for queue and topic
        queue_arn = self._get_queue_arn(queue_name)
        topic_arn = self.sns._get_topic_arn(exchange_name)

        # Subscribe the SQS queue to the SNS topic
        subscription_arn = self._subscribe_queue_to_sns_topic(
            queue_arn=queue_arn, topic_arn=topic_arn
        )

        # Setup permissions for the queue to receive messages from the topic
        self._set_permission_on_sqs_queue(
            topic_arn=topic_arn, queue_arn=queue_arn, queue_name=queue_name
        )

        # Update subscription ARN cache
        with self._lock:
            self._subscription_arn_cache[cache_key] = subscription_arn

        return subscription_arn

    def unsubscribe_queue(self, queue_name: str, exchange_name: str) -> None:
        """Unsubscribes a queue from an AWS SNS topic.

        :param queue_name: The queue to unsubscribe
        :param exchange_name: The exchange to unsubscribe from the queue, if not provided
        :return: None
        """
        cache_key = f"{exchange_name}:{queue_name}"
        # Get subscription ARN from cache if it exists, and return if it exists
        if not (subscription_arn := self._subscription_arn_cache.get(cache_key)):
            return

        # Unsubscribe the SQS queue from the SNS topic
        self._unsubscribe_sns_subscription(subscription_arn)
        logger.info(
            f"Unsubscribed subscription '{subscription_arn}' for SQS queue '{queue_name}'"
        )

    def cleanup(self, exchange_name: str) -> None:
        """Removes any stale SNS topic subscriptions.

        This method will check that any SQS subscriptions on the SNS topic are associated with SQS queues. If not,
        it will remove the stale subscription.

        :param exchange_name: The exchange to check for stale subscriptions
        :return: None
        """
        # If predefined_exchanges are set, then do not try to remove subscriptions
        if self.sns.channel.predefined_exchanges:
            logger.debug(
                "'predefined_exchanges' has been specified, so stale SNS subscription cleanup will be skipped."
            )
            return

        logger.debug(
            f"Checking for stale SNS subscriptions for exchange '{exchange_name}'"
        )

        # Get subscriptions to check
        topic_arn = self.sns._get_topic_arn(exchange_name)

        # Iterate through the subscriptions and remove any that are not associated with SQS queues
        for subscription_arn in self._get_invalid_sns_subscriptions(topic_arn):
            # Unsubscribe the SQS queue from the SNS topic
            try:
                self._unsubscribe_sns_subscription(subscription_arn)
                logger.info(
                    f"Removed stale subscription '{subscription_arn}' for SNS topic '{topic_arn}'"
                )

            # Report any failures to remove the subscription and continue to the next as this is not a critical error
            except Exception as e:
                logger.warning(
                    f"Failed to remove stale subscription '{subscription_arn}' for SNS topic '{topic_arn}': {e}"
                )

    def _subscribe_queue_to_sns_topic(self, queue_arn: str, topic_arn: str) -> str:
        """Subscribes a queue to an AWS SNS topic.

        :param queue_arn: The ARN of the queue to subscribe
        :param topic_arn: The ARN of the SNS topic to subscribe to
        :raises: UndefinedExchangeException if exchange is not defined.
        :return: The subscription ARN
        """
        logger.debug(f"Subscribing queue '{queue_arn}' to SNS topic '{topic_arn}'")

        # Request SNS client to subscribe the queue to the topic
        response = self.sns.get_client().subscribe(
            TopicArn=topic_arn,
            Protocol="sqs",
            Endpoint=queue_arn,
            Attributes={"RawMessageDelivery": "true"},
            ReturnSubscriptionArn=True,
        )
        if (status_code := response["ResponseMetadata"]["HTTPStatusCode"]) != 200:
            raise Exception(f"Unable to subscribe queue: status code was {status_code}")

        # Extract the subscription ARN from the response and log
        subscription_arn = response["SubscriptionArn"]
        logger.info(
            f"Create subscription '{subscription_arn}' for SQS queue '{queue_arn}' to"
            f" SNS topic '{topic_arn}'"
        )

        return subscription_arn

    def _set_permission_on_sqs_queue(
        self, topic_arn: str, queue_name: str, queue_arn: str
    ):
        """Sets the permissions on an AWS SQS queue to enable the SNS topic to publish to the queue.

        :param topic_arn: The ARN of the SNS topic
        :param queue_name: The queue name to set permissions for
        :param queue_arn: The ARN of the SQS queue
        :return: None
        """
        self.sns.channel.sqs().set_queue_attributes(
            QueueUrl=self.sns.channel._resolve_queue_url(queue_name),
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

    def _unsubscribe_sns_subscription(self, subscription_arn: str) -> None:
        """Unsubscribes a subscription from an AWS SNS topic.

        :param subscription_arn: The ARN of the subscription to unsubscribe
        :return: None
        """
        response = self.sns.get_client().unsubscribe(SubscriptionArn=subscription_arn)
        if (status_code := response["ResponseMetadata"]["HTTPStatusCode"]) != 200:
            logger.error(
                f"Unable to remove subscription '{subscription_arn}': status code was {status_code}"
            )

    def _get_invalid_sns_subscriptions(self, sns_topic_arn: str) -> list[str]:
        """Get a list of all invalid SQS subscriptions associated with a given SNS topic.

        :param sns_topic_arn: The SNS topic ARN to check
        :return: A list of SNS subscription ARNs that are invalid
        """
        paginator = self.sns.get_client().get_paginator("list_subscriptions_by_topic")

        # Iterate through the paginated subscriptions and build a list of subscriptions to check
        invalid_subscription_arns = []
        for response in paginator.paginate(TopicArn=sns_topic_arn):
            invalid_subscription_arns.extend(
                self._filter_sns_subscription_response(response.get("Subscriptions"))
            )

        return invalid_subscription_arns

    def _filter_sns_subscription_response(self, subscriptions: list[dict]) -> list[str]:
        """Returns a list of SNS subscription ARNs that are not associated with a SQS queue.

        :param subscriptions: A list of subscriptions for an SNS topic
        :return: A list of subscription ARNs that are dead
        """
        subscription_arns = []

        # If the subscriptions list is empty or None, return an empty list
        if not subscriptions:
            return subscription_arns

        # Iterate through the subscriptions and check if the queue is valid
        for subscription in subscriptions:
            # Skip subscription if it is not for SQS
            if not subscription.get("Protocol", "").lower() == "sqs":
                continue

            # Extract the SQS queue ARN from the subscription endpoint
            queue_name = subscription["Endpoint"].split(":")[-1]

            # Check if the queue has been removed by calling the get queue URL method.
            # Note: listing the queues sometimes results in a valid queue not being
            # returned (due to eventual consistency in SQS), so calling this method
            # helps to mitigate this.
            try:
                self.sns.channel.sqs().get_queue_url(QueueName=queue_name)
            except ClientError as e:
                queue_missing_errs = ["QueueDoesNotExist", "NonExistentQueue"]
                # If one of the errors above has been raised, then the queue has been
                # removed and the subscription should be removed too.
                if any(err in str(e) for err in queue_missing_errs):
                    subscription_arns.append(subscription["SubscriptionArn"])
                else:
                    raise

        return subscription_arns

    def _get_queue_arn(self, queue_name: str) -> str:
        """Returns the ARN of the SQS queue associated with the given queue.

        This method will return the ARN from the cache if it exists, otherwise it will fetch it from SQS.

        :param queue_name: The queue to get the ARN for
        """
        # Check if the queue ARN is already cached, and return if it exists
        if arn := self._queue_arn_cache.get(queue_name):
            return arn

        queue_url = self.sns.channel._resolve_queue_url(queue_name)

        # Get the ARN for the SQS queue
        response = self.sns.channel.sqs().get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )
        if (status_code := response["ResponseMetadata"]["HTTPStatusCode"]) != 200:
            raise KombuError(
                f"Unable to get ARN for SQS queue '{queue_name}': "
                f"status code was '{status_code}'"
            )

        # Update queue ARN cache
        with self._lock:
            arn = self._queue_arn_cache[queue_name] = response["Attributes"]["QueueArn"]

        return arn
