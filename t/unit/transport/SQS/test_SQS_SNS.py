"""Testing module for the kombu.transport.SQS package.

NOTE: The SQSQueueMock and SQSConnectionMock classes originally come from
http://github.com/pcsforeducation/sqs-mock-python. They have been patched
slightly.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from kombu import Exchange, Queue
from kombu.exceptions import KombuError
from kombu.transport.SQS import SnsFanout
from kombu.transport.SQS.exceptions import (
    UnableToUnsubscribeQueueFromTopicException, UndefinedExchangeException)

boto3 = pytest.importorskip('boto3')

from botocore.exceptions import ClientError  # noqa

from kombu.transport import SQS  # noqa

SQS_Channel_sqs = SQS.Channel.sqs

example_predefined_queues = {
    'queue-1':      {
        'url':               'https://sqs.us-east-1.amazonaws.com/xxx/queue-1',
        'access_key_id':     'a',
        'secret_access_key': 'b',
        'backoff_tasks':     ['svc.tasks.tasks.task1'],
        'backoff_policy':    {1: 10, 2: 20, 3: 40, 4: 80, 5: 320, 6: 640}
    },
    'queue-2':      {
        'url':               'https://sqs.us-east-1.amazonaws.com/xxx/queue-2',
        'access_key_id':     'c',
        'secret_access_key': 'd',
    },
    "queue-3.fifo": {
        "url":               "https://sqs.us-east-1.amazonaws.com/xxx/queue-3.fifo",
        "access_key_id":     "e",
        "secret_access_key": "f",
    },
}


class test_SNS:
    @pytest.fixture
    def mock_sts_credentials(self):
        return {
            "AccessKeyId":     "test_access_key",
            "SecretAccessKey": "test_secret_key",
            "SessionToken":    "test_session_token",
            "Expiration":      datetime.now(timezone.utc) + timedelta(hours=1),
        }

    def test_initialise_exchange_with_existing_topic(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        sns_fanout._topic_arn_cache[exchange_name] = "existing_arn"
        sns_fanout.subscriptions = Mock()

        # Act
        result = sns_fanout.initialise_exchange(exchange_name)

        # Assert
        assert result is None
        assert sns_fanout.subscriptions.cleanup.call_args_list == [call(exchange_name)]
        assert sns_fanout._topic_arn_cache[exchange_name] == "existing_arn"

    def test_initialise_exchange_create_new_topic(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"

        sns_fanout.channel.predefined_exchanges = {}
        sns_fanout.subscriptions = Mock()
        sns_fanout._create_sns_topic = Mock(return_value="new_arn")

        # Act
        result = sns_fanout.initialise_exchange(exchange_name)

        # Assert
        assert result is None
        assert sns_fanout.subscriptions.cleanup.call_args_list == [call(exchange_name)]
        assert sns_fanout._create_sns_topic.call_args_list == [call(exchange_name)]
        assert sns_fanout._topic_arn_cache[exchange_name] == "new_arn"

    def test_get_or_create_topic_create_new_topic(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"

        sns_fanout.channel.predefined_exchanges = {}
        sns_fanout._create_sns_topic = Mock(return_value="new_arn")

        # Act
        result = sns_fanout._get_or_create_topic(exchange_name)

        # Assert
        assert result == "new_arn"
        assert sns_fanout._create_sns_topic.call_args_list == [call(exchange_name)]
        assert sns_fanout._topic_arn_cache[exchange_name] == "new_arn"

    def test_get_or_create_topic_predefined_exchange_found(self, sns_fanout):
        # Arrange
        exchange_name = "exchange-1"

        sns_fanout.channel.predefined_exchanges = {
            "exchange-1": {"arn": "some-existing-arn"}
        }
        sns_fanout._create_sns_topic = Mock(return_value="new_arn")

        # Act
        result = sns_fanout._get_or_create_topic(exchange_name)

        # Assert
        assert result == "some-existing-arn"
        assert sns_fanout._create_sns_topic.call_count == 0
        assert sns_fanout._topic_arn_cache[exchange_name] == "some-existing-arn"

    def test_get_or_create_topic_predefined_exchange_not_found(self, sns_fanout):
        # Arrange
        exchange_name = "exchange-2"

        sns_fanout.channel.predefined_exchanges = {
            "exchange-1": {"arn": "some-existing-arn"}
        }
        sns_fanout._create_sns_topic = Mock(return_value="new_arn")

        # Act
        with pytest.raises(
            UndefinedExchangeException,
            match="Exchange with name 'exchange-2' must be defined in 'predefined_exchanges'.",
        ):
            sns_fanout._get_or_create_topic(exchange_name)

        # Assert
        assert sns_fanout._create_sns_topic.call_count == 0
        assert sns_fanout._topic_arn_cache.get(exchange_name) is None

    def test_publish_successful(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        message = "test_message"

        sns_fanout._topic_arn_cache[exchange_name] = "existing_arn"
        mock_client = Mock()
        mock_client.publish.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        sns_fanout.get_client = Mock(return_value=mock_client)

        # Act
        sns_fanout.publish(exchange_name, message)

        # Assert
        assert sns_fanout.get_client.call_args_list == [call(exchange_name)]
        assert mock_client.publish.call_args_list == [
            call(TopicArn="existing_arn", Message="test_message")
        ]

    def test_publish_with_attributes_and_params(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        message = "test_message"
        message_attributes = {"attr1": "value1", "attr2": 123, "A boolean?": True}
        request_params = {"param1": "value1"}

        sns_fanout._topic_arn_cache[exchange_name] = "existing_arn"
        mock_client = Mock()
        mock_client.publish.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        sns_fanout.get_client = Mock(return_value=mock_client)

        # Act
        sns_fanout.publish(exchange_name, message, message_attributes, request_params)

        # Assert
        assert sns_fanout.get_client.call_args_list == [((exchange_name,), {})]
        assert mock_client.publish.call_args_list == [
            call(
                TopicArn="existing_arn",
                Message="test_message",
                param1="value1",
                MessageAttributes={
                    "attr1":      {"DataType": "String", "StringValue": "value1"},
                    "attr2":      {"DataType": "String", "StringValue": "123"},
                    "A boolean?": {"DataType": "String", "StringValue": "True"},
                },
            )
        ]

    def test_publish_failure(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        message = "test_message"

        sns_fanout._topic_arn_cache[exchange_name] = "existing_arn"

        mock_client = Mock()
        mock_client.publish.return_value = {"ResponseMetadata": {"HTTPStatusCode": 400}}
        sns_fanout.get_client = Mock(return_value=mock_client)

        # Act and Assert
        with pytest.raises(
            UndefinedExchangeException,
            match="Unable to send message to topic 'existing_arn': status code was 400",
        ):
            sns_fanout.publish("test_exchange", message)

        assert sns_fanout.get_client.call_args_list == [call(exchange_name)]
        assert mock_client.publish.call_args_list == [
            call(TopicArn="existing_arn", Message="test_message")
        ]

    def test_create_sns_topic_success(self, sns_fanout, caplog):
        # Arrange
        caplog.set_level(logging.DEBUG)
        sns_fanout.get_client = Mock()
        mock_client = sns_fanout.get_client.return_value
        mock_client.create_topic.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "TopicArn":         "arn:aws:sns:us-east-1:123456789012:my-new-topic",
        }

        # Act
        result = sns_fanout._create_sns_topic("my-new-topic")

        # Assert
        assert result == "arn:aws:sns:us-east-1:123456789012:my-new-topic"
        assert mock_client.create_topic.call_args_list == [
            call(
                Name="my-new-topic",
                Attributes={"FifoTopic": "false"},
                Tags=[
                    {"Key": "ManagedBy", "Value": "Celery/Kombu"},
                    {
                        "Key":   "Description",
                        "Value": "This SNS topic is used by Kombu to enable Fanout support for AWS SQS.",
                    },
                ],
            )
        ]
        assert "Creating SNS topic 'my-new-topic'" in caplog.text
        assert (
            "Created SNS topic 'my-new-topic' with ARN 'arn:aws:sns:us-east-1:123456789012:my-new-topic'"
            in caplog.text
        )

    def test_create_sns_topic_failure(self, sns_fanout):
        # Arrange
        sns_fanout.get_client = Mock()
        mock_client = sns_fanout.get_client.return_value
        mock_client.create_topic.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 400}
        }

        # Act and Assert
        with pytest.raises(
            UndefinedExchangeException, match="Unable to create SNS topic"
        ):
            sns_fanout._create_sns_topic("test_exchange")

    def test_create_sns_topic_fifo(self, sns_fanout, caplog):
        # Arrange
        caplog.set_level(logging.DEBUG)
        sns_fanout.get_client = Mock()
        mock_client = sns_fanout.get_client.return_value
        mock_client.create_topic.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "TopicArn":         "arn:aws:sns:us-east-1:123456789012:test_topic.fifo",
        }

        # Act
        result = sns_fanout._create_sns_topic("test_topic.fifo")

        # Assert
        assert result == "arn:aws:sns:us-east-1:123456789012:test_topic.fifo"
        assert mock_client.create_topic.call_args_list == [
            call(
                Name="test_topic.fifo",
                Attributes={"FifoTopic": "true"},
                Tags=[
                    {"Key": "ManagedBy", "Value": "Celery/Kombu"},
                    {
                        "Key":   "Description",
                        "Value": "This SNS topic is used by Kombu to enable Fanout support for AWS SQS.",
                    },
                ],
            )
        ]
        assert "Creating SNS topic 'test_topic.fifo'" in caplog.text
        assert (
            "Created SNS topic 'test_topic.fifo' with ARN 'arn:aws:sns:us-east-1:123456789012:test_topic.fifo'"
            in caplog.text
        )

    def test_get_client_predefined_exchange(self, sns_fanout):
        # Arrange
        sns_fanout.channel.predefined_exchanges = {
            "test_exchange": {"region": "us-west-2"}
        }
        sns_fanout._create_boto_client = Mock()

        # Act
        result = sns_fanout.get_client("test_exchange")

        # Assert
        assert result == sns_fanout._create_boto_client.return_value
        assert sns_fanout._create_boto_client.call_args_list == [
            call(region="us-west-2", access_key_id=None, secret_access_key=None)
        ]

    def test_get_client_undefined_exchange(self, sns_fanout):
        # Arrange
        sns_fanout.channel.predefined_exchanges = {"exchange-1": {}}

        # Act & Assert
        with pytest.raises(
            UndefinedExchangeException,
            match="Exchange with name 'test_exchange' must be defined in 'predefined_exchanges'.",
        ):
            sns_fanout.get_client("test_exchange")

    def test_get_client_sts_session(self, sns_fanout):
        # Arrange
        sns_fanout.channel.predefined_exchanges = {
            "test_exchange": {
                "arn": "test_arn",
            }
        }
        sns_fanout.channel.connection.client.transport_options = {
            "sts_role_arn": "test_arn"
        }
        sns_fanout._handle_sts_session = Mock()

        # Act
        result = sns_fanout.get_client("test_exchange")

        # Assert
        assert result == sns_fanout._handle_sts_session.return_value
        assert sns_fanout._handle_sts_session.call_args_list == [
            call("test_exchange", {"arn": "test_arn"})
        ]

    def test_get_client_existing_predefined_client(self, sns_fanout):
        # Arrange
        sns_fanout.channel.predefined_exchanges = {
            "test_exchange": {
                "arn": "test_arn",
            }
        }
        client_mock = Mock()
        sns_fanout._predefined_clients = {"test_exchange": client_mock}

        # Act
        result = sns_fanout.get_client("test_exchange")

        # Assert
        assert result is client_mock

    def test_get_client_existing_client(self, sns_fanout):
        # Arrange
        sns_fanout._client = Mock()

        # Act
        result = sns_fanout.get_client()

        # Assert
        assert result == sns_fanout._client

    def test_get_client_new_client(self, sns_fanout):
        # Arrange
        sns_fanout._create_boto_client = Mock()
        sns_fanout.channel.conninfo.userid = "MyAccessKeyID"
        sns_fanout.channel.conninfo.password = "MySecretAccessKey"

        # Act
        result = sns_fanout.get_client()

        # Assert
        assert result == sns_fanout._create_boto_client.return_value
        assert (
            sns_fanout._create_boto_client.call_args_list
            == [
                call(
                    region="some-aws-region",
                    access_key_id="MyAccessKeyID",
                    secret_access_key="MySecretAccessKey",
                )
            ]
        )

    def test_token_refresh_required_no_date(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        exchange_config = {"region": "us-west-2"}

        create_session_mock = Mock()
        sns_fanout._create_boto_client_with_sts_session = create_session_mock

        # Act
        result = sns_fanout._handle_sts_session(exchange_name, exchange_config)

        # Assert
        assert result == sns_fanout._create_boto_client_with_sts_session.return_value
        assert create_session_mock.call_args_list == [
            call("test_exchange", region="us-west-2")
        ]

    def test_token_refresh_required_expired_date(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        exchange_config = {"region": "us-west-2"}

        create_session_mock = Mock()
        sns_fanout._create_boto_client_with_sts_session = create_session_mock
        sns_fanout.sts_expiration = datetime.now(timezone.utc) - timedelta(minutes=1)

        client_mock = Mock()
        sns_fanout._predefined_clients = {"test_exchange": client_mock}

        # Act
        result = sns_fanout._handle_sts_session(exchange_name, exchange_config)

        # Assert
        assert result == sns_fanout._create_boto_client_with_sts_session.return_value
        assert create_session_mock.call_args_list == [
            call("test_exchange", region="us-west-2")
        ]

    def test_token_refresh_required_non_expired_date_without_client(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        exchange_config = {"region": "us-west-2"}

        create_session_mock = Mock()
        sns_fanout._create_boto_client_with_sts_session = create_session_mock
        sns_fanout.sts_expiration = datetime.now(timezone.utc) + timedelta(minutes=1)
        client_mock = Mock()

        sns_fanout._predefined_clients = {"another-exchange": client_mock}

        # Act
        result = sns_fanout._handle_sts_session(exchange_name, exchange_config)

        # Assert
        assert result == sns_fanout._create_boto_client_with_sts_session.return_value
        assert create_session_mock.call_args_list == [
            call("test_exchange", region="us-west-2")
        ]

    def test_token_refresh_required_non_expired_date_with_client(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        exchange_config = {"region": "us-west-2"}

        create_session_mock = Mock()
        sns_fanout._create_boto_client_with_sts_session = create_session_mock
        sns_fanout.sts_expiration = datetime.now(timezone.utc) + timedelta(minutes=1)
        client_mock = Mock()

        sns_fanout._predefined_clients = {exchange_name: client_mock}

        # Act
        result = sns_fanout._handle_sts_session(exchange_name, exchange_config)

        # Assert
        assert create_session_mock.call_count == 0
        assert result is client_mock

    def test_create_boto_client_with_sts_session(
        self, sns_fanout, mock_sts_credentials
    ):
        # Arrange
        exchange_name = "test_exchange"
        region = "us-west-2"
        sns_fanout.channel.get_sts_credentials = Mock(return_value=mock_sts_credentials)

        boto_client_mock = Mock(name="My new boto client")
        sns_fanout.channel._new_boto_client = Mock(return_value=boto_client_mock)

        # Act
        result = sns_fanout._create_boto_client_with_sts_session(exchange_name, region)

        # Assert
        assert result is boto_client_mock

        # Check class vars have been updated
        assert sns_fanout.sts_expiration == mock_sts_credentials["Expiration"]
        assert sns_fanout._predefined_clients[exchange_name] == boto_client_mock

        # Check calls
        assert sns_fanout.channel.get_sts_credentials.call_args_list == [call()]
        assert sns_fanout.channel._new_boto_client.call_args_list == [
            call(
                service="sns",
                region="us-west-2",
                access_key_id="test_access_key",
                secret_access_key="test_secret_key",
                session_token="test_session_token",
            )
        ]

    def test_get_topic_arn_for_predefined_exchange_found_in_cache(self, sns_fanout):
        # Arrange
        exchange = "test-exchange"
        sns_fanout._topic_arn_cache = {"another-exchange": "another-topics-arn"}
        sns_fanout.channel.predefined_exchanges = {
            "test-exchange": {"arn": "arn:aws:sns:us-east-1:123456789012:test_topic"}
        }

        # Act
        result = sns_fanout._get_topic_arn_for_predefined_exchange(exchange)

        # Assert
        assert result == "arn:aws:sns:us-east-1:123456789012:test_topic"
        assert sns_fanout._topic_arn_cache == {
            "another-exchange": "another-topics-arn",
            "test-exchange": "arn:aws:sns:us-east-1:123456789012:test_topic"
        }

    def test_get_topic_arn_for_predefined_exchange_not_found_in_cache(self, sns_fanout):
        # Arrange
        exchange = "test-exchange"
        sns_fanout._topic_arn_cache = {"another-exchange": "another-topics-arn"}
        sns_fanout.channel.predefined_exchanges = {"topic-2": {"arn": "arn:aws:sns:us-east-1:123456789012:topic-2"}}

        # Act
        with pytest.raises(
            UndefinedExchangeException,
            match="Exchange with name 'test-exchange' must be defined in 'predefined_exchanges'.",
        ):
            sns_fanout._get_topic_arn_for_predefined_exchange(exchange)

        # Assert
        assert sns_fanout._topic_arn_cache == {
            "another-exchange": "another-topics-arn",
        }


class test_SnsSubscription:
    @pytest.fixture
    def mock_get_queue_arn(self, sns_subscription):
        with patch.object(sns_subscription, "_get_queue_arn") as mock:
            yield mock

    @pytest.fixture
    def mock_get_or_create_topic(self, sns_fanout):
        with patch.object(sns_fanout, "_get_or_create_topic") as mock:
            yield mock

    @pytest.fixture
    def mock_get_client(self, sns_fanout):
        with patch.object(sns_fanout, "get_client") as mock:
            yield mock

    @pytest.fixture
    def mock_set_permission_on_sqs_queue(self, sns_subscription):
        with patch.object(sns_subscription, "_set_permission_on_sqs_queue") as mock:
            yield mock

    @pytest.fixture
    def mock_subscribe_queue_to_sns_topic(self, sns_subscription):
        with patch.object(sns_subscription, "_subscribe_queue_to_sns_topic") as mock:
            yield mock

    @pytest.fixture
    def mock_get_queue_attributes(self, sns_subscription):
        with patch.object(sns_subscription, "_get_queue_attributes") as mock:
            yield mock

    def test_subscribe_queue_already_subscribed(
        self,
        sns_subscription,
        sns_fanout,
        mock_get_client
    ):
        # Arrange
        queue_name = "test_queue"
        exchange_name = "test_exchange"
        cached_subscription_arn = "arn:aws:sns:us-east-1:123456789012:test_topic:cached"
        sns_subscription._subscription_arn_cache[f"{exchange_name}:{queue_name}"] = (
            cached_subscription_arn
        )

        # Act
        result = sns_subscription.subscribe_queue(queue_name, exchange_name)

        # Assert
        assert result == cached_subscription_arn
        assert mock_get_client.call_count == 0

    def test_subscribe_queue_success_queue_in_cache(
        self,
        sns_subscription,
        caplog,
        mock_get_or_create_topic,
        mock_get_queue_arn,
        mock_set_permission_on_sqs_queue,
        mock_subscribe_queue_to_sns_topic,
    ):
        # Arrange
        queue_name = "test_queue"
        exchange_name = "test_exchange"
        queue_arn = "arn:aws:sqs:us-east-1:123456789012:test_queue"
        topic_arn = "arn:aws:sns:us-east-1:123456789012:test_topic"
        subscription_arn = "arn:aws:sns:us-east-1:123456789012:test_topic:12345678-1234-1234-1234-123456789012"

        mock_get_queue_arn.return_value = queue_arn
        mock_get_or_create_topic.return_value = topic_arn
        mock_subscribe_queue_to_sns_topic.return_value = subscription_arn

        # Act
        result = sns_subscription.subscribe_queue(queue_name, exchange_name)

        # Assert
        assert result == subscription_arn
        assert (
            sns_subscription._subscription_arn_cache[f"{exchange_name}:{queue_name}"]
            == subscription_arn
        )
        assert mock_get_queue_arn.call_args_list == [call("test_queue")]
        assert mock_get_or_create_topic.call_args_list == [call(exchange_name)]
        assert mock_subscribe_queue_to_sns_topic.call_args_list == [
            call(topic_arn=topic_arn, queue_arn=queue_arn)
        ]
        assert mock_set_permission_on_sqs_queue.call_args_list == [
            call(topic_arn=topic_arn, queue_arn=queue_arn, queue_name=queue_name)
        ]

    def test_create_subscription_double_lock_check(
        self, sns_subscription, mock_subscribe_queue_to_sns_topic
    ):
        # Arrange
        queue_arn = "arn:aws:sqs:us-west-2:123456789012:my-queue"
        exchange = "test-exchange"
        sns_subscription._subscription_arn_cache = MagicMock()
        sns_subscription._subscription_arn_cache.get.side_effect = [None, "cached-subscription-arn"]

        # Act
        result = sns_subscription.subscribe_queue(queue_arn, exchange)

        # Assert
        assert result == "cached-subscription-arn"
        assert mock_subscribe_queue_to_sns_topic.call_count == 0

    def test_unsubscribe_queue_not_in_cache(
        self,
        sns_subscription,
    ):
        # Arrange
        queue_name = "test_queue"
        exchange_name = "test_exchange"
        sns_subscription._subscription_arn_cache = {
            "another-exchange:another_queue": "123"
        }
        sns_subscription._unsubscribe_sns_subscription = Mock()

        # Act
        result = sns_subscription.unsubscribe_queue(queue_name, exchange_name)

        # Assert
        assert result is None
        assert sns_subscription._unsubscribe_sns_subscription.call_count == 0

    def test_unsubscribe_queue_in_cache(self, sns_subscription, caplog):
        # Arrange
        caplog.set_level(logging.DEBUG)
        queue_name = "test_queue"
        exchange_name = "test_exchange"
        subscription_arn = "arn:aws:sns:us-east-1:123456789012:test_topic:12345678-1234-1234-1234-123456789012"
        sns_subscription._subscription_arn_cache = {
            "test_exchange:test_queue": subscription_arn
        }
        sns_subscription._unsubscribe_sns_subscription = Mock()

        # Act
        result = sns_subscription.unsubscribe_queue(queue_name, exchange_name)

        # Assert
        assert result is None
        assert (
            f"Unsubscribed subscription '{subscription_arn}' for SQS queue '{queue_name}'"
            in caplog.text
        )
        assert sns_subscription._unsubscribe_sns_subscription.call_args_list == [
            call(subscription_arn)
        ]

    def test_cleanup_with_predefined_exchanges(
        self, sns_subscription, caplog, channel_fixture, sns_fanout
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)

        exchange_name = "exchange-1"

        channel_fixture.predefined_exchanges = {"exchange-1": {}}
        sns_fanout._get_or_create_topic = Mock()

        # Act
        result = sns_subscription.cleanup(exchange_name)

        # Assert
        assert result is None
        assert (
                   "'predefined_exchanges' has been specified, so stale SNS subscription"
                   " cleanup will be skipped."
               ) in caplog.text
        assert sns_fanout._get_or_create_topic.call_count == 0

    def test_cleanup_no_invalid_subscriptions(
        self, sns_subscription, caplog, channel_fixture, sns_fanout
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)

        topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        exchange_name = "exchange-1"

        channel_fixture.predefined_exchanges = {}
        sns_fanout._get_or_create_topic = Mock(return_value=topic_arn)
        sns_subscription._get_invalid_sns_subscriptions = Mock(return_value=[])
        sns_subscription._unsubscribe_sns_subscription = Mock()

        # Act
        result = sns_subscription.cleanup(exchange_name)

        # Assert
        assert result is None
        assert (
                   f"Checking for stale SNS subscriptions for exchange '{exchange_name}'"
               ) in caplog.text
        assert sns_fanout._get_or_create_topic.call_args_list == [call(exchange_name)]
        assert sns_subscription._unsubscribe_sns_subscription.call_count == 0

    def test_cleanup_with_invalid_subscriptions(
        self, sns_subscription, caplog, channel_fixture, sns_fanout
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)

        topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        exchange_name = "exchange-1"

        channel_fixture.predefined_exchanges = {}
        sns_fanout._get_or_create_topic = Mock(return_value=topic_arn)
        sns_subscription._get_invalid_sns_subscriptions = Mock(
            return_value=[
                "subscription-arn-1",
                "subscription-arn-2",
                "subscription-arn-3",
            ]
        )

        # Ensure that we carry on after hitting an exception
        sns_subscription._unsubscribe_sns_subscription = Mock(
            side_effect=[None, ConnectionError("A test exception"), None]
        )

        # Act
        result = sns_subscription.cleanup(exchange_name)

        # Assert
        assert result is None
        assert sns_fanout._get_or_create_topic.call_args_list == [call(exchange_name)]
        assert sns_subscription._unsubscribe_sns_subscription.call_args_list == [
            call("subscription-arn-1"),
            call("subscription-arn-2"),
            call("subscription-arn-3"),
        ]

        # Check logs
        log_lines = [
            f"Removed stale subscription 'subscription-arn-1' for SNS topic '{topic_arn}'",
            f"Failed to remove stale subscription 'subscription-arn-2' for SNS topic"
            f" '{topic_arn}': A test exception",
            f"Removed stale subscription 'subscription-arn-3' for SNS topic '{topic_arn}'",
        ]
        for line in log_lines:
            assert line in caplog.text

    def test_set_permission_on_sqs_queue(
        self, sns_subscription, caplog, mock_sqs, channel_fixture
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)

        topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        queue_name = "my-queue"
        queue_arn = "arn:aws:sqs:us-east-1:123456789012:my-queue"

        channel_fixture.predefined_queues = {}
        channel_fixture.sqs.return_value = mock_sqs()
        channel_fixture._queue_cache[queue_name] = (
            "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"
        )

        exchange = Exchange("test_SQS", type="direct")
        queue = Queue(queue_name, exchange)
        queue(channel_fixture).declare()

        # Act
        sns_subscription._set_permission_on_sqs_queue(topic_arn, queue_name, queue_arn)

        # Assert
        expected_policy = {
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

        assert mock_sqs().set_queue_attributes.call_args_list == [
            call(
                QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
                Attributes={"Policy": json.dumps(expected_policy)},
            )
        ]

        assert (
                   "Set permissions on SNS topic 'arn:aws:sns:us-east-1:123456789012:my-topic'"
               ) in caplog.text

    def test_get_existing_queue_policy_with_existing_policy(
        self, sns_subscription
    ):
        # Arrange
        queue_name = "my-queue"
        queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"

        sqs_client_mock = MagicMock()
        sqs_client_mock.get_queue_attributes.return_value = {
            "Attributes": {
                "key1": "Some value",
                "Policy": json.dumps({
                    "Version":   "2012-10-17",
                    "Statement": [
                        {
                            "Sid":       "ExistingStatement",
                            "Effect":    "Allow",
                            "Principal": {"Service": "sns.amazonaws.com"},
                            "Action":    "SQS:SendMessage",
                            "Resource":  "arn:aws:sqs:us-east-1:123456789012:my-queue",
                            "Condition": {"ArnLike": {"aws:SourceArn": "123"}},
                        }
                    ],
                })
            }
        }

        # Act
        result = sns_subscription._get_existing_queue_policy(sqs_client_mock, queue_name, queue_url)

        # Assert
        expected_policy = {
            "Version":   "2012-10-17",
            "Statement": [
                {
                    "Sid":       "ExistingStatement",
                    "Effect":    "Allow",
                    "Principal": {"Service": "sns.amazonaws.com"},
                    "Action":    "SQS:SendMessage",
                    "Resource":  "arn:aws:sqs:us-east-1:123456789012:my-queue",
                    "Condition": {"ArnLike": {"aws:SourceArn": "123"}},
                }
            ],
        }
        assert result == expected_policy
        assert sqs_client_mock.get_queue_attributes.call_args_list == [
            call(QueueUrl=queue_url, AttributeNames=["Policy"])
        ]

    def test_get_existing_queue_policy_with_no_existing_policy(
        self, sns_subscription
    ):
        # Arrange
        queue_name = "my-queue"
        queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"

        sqs_client_mock = MagicMock()
        sqs_client_mock.get_queue_attributes.return_value = {
            "Attributes": {
                "key1": "Some value",
            }
        }

        # Act
        result = sns_subscription._get_existing_queue_policy(sqs_client_mock, queue_name, queue_url)

        # Assert
        assert result == {}

    def test_get_existing_queue_policy_with_invalid_policy(
        self, sns_subscription, caplog
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)
        queue_name = "my-queue"
        queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"

        sqs_client_mock = MagicMock()
        sqs_client_mock.get_queue_attributes.return_value = {
            "Attributes": {
                "key1": "Some value",
                "Policy": "Not a valid JSON string"
            }
        }

        # Act
        result = sns_subscription._get_existing_queue_policy(sqs_client_mock, queue_name, queue_url)

        # Assert
        assert result == {}
        assert f"Existing SQS policy for queue '{queue_name}' is malformed:" in caplog.text

    def test_get_existing_queue_policy_client_error(
        self, sns_subscription, caplog
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)
        queue_name = "my-queue"
        queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"

        sqs_client_mock = MagicMock()
        sqs_client_mock.get_queue_attributes.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDenied"}}, operation_name="GetQueueAttributes"
        )

        # Act
        result = sns_subscription._get_existing_queue_policy(sqs_client_mock, queue_name, queue_url)

        # Assert
        assert result == {}
        assert f"Unable to retrieve existing SQS policy for queue '{queue_name}':" in caplog.text

    def test_generate_new_sqs_policy_with_no_existing_policy(self, sns_subscription):
        # Arrange
        existing_policy = {}
        topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        queue_arn = "arn:aws:sqs:us-east-1:123456789012:my-queue"

        # Act
        result = sns_subscription._generate_new_sqs_policy(existing_policy, topic_arn, queue_arn)

        # Assert
        assert result == {
            'Statement': [
                {
                    'Action': 'SQS:SendMessage',
                    'Condition': {
                        'ArnLike': {
                            'aws:SourceArn': topic_arn,
                        },
                    },
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'sns.amazonaws.com',
                    },
                    'Resource': queue_arn,
                    'Sid': 'KombuManaged',
                },
            ],
            'Version': '2012-10-17',
        }
        assert existing_policy is not result, "The original policy object should not be mutated"

    def test_generate_new_sqs_policy_with_existing_policy_no_kombu_managed(
            self, sns_subscription
    ):
        # Arrange
        topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        queue_arn = "arn:aws:sqs:us-east-1:123456789012:my-queue"
        existing_policy = {
            'Statement': [
                {
                    'Action': 'SQS:SendMessage',
                    'Condition': {
                        'ArnLike': {
                            'aws:SourceArn': "arn:aws:sns:us-east-1:123456789012:another-topic",
                        },
                    },
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'sns.amazonaws.com',
                    },
                    'Resource': "arn:aws:sqs:us-east-1:123:another-queue",
                    'Sid': 'SqsPolicyForSomethingCool',
                },
            ],
            'Version': '2012-10-17',
        }

        # Act
        result = sns_subscription._generate_new_sqs_policy(
            existing_policy, topic_arn, queue_arn
        )

        # Assert
        assert result == {
            'Statement': [
                {
                    'Action': 'SQS:SendMessage',
                    'Condition': {
                        'ArnLike': {
                            'aws:SourceArn': 'arn:aws:sns:us-east-1:123456789012:another-topic',
                        },
                    },
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'sns.amazonaws.com',
                    },
                    'Resource': 'arn:aws:sqs:us-east-1:123:another-queue',
                    'Sid': 'SqsPolicyForSomethingCool',
                },
                {
                    'Action': 'SQS:SendMessage',
                    'Condition': {
                        'ArnLike': {
                            'aws:SourceArn': 'arn:aws:sns:us-east-1:123456789012:my-topic',
                        },
                    },
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'sns.amazonaws.com',
                    },
                    'Resource': 'arn:aws:sqs:us-east-1:123456789012:my-queue',
                    'Sid': 'KombuManaged',
                },
            ],
            'Version': '2012-10-17',
        }
        assert existing_policy is not result, "The original policy object should not be mutated"

    def test_generate_new_sqs_policy_with_existing_policy_with_kombu_managed(self, sns_subscription):
        # Arrange
        topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        queue_arn = "arn:aws:sqs:us-east-1:123456789012:my-queue"
        existing_policy = {
            'Statement': [
                {
                    'Action': 'SQS:SendMessage',
                    'Condition': {
                        'ArnLike': {
                            'aws:SourceArn': "arn:aws:sns:us-east-1:123456789012:another-topic",
                        },
                    },
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'sns.amazonaws.com',
                    },
                    'Resource': "arn:aws:sqs:us-east-1:123:another-queue",
                    'Sid': 'KombuManaged',
                },
            ],
            'Version': '2012-10-17',
        }

        # Act
        result = sns_subscription._generate_new_sqs_policy(existing_policy, topic_arn, queue_arn)

        # Assert
        assert result == {
            'Statement': [
                {
                    'Action': 'SQS:SendMessage',
                    'Condition': {
                        'ArnLike': {
                            'aws:SourceArn': 'arn:aws:sns:us-east-1:123456789012:my-topic',
                        },
                    },
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'sns.amazonaws.com',
                    },
                    'Resource': 'arn:aws:sqs:us-east-1:123456789012:my-queue',
                    'Sid': 'KombuManaged',
                },
            ],
            'Version': '2012-10-17',
        }
        assert existing_policy is not result, "The original policy object should not be mutated"

    @pytest.mark.parametrize("policy, expected_statements", [
        (None, []),
        ([], []),
        ({}, []),
        ({"Version": "2012-10-17"}, []),
        ({"Statement": []}, []),
        (
            {"Statement": [{"PolicyName": "Policy1"}, {"PolicyName": "Policy2"}]},
            [{"PolicyName": "Policy1"}, {"PolicyName": "Policy2"}]
        ),
        ({"Statement": {"PolicyName": "AStatementAsADict"}}, [{"PolicyName": "AStatementAsADict"}])
    ])
    def test_extract_statements_from_policy(self, sns_subscription, policy, expected_statements):
        # Act
        result = sns_subscription._extract_statements_from_policy(policy)

        # Assert
        assert result == expected_statements

    def test_subscribe_queue_to_sns_topic_successful_subscription(
        self, sns_subscription, caplog, sns_fanout, mock_get_client
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)

        queue_arn = "arn:aws:sqs:us-west-2:123456789012:my-queue"
        topic_arn = "arn:aws:sns:us-west-2:123456789012:my-topic"
        subscription_arn = "arn:aws:sns:us-west-2:123456789012:my-topic:12345678-1234-1234-1234-123456789012"
        mock_client = Mock()
        mock_client.return_value.subscribe.return_value = {
            "SubscriptionArn":  subscription_arn,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        sns_fanout.get_client = mock_client

        # Act
        result = sns_subscription._subscribe_queue_to_sns_topic(queue_arn, topic_arn)

        # Assert
        assert result == subscription_arn
        assert mock_get_client.call_args_list == []
        assert mock_client.return_value.subscribe.call_args_list == [
            call(
                TopicArn="arn:aws:sns:us-west-2:123456789012:my-topic",
                Protocol="sqs",
                Endpoint="arn:aws:sqs:us-west-2:123456789012:my-queue",
                Attributes={"RawMessageDelivery": "true"},
                ReturnSubscriptionArn=True,
            )
        ]
        assert (
            f"Subscribing queue '{queue_arn}' to SNS topic '{topic_arn}'" in caplog.text
        )
        assert (
            f"Create subscription '{subscription_arn}' for SQS queue '{queue_arn}' to SNS topic '{topic_arn}'"
            in caplog.text
        )

    def test_subscribe_queue_to_sns_topic_subscription_failure(
        self, sns_subscription, sns_fanout
    ):
        # Arrange
        queue_arn = "arn:aws:sqs:us-west-2:123456789012:my-queue"
        topic_arn = "arn:aws:sns:us-west-2:123456789012:my-topic"
        mock_client = Mock()
        mock_client.return_value.subscribe.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 400}
        }
        sns_fanout.get_client = mock_client

        # Act and Assert
        with pytest.raises(
            Exception, match="Unable to subscribe queue: status code was 400"
        ):
            sns_subscription._subscribe_queue_to_sns_topic(queue_arn, topic_arn)

    def test_subscribe_queue_to_sns_topic_client_error(
        self, sns_subscription, sns_fanout
    ):
        # Arrange
        queue_arn = "arn:aws:sqs:us-west-2:123456789012:my-queue"
        topic_arn = "arn:aws:sns:us-west-2:123456789012:my-topic"

        mock_client = Mock()
        mock_client.return_value.subscribe.side_effect = ClientError(
            error_response={"Error": {"Code": "InvalidParameter"}},
            operation_name="Subscribe",
        )
        sns_fanout.get_client = mock_client

        # Act and Assert
        with pytest.raises(ClientError):
            sns_subscription._subscribe_queue_to_sns_topic(queue_arn, topic_arn)

    def test_unsubscribe_sns_subscription_success(self, sns_subscription, sns_fanout):
        # Arrange
        subscription_arn = (
            "arn:aws:sns:us-west-2:123456789012:my-topic:12345678-12:sub-id"
        )

        mock_client = Mock()
        mock_client.return_value.unsubscribe.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        sns_fanout.get_client = mock_client

        # Act
        result = sns_subscription._unsubscribe_sns_subscription(subscription_arn)

        # Assert
        assert result is None
        assert mock_client.return_value.unsubscribe.call_args_list == [
            call(SubscriptionArn=subscription_arn)
        ]

    def test_unsubscribe_sns_subscription_error(
        self, sns_subscription, sns_fanout
    ):
        # Arrange
        subscription_arn = (
            "arn:aws:sns:us-west-2:123456789012:my-topic:12345678-12:sub-id"
        )

        mock_client = Mock()
        mock_client.return_value.unsubscribe.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 400}
        }
        sns_fanout.get_client = mock_client

        # Act
        with pytest.raises(
            UnableToUnsubscribeQueueFromTopicException,
            match="SNS unsubscribe API returned status code '400'",
        ):
            sns_subscription._unsubscribe_sns_subscription(subscription_arn)

        # Assert
        assert mock_client.return_value.unsubscribe.call_args_list == [
            call(SubscriptionArn=subscription_arn)
        ]

    def test_get_invalid_sns_subscriptions(self, sns_subscription, sns_fanout):
        # Arrange
        client_mock = Mock()
        sns_fanout.get_client = client_mock

        # Mock paginator
        mock_paginate = Mock()
        sns_fanout.get_client().get_paginator.return_value = mock_paginate
        mock_paginate.paginate.return_value = iter(
            [
                {
                    "Subscriptions": [
                        {"SubscriptionArn": "arn1"},
                        {"SubscriptionArn": "arn2"},
                    ]
                },
                {"Subscriptions": [{"SubscriptionArn": "arn3"}]},
            ]
        )

        # Mock filter
        sns_subscription._filter_sns_subscription_response = Mock(
            side_effect=[["arn3"], ["arn2"]]
        )

        sns_topic_arn = "arn:aws:sns:us-west-2:123456789012:my-topic"

        # Act
        result = sns_subscription._get_invalid_sns_subscriptions(sns_topic_arn)

        # Assert
        assert result == ["arn3", "arn2"]
        assert mock_paginate.paginate.call_args_list == [call(TopicArn=sns_topic_arn)]
        assert sns_subscription._filter_sns_subscription_response.call_args_list == [
            call([{"SubscriptionArn": "arn1"}, {"SubscriptionArn": "arn2"}]),
            call([{"SubscriptionArn": "arn3"}]),
        ]

    def test_get_invalid_sns_subscriptions_empty(self, sns_subscription, sns_fanout):
        # Arrange
        client_mock = Mock()
        sns_fanout.get_client = client_mock

        # Mock paginator
        mock_paginate = Mock()
        sns_fanout.get_client().get_paginator.return_value = mock_paginate
        mock_paginate.paginate.return_value = iter(
            [
                {"Subscriptions": []},
                {"Subscriptions": []},
            ]
        )

        # Mock filter
        sns_subscription._filter_sns_subscription_response = Mock(return_value=[])

        # Act
        result = sns_subscription._get_invalid_sns_subscriptions(
            "arn:aws:sns:us-west-2:123456789012:my-topic"
        )

        # Assert
        assert result == []

    def test_get_invalid_sns_subscriptions_no_subscriptions_key(
        self, sns_subscription, sns_fanout
    ):
        # Arrange
        client_mock = Mock()
        sns_fanout.get_client = client_mock

        # Mock paginator
        mock_paginate = Mock()
        sns_fanout.get_client().get_paginator.return_value = mock_paginate
        mock_paginate.paginate.return_value = iter(
            [
                {},
                {"Subscriptions": [{"SubscriptionArn": "arn1"}]},
            ]
        )

        # Mock filter
        sns_subscription._filter_sns_subscription_response = Mock(return_value=[])

        sns_topic_arn = "arn:aws:sns:us-west-2:123456789012:my-topic"

        sns_subscription._filter_sns_subscription_response = Mock(
            side_effect=[[], ["arn1"]]
        )

        # Act
        result = sns_subscription._get_invalid_sns_subscriptions(sns_topic_arn)

        # Assert
        assert result == ["arn1"]
        assert sns_subscription._filter_sns_subscription_response.call_args_list == [
            call(None),
            call([{"SubscriptionArn": "arn1"}]),
        ]

    @pytest.mark.parametrize("value", [None, "", []])
    def test__filter_sns_subscription_response_nothing_provided(
        self, value, sns_subscription
    ):
        # Act & Assert
        assert sns_subscription._filter_sns_subscription_response(value) == []

    def test__filter_sns_subscription_response(self, sns_subscription, channel_fixture):
        # Arrange
        subscriptions = [
            {
                "Protocol":        "SqS",
                "Endpoint":        "arn:aws:sqs:us-east-2:444455556666:my-queue-1",
                "SubscriptionArn": "arn-1",
            },  # Test case-sensitivity
            {
                "Protocol":        "sqs",
                "Endpoint":        "arn:aws:sqs:us-east-2:444455556666:my-queue-2",
                "SubscriptionArn": "arn-2",
            },  # Test case-sensitivity
            {
                "Protocol":        "Lambda",
                "Endpoint":        "arn:aws:lambda:us-west-2:123456789012:function:my-lambda-function",
                "SubscriptionArn": "lambda-arn-1",
            },  # This should be filtered out
            {
                "Protocol":        "sqs",
                "Endpoint":        "arn:aws:sqs:us-west-2:123456789012:my-queue-3",
                "SubscriptionArn": "arn-3",
            },
            {
                "Protocol":        "sqs",
                "Endpoint":        "arn:aws:sqs:us-west-2:123456789012:my-queue-4",
                "SubscriptionArn": "arn-4",
            },
            {
                "Protocol":        "sqs",
                "Endpoint":        "arn:aws:sqs:us-west-2:123456789012:my-queue-5",
                "SubscriptionArn": "arn-5",
            },
            {
                "Protocol":        "SQS",
                "Endpoint":        "arn:aws:sqs:us-west-2:123456789012:my-queue-6",
                "SubscriptionArn": "arn-6",
            },
        ]
        sqs_mock = Mock()
        channel_fixture.sqs = sqs_mock

        # Setup errors on queues 2,4 and 5
        sqs_mock.return_value.get_queue_url.side_effect = [
            None,  # queue-1
            ClientError(
                error_response={"Error": {"Code": "QueueDoesNotExist"}},
                operation_name="GetQueueUrl",
            ),  # queue-2
            None,  # queue-3
            ClientError(
                error_response={"Error": {"Code": "NonExistentQueue"}},
                operation_name="GetQueueUrl",
            ),  # queue-4
            ClientError(
                error_response={"Error": {"Code": "NonExistentQueue"}},
                operation_name="GetQueueUrl",
            ),  # queue-5
            None,  # queue-6
        ]

        # Act
        result = sns_subscription._filter_sns_subscription_response(subscriptions)

        # Assert
        assert result == ["arn-2", "arn-4", "arn-5"]
        assert sqs_mock.return_value.get_queue_url.call_args_list == [
            call(QueueName="my-queue-1"),
            call(QueueName="my-queue-2"),
            call(QueueName="my-queue-3"),
            call(QueueName="my-queue-4"),
            call(QueueName="my-queue-5"),
            call(QueueName="my-queue-6"),
        ]

    @pytest.mark.parametrize(
        "exc_type", [ClientError, ValueError, Exception, IndexError, KeyError]
    )
    def test__filter_sns_subscription_response_exceptions(
        self, exc_type, sns_subscription, channel_fixture
    ):
        # Arrange
        subscriptions = [
            {
                "Protocol":        "SqS",
                "Endpoint":        "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-1",
                "SubscriptionArn": "arn-1",
            },  # Test case-sensitivity
            {
                "Protocol":        "sqs",
                "Endpoint":        "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-2",
                "SubscriptionArn": "arn-2",
            },  # Test case-sensitivity
            {
                "Protocol":        "Lambda",
                "Endpoint":        "arn:aws:lambda:us-west-2:123456789012:function:my-lambda-function",
                "SubscriptionArn": "lambda-arn-1",
            },  # This should be filtered out
            {
                "Protocol":        "sqs",
                "Endpoint":        "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-3",
                "SubscriptionArn": "arn-3",
            },
            {
                "Protocol":        "sqs",
                "Endpoint":        "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-4",
                "SubscriptionArn": "arn-4",
            },
            {
                "Protocol":        "sqs",
                "Endpoint":        "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-5",
                "SubscriptionArn": "arn-5",
            },
            {
                "Protocol":        "SQS",
                "Endpoint":        "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-6",
                "SubscriptionArn": "arn-6",
            },
        ]
        sqs_mock = Mock()
        channel_fixture.sqs = sqs_mock

        # Build exception
        if exc_type == ClientError:
            exc = ClientError(
                error_response={"Error": {"Code": "ThisIsATest"}},
                operation_name="GetQueueUrl",
            )
        else:
            exc = exc_type("This is a test exception")

        sqs_mock.return_value.get_queue_url.side_effect = [
            None,  # queue-1
            exc,  # queue-2
            None,  # queue-3
            ClientError(
                error_response={"Error": {"Code": "NonExistentQueue"}},
                operation_name="GetQueueUrl",
            ),  # queue-4
            ClientError(
                error_response={"Error": {"Code": "NonExistentQueue"}},
                operation_name="GetQueueUrl",
            ),  # queue-5
            None,  # queue-6
        ]

        # Act & Assert
        with pytest.raises(exc_type):
            sns_subscription._filter_sns_subscription_response(subscriptions)

    def test_get_queue_arn_in_cache(self, sns_subscription, sns_fanout):
        # Arrange
        sns_subscription._queue_arn_cache = {
            "my_queue":   "arn:aws:sqs:us-west-2:123456789012:my-queue",
            "my-queue-2": "arn:aws:sqs:us-west-2:123456789012:my-queue-2",
            "my-queue-3": "arn:aws:sqs:us-west-2:123456789012:my-queue-3",
        }

        chan_mock = Mock()
        sns_fanout.channel = chan_mock

        # Act
        result = sns_subscription._get_queue_arn("my-queue-2")

        # Assert
        assert result == "arn:aws:sqs:us-west-2:123456789012:my-queue-2"
        assert chan_mock._resolve_queue_url.call_count == 0

    def test_get_queue_arn_lookup_success(self, sns_subscription, sns_fanout):
        # Arrange
        sns_subscription._queue_arn_cache = {
            "my_queue":   "arn:aws:sqs:us-west-2:123456789012:my-queue",
            "my-queue-2": "arn:aws:sqs:us-west-2:123456789012:my-queue-2",
            "my-queue-3": "arn:aws:sqs:us-west-2:123456789012:my-queue-3",
        }
        queue_arn = "arn:aws:sqs:us-west-2:123456789012:my-queue-4"
        queue_url = "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-4"

        chan_mock = Mock()
        sns_fanout.channel = chan_mock
        chan_mock._resolve_queue_url.return_value = queue_url
        chan_mock.sqs.return_value.get_queue_attributes.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "Attributes":       {"QueueArn": queue_arn},
        }
        assert "my-queue-4" not in sns_subscription._queue_arn_cache

        # Act
        result = sns_subscription._get_queue_arn("my-queue-4")

        # Assert
        assert result == queue_arn
        assert chan_mock._resolve_queue_url.call_args_list == [call("my-queue-4")]
        assert chan_mock.sqs.return_value.get_queue_attributes.call_args_list == [
            call(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        ]
        assert sns_subscription._queue_arn_cache["my-queue-4"] == queue_arn

    def test_get_queue_arn_lookup_failure(self, sns_subscription, sns_fanout):
        # Arrange
        sns_subscription._queue_arn_cache = {
            "my_queue":   "arn:aws:sqs:us-west-2:123456789012:my-queue",
            "my-queue-2": "arn:aws:sqs:us-west-2:123456789012:my-queue-2",
            "my-queue-3": "arn:aws:sqs:us-west-2:123456789012:my-queue-3",
        }
        queue_arn = "arn:aws:sqs:us-west-2:123456789012:my-queue-4"
        queue_url = "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-4"

        chan_mock = Mock()
        sns_fanout.channel = chan_mock
        chan_mock._resolve_queue_url.return_value = queue_url
        chan_mock.sqs.return_value.get_queue_attributes.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 500},
            "Attributes":       {"QueueArn": queue_arn},
        }
        assert "my-queue-4" not in sns_subscription._queue_arn_cache

        # Act & assert
        with pytest.raises(
            KombuError,
            match="Unable to get ARN for SQS queue 'my-queue-4': status code was '500'",
        ):
            sns_subscription._get_queue_arn("my-queue-4")

    @pytest.fixture
    def unsubscribe_mock(self, sns_subscription):
        mock = Mock()
        sns_subscription._unsubscribe_sns_subscription = mock
        return mock

    @pytest.mark.parametrize(
        "queue_name, exchange_name",
        [
            (None, None),
            ("", ""),
            ("test_queue", "test_exchange"),
        ],
    )
    def test_unsubscribe_queue_returns_none_when_subscription_not_cached(
        self, sns_subscription, unsubscribe_mock, queue_name, exchange_name
    ):
        """Return early without side effects when no cached subscription exists."""
        # Arrange
        sns_subscription._subscription_arn_cache = {
            "another-exchange:another-queue": "arn:aws:sns:us-east-1:123456789012:topic:sub"
        }

        # Act
        result = sns_subscription.unsubscribe_queue(queue_name, exchange_name)

        # Assert
        assert result is None
        assert unsubscribe_mock.call_count == 0

    def test_unsubscribe_queue_logs_and_removes_cache_on_success(
        self, sns_subscription, unsubscribe_mock, caplog
    ):
        """Unsubscribe successfully, emit an info log, and remove the cached ARN."""
        # Arrange
        caplog.set_level(logging.INFO)
        queue_name = "test_queue"
        exchange_name = "test_exchange"
        cache_key = f"{exchange_name}:{queue_name}"
        subscription_arn = (
            "arn:aws:sns:us-east-1:123456789012:test-topic:"
            "12345678-1234-1234-1234-123456789012"
        )
        sns_subscription._subscription_arn_cache = {cache_key: subscription_arn}

        # Act
        result = sns_subscription.unsubscribe_queue(queue_name, exchange_name)

        # Assert
        assert result is None
        assert unsubscribe_mock.call_args_list == [call(subscription_arn)]
        assert cache_key not in sns_subscription._subscription_arn_cache
        assert (
            f"Unsubscribed subscription '{subscription_arn}' for SQS queue '{queue_name}'"
            in caplog.text
        )

    def test_unsubscribe_queue_logs_and_reraises_on_unsubscribe_failure_and_still_removes_cache(
        self, sns_subscription, unsubscribe_mock, caplog
    ):
        """Log and re-raise unsubscribe failures while always invalidating cache."""
        # Arrange
        caplog.set_level(logging.ERROR)
        queue_name = "test_queue"
        exchange_name = "test_exchange"
        cache_key = f"{exchange_name}:{queue_name}"
        subscription_arn = "arn:aws:sns:us-east-1:123456789012:test-topic:sub-1"
        sns_subscription._subscription_arn_cache = {cache_key: subscription_arn}
        unsubscribe_mock.side_effect = UnableToUnsubscribeQueueFromTopicException(
            "SNS unsubscribe API returned status code '500'"
        )

        # Act / Assert
        with pytest.raises(
            UnableToUnsubscribeQueueFromTopicException,
            match="SNS unsubscribe API returned status code '500'",
        ):
            sns_subscription.unsubscribe_queue(queue_name, exchange_name)

        # Assert
        assert unsubscribe_mock.call_args_list == [call(subscription_arn)]
        assert cache_key not in sns_subscription._subscription_arn_cache
        assert (
            f"Failed to unsubscribe queue '{queue_name}' from SNS topic '{exchange_name}': "
            "SNS unsubscribe API returned status code '500'"
        ) in caplog.text


class _SqsSnsE2eTestBase:
    @pytest.fixture
    def mock_sns_client(self):
        sns_client_mock = MagicMock(name='MockSNSClient')
        sns_client_mock.create_topic.return_value = {
            "TopicArn": "arn:aws:sns:us-east-1:123456789012:test_exchange"
        }
        return sns_client_mock

    @pytest.fixture
    def mock_sqs_client(self):
        sqs_client_mock = MagicMock(name='MockSQSClient')
        return sqs_client_mock

    @pytest.fixture
    def mock_channel(self, mock_sns_client, mock_sqs_client):
        channel_mock = MagicMock(name='MockChannel')
        channel_mock.predefined_exchanges = {}
        channel_mock.transport_options = {}
        channel_mock.region = "us-east-1"
        channel_mock.conninfo.userid = "test_access_key_id"
        channel_mock.conninfo.password = "test_secret_access_key"
        channel_mock.is_sts_token_refresh_required.return_value = False
        channel_mock._resolve_queue_url.side_effect = (
            lambda name: f"https://sqs.us-east-1.amazonaws.com/123456789012/{name}"
        )
        channel_mock._new_boto_client.return_value = mock_sns_client
        channel_mock.sqs.return_value = mock_sqs_client
        channel_mock.canonical_queue_name.side_effect = lambda name: name
        return channel_mock

    @pytest.fixture
    def sns_instance(self, mock_channel) -> SnsFanout:
        return SnsFanout(mock_channel)

    @pytest.fixture(autouse=True)
    def _setup(self, caplog):
        caplog.set_level(logging.DEBUG)


class test_SqsSnsInitialiseExchangeE2E(_SqsSnsE2eTestBase):
    @pytest.mark.parametrize("exchange_name, is_fifo", [("test_exchange", False), ("another_exchange.fifo", True)])
    def test_not_initialised_yet(self, sns_instance, caplog, mock_channel, mock_sns_client, exchange_name, is_fifo):
        # Arrange
        mock_sns_client.create_topic.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "TopicArn":        f"arn:aws:sns:us-east-1:testing:{exchange_name}"
        }
        assert sns_instance._topic_arn_cache == {}

        # Act
        sns_instance.initialise_exchange(exchange_name)

        # Assert
        assert f"Checking for stale SNS subscriptions for exchange '{exchange_name}'" in caplog.text
        assert sns_instance._lock.locked() is False

        assert mock_channel._new_boto_client.call_args_list == [
            call(
                service='sns',
                region=mock_channel.region,
                access_key_id=mock_channel.conninfo.userid,
                secret_access_key=mock_channel.conninfo.password,
                session_token=None
            )
        ]
        assert mock_sns_client.create_topic.call_args_list == [
            call(
                Name=exchange_name,
                Attributes={'FifoTopic': 'true' if is_fifo else 'false'},
                Tags=[
                    {'Key': 'ManagedBy', 'Value': 'Celery/Kombu'},
                    {
                        'Key': 'Description',
                        'Value': 'This SNS topic is used by Kombu to enable Fanout support for AWS SQS.'
                    }
                ]
            )
        ]
        assert sns_instance._topic_arn_cache == {exchange_name: f"arn:aws:sns:us-east-1:testing:{exchange_name}"}

    @pytest.mark.parametrize("exchange_name, is_fifo", [("test_exchange", False), ("another_exchange.fifo", True)])
    def test_initialised_already(self, sns_instance, caplog, mock_channel, mock_sns_client, exchange_name, is_fifo):
        # Arrange
        sns_instance._topic_arn_cache = {exchange_name: f"arn:aws:sns:us-east-1:testing:{exchange_name}"}

        # Act
        sns_instance.initialise_exchange(exchange_name)

        # Assert
        assert sns_instance._lock.locked() is False

        assert mock_sns_client.create_topic.call_count == 0
        assert sns_instance._topic_arn_cache == {exchange_name: f"arn:aws:sns:us-east-1:testing:{exchange_name}"}

    def test_with_invalid_subscriptions(self, sns_instance, caplog, mock_channel, mock_sns_client, mock_sqs_client):
        def _get_queue_url_side_effect(QueueName):
            if QueueName == "my-queue":
                return {
                    "ResponseMetadata": {"HTTPStatusCode": 200},
                    "QueueUrl": f"https://sqs.us-east-1.amazonaws.com/123456789012/{QueueName}"
                    }
            else:
                raise ClientError(
                    error_response={"Error": {"Code": "QueueDoesNotExist"}},
                    operation_name="GetQueueUrl"
                )

        # Arrange
        exchange_name = "test_exchange"
        mock_sns_client.create_topic.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "TopicArn":        f"arn:aws:sns:us-east-1:testing:{exchange_name}"
        }
        mock_sns_client.get_paginator.return_value.paginate.return_value = iter([
            {"Subscriptions": [
                {
                    "SubscriptionArn": "arn:aws:sns:us-east-1:123456789012:my-topic:sub1",
                    "Protocol": "http",
                    "Endpoint": "http://localhost/example-endpoint"
                },  # Protocol is not SQS, so is not eligible for cleanup
                {
                    "SubscriptionArn": "arn:aws:sns:us-east-1:123456789012:my-topic:sub2",
                    "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:123456789012:my-queue"
                }  # This SQS subscription is valid (queue exists) and should not be removed
            ]},
            {"Subscriptions": [
                {
                    "SubscriptionArn": "arn:aws:sns:us-east-1:123456789012:my-topic:sub3",
                    "Protocol": "sqs",
                    "Endpoint": "arn:aws:sqs:us-east-1:123456789012:a-deleted-queue"
                }  # This SQS subscription is invalid (queue does not exist) and should be removed
            ]},
        ])
        mock_sqs_client.get_queue_url.side_effect = _get_queue_url_side_effect
        mock_sns_client.unsubscribe.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

        # Act
        sns_instance.initialise_exchange(exchange_name)

        # Assert
        assert f"Checking for stale SNS subscriptions for exchange '{exchange_name}'" in caplog.text
        assert (
            "Removed stale subscription 'arn:aws:sns:us-east-1:123456789012:my-topic:sub3' "
            "for SNS topic 'arn:aws:sns:us-east-1:testing:test_exchange'"
        ) in caplog.text
        assert (
            f"Created SNS topic '{exchange_name}' with "
            f"ARN 'arn:aws:sns:us-east-1:testing:{exchange_name}'"
        ) in caplog.text
        assert sns_instance._lock.locked() is False

        assert mock_channel._new_boto_client.call_count == 1
        assert mock_sns_client.create_topic.call_count == 1
        assert mock_sns_client.unsubscribe.call_args_list == [
            call(SubscriptionArn="arn:aws:sns:us-east-1:123456789012:my-topic:sub3")
        ]

        assert sns_instance._topic_arn_cache == {
            exchange_name: f"arn:aws:sns:us-east-1:testing:{exchange_name}"
        }

    def test_not_initialised_yet_with_predefined_exchanges_found(
        self, sns_instance, caplog, mock_channel, mock_sns_client
    ):
        # Arrange
        exchange_name = "test_exchange"
        mock_sns_client.create_topic.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "TopicArn":        f"arn:aws:sns:us-east-1:testing:{exchange_name}"
        }
        mock_channel.predefined_exchanges = {
            exchange_name: {"arn": f"arn:aws:sns:us-east-1:testing:{exchange_name}"}
        }
        assert sns_instance._topic_arn_cache == {}

        # Act
        sns_instance.initialise_exchange(exchange_name)

        # Assert
        assert (
            "'predefined_exchanges' has been specified,"
            " so stale SNS subscription cleanup will be skipped."
        ) in caplog.text
        assert sns_instance._lock.locked() is False

        assert mock_channel._new_boto_client.call_count == 0
        assert mock_sns_client.create_topic.call_count == 0
        assert sns_instance._topic_arn_cache == {exchange_name: f"arn:aws:sns:us-east-1:testing:{exchange_name}"}

    def test_not_initialised_yet_with_predefined_exchanges_not_found(
        self, sns_instance, caplog, mock_channel, mock_sns_client
    ):
        # Arrange
        exchange_name = "test_exchange"
        mock_sns_client.create_topic.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "TopicArn":        f"arn:aws:sns:us-east-1:testing:{exchange_name}"
        }
        mock_channel.predefined_exchanges = {
            "another-topic": {"arn": "arn:aws:sns:us-east-1:testing:another-topic"}
        }
        assert sns_instance._topic_arn_cache == {}

        # Act
        with pytest.raises(
            UndefinedExchangeException,
            match=f"Exchange with name '{exchange_name}' must be defined in 'predefined_exchanges'."
        ) as e:
            sns_instance.initialise_exchange(exchange_name)

        # Assert
        assert isinstance(e.value, KombuError)
        assert (
            "'predefined_exchanges' has been specified, so stale "
            "SNS subscription cleanup will be skipped."
        ) in caplog.text
        assert sns_instance._lock.locked() is False

        assert mock_channel._new_boto_client.call_count == 0
        assert mock_sns_client.create_topic.call_count == 0
        assert sns_instance._topic_arn_cache == {}


class test_SqsSnsPublishE2E(_SqsSnsE2eTestBase):
    def test_publish_end_to_end_with_attributes_and_request_params(
        self, sns_instance, mock_sns_client
    ):
        # Arrange
        exchange_name = "events"
        message = "hello"
        mock_sns_client.create_topic.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "TopicArn": "arn:aws:sns:us-east-1:testing:events",
        }
        mock_sns_client.publish.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

        # Act
        sns_instance.publish(
            exchange_name=exchange_name,
            message=message,
            message_attributes={"bool": True, "number": 12},
            request_params={"Subject": "example-subject"},
        )

        # Assert
        assert mock_sns_client.create_topic.call_args_list == [
            call(
                Name="events",
                Attributes={"FifoTopic": "false"},
                Tags=[
                    {"Key": "ManagedBy", "Value": "Celery/Kombu"},
                    {
                        "Key": "Description",
                        "Value": "This SNS topic is used by Kombu to enable Fanout support for AWS SQS.",
                    },
                ],
            )
        ]
        assert mock_sns_client.publish.call_args_list == [
            call(
                TopicArn="arn:aws:sns:us-east-1:testing:events",
                Message="hello",
                Subject="example-subject",
                MessageAttributes={
                    "bool": {"DataType": "String", "StringValue": "True"},
                    "number": {"DataType": "String", "StringValue": "12"},
                },
            )
        ]

    def test_subscribe_queue_end_to_end_sets_subscription_and_merges_policy(
        self, sns_instance, mock_sns_client, mock_sqs_client
    ):
        # Arrange
        exchange_name = "events"
        queue_name = "worker-queue"
        queue_url = f"https://sqs.us-east-1.amazonaws.com/123456789012/{queue_name}"
        queue_arn = "arn:aws:sqs:us-east-1:123456789012:worker-queue"
        topic_arn = "arn:aws:sns:us-east-1:testing:events"
        subscription_arn = "arn:aws:sns:us-east-1:testing:events:sub-1"
        existing_policy = {
            "Version": "2012-10-17",
            "Statement": [{"Sid": "ExistingStatement", "Action": "sqs:GetQueueAttributes"}],
        }

        mock_sns_client.create_topic.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "TopicArn": topic_arn,
        }
        mock_sns_client.subscribe.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "SubscriptionArn": subscription_arn,
        }

        def _queue_attrs_side_effect(QueueUrl, AttributeNames):
            assert QueueUrl == queue_url
            if AttributeNames == ["QueueArn"]:
                return {
                    "ResponseMetadata": {"HTTPStatusCode": 200},
                    "Attributes": {"QueueArn": queue_arn},
                }
            if AttributeNames == ["Policy"]:
                return {"Attributes": {"Policy": json.dumps(existing_policy)}}
            raise AssertionError("Unexpected SQS get_queue_attributes request")

        mock_sqs_client.get_queue_attributes.side_effect = _queue_attrs_side_effect

        # Act
        result = sns_instance.subscriptions.subscribe_queue(queue_name, exchange_name)

        # Assert
        assert result == subscription_arn
        assert sns_instance.subscriptions._subscription_arn_cache[
            f"{exchange_name}:{queue_name}"
        ] == subscription_arn
        assert mock_sns_client.subscribe.call_args_list == [
            call(
                TopicArn=topic_arn,
                Protocol="sqs",
                Endpoint=queue_arn,
                Attributes={"RawMessageDelivery": "true"},
                ReturnSubscriptionArn=True,
            )
        ]

        set_policy_call = mock_sqs_client.set_queue_attributes.call_args_list
        assert len(set_policy_call) == 1
        assert set_policy_call[0].kwargs["QueueUrl"] == queue_url
        policy_doc = json.loads(set_policy_call[0].kwargs["Attributes"]["Policy"])
        assert policy_doc["Statement"][0] == existing_policy["Statement"][0]
        assert policy_doc["Statement"][1] == {
            "Sid": "KombuManaged",
            "Effect": "Allow",
            "Principal": {"Service": "sns.amazonaws.com"},
            "Action": "SQS:SendMessage",
            "Resource": queue_arn,
            "Condition": {"ArnLike": {"aws:SourceArn": topic_arn}},
        }

    def test_unsubscribe_queue_end_to_end_removes_cache_entry(
        self, sns_instance, mock_sns_client
    ):
        # Arrange
        queue_name = "worker-queue"
        exchange_name = "events"
        cache_key = f"{exchange_name}:{queue_name}"
        subscription_arn = "arn:aws:sns:us-east-1:testing:events:sub-1"
        sns_instance.subscriptions._subscription_arn_cache[cache_key] = subscription_arn
        mock_sns_client.unsubscribe.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }

        # Act
        sns_instance.subscriptions.unsubscribe_queue(queue_name, exchange_name)

        # Assert
        assert cache_key not in sns_instance.subscriptions._subscription_arn_cache
        assert mock_sns_client.unsubscribe.call_args_list == [
            call(SubscriptionArn=subscription_arn)
        ]

    def test_publish_predefined_exchange_uses_sts_refreshed_client(
        self, sns_instance, mock_channel
    ):
        # Arrange
        exchange_name = "predefined-topic"
        topic_arn = "arn:aws:sns:eu-west-1:testing:predefined-topic"
        mock_channel.predefined_exchanges = {
            exchange_name: {"arn": topic_arn, "region": "eu-west-1"}
        }
        mock_channel.transport_options = {"sts_role_arn": "arn:aws:iam::123456789012:role/test"}
        mock_channel.is_sts_token_refresh_required.return_value = True
        mock_channel.get_sts_credentials.return_value = {
            "AccessKeyId": "sts-access",
            "SecretAccessKey": "sts-secret",
            "SessionToken": "sts-token",
            "Expiration": datetime.now(timezone.utc) + timedelta(minutes=30),
        }

        sts_sns_client = MagicMock(name="StsSnsClient")
        sts_sns_client.publish.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        mock_channel._new_boto_client.side_effect = [sts_sns_client]

        # Act
        sns_instance.publish(exchange_name=exchange_name, message="from-sts")

        # Assert
        assert mock_channel._new_boto_client.call_args_list == [
            call(
                service="sns",
                region="eu-west-1",
                access_key_id="sts-access",
                secret_access_key="sts-secret",
                session_token="sts-token",
            )
        ]
        assert sts_sns_client.publish.call_args_list == [
            call(TopicArn=topic_arn, Message="from-sts")
        ]
        assert sns_instance.sts_expiration == mock_channel.get_sts_credentials.return_value[
            "Expiration"
        ]
