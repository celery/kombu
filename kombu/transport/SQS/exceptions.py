"""AWS SQS and SNS exceptions."""

from __future__ import annotations

from kombu.exceptions import KombuError


class UndefinedQueueException(KombuError):
    """Predefined queues are being used and an undefined queue was used."""


class UndefinedExchangeException(KombuError):
    """Predefined exchanges are being used and an undefined exchange/SNS topic was used."""


class InvalidQueueException(KombuError):
    """Predefined queues are being used and configuration is not valid."""


class AccessDeniedQueueException(KombuError):
    """Raised when access to the AWS queue is denied.

    This may occur if the permissions are not correctly set or the
    credentials are invalid.
    """


class DoesNotExistQueueException(KombuError):
    """The specified queue doesn't exist."""


class UnableToSubscribeQueueToTopicException(KombuError):
    """Raised when unable to subscribe a queue to an SNS topic."""
