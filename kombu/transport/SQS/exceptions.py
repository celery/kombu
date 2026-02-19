"""AWS SQS and SNS exceptions."""

from __future__ import annotations


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
