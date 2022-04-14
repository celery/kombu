from typing import Any, Optional

from kombu.asynchronous.aws.sqs.connection import AsyncSQSConnection


def connect_sqs(
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    **kwargs: Any
) -> AsyncSQSConnection:
    """Return async connection to Amazon SQS."""
    from .sqs.connection import AsyncSQSConnection
    return AsyncSQSConnection(
        aws_access_key_id, aws_secret_access_key, **kwargs
    )
