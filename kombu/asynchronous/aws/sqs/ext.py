"""Amazon SQS boto3 interface."""


from __future__ import annotations

try:
    import boto3
except ImportError:
    boto3 = None

try:
    import sqs_extended_client
except ImportError:
    sqs_extended_client = None
