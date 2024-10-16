"""Amazon SQS boto3 interface."""


from __future__ import annotations

try:
    import boto3
    import sqs_extended_client
except ImportError:
    boto3 = None
    sqs_extended_client = None
