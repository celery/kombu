"""Amazon SQS Connection."""

from __future__ import annotations

from kombu.asynchronous import get_event_loop
from kombu.asynchronous.aws.connection import AsyncAWSQueryConnection

from .ext import boto3

__all__ = ('AsyncSQSConnection',)


class AsyncSQSConnection(AsyncAWSQueryConnection):
    """Async SQS Connection."""

    def __init__(self, sqs_connection, debug=0, region=None, **kwargs):
        if boto3 is None:
            raise ImportError('boto3 is not installed')
        super().__init__(
            sqs_connection,
            region_name=region,
            debug=debug,
            **kwargs
        )
        self.hub = kwargs.get('hub') or get_event_loop()

    def _async_sqs_request(self, api, callback, *args, **kwargs):
        """Makes an asynchronous request to an SQS API.

        Arguments:
        ---------
        api -- The name of the API, e.g. 'receive_message'.
        callback -- The callback to pass the response to when it is available.
        *args, **kwargs -- The arguments and keyword arguments to pass to the
            SQS API. Those are API dependent and can be found in the boto3
            documentation.
        """
        # Define a method to execute the SQS API synchronously.
        def sqs_request(api, callback, args, kwargs):
            method = getattr(self.sqs_connection, api)
            resp = method(*args, **kwargs)
            if callback:
                callback(resp)

        # Hand off the request to the event loop to execute it asynchronously.
        self.hub.call_soon(sqs_request, api, callback, args, kwargs)

    def receive_message(
        self, queue_url, number_messages=1, visibility_timeout=None,
        attributes=('ApproximateReceiveCount',), wait_time_seconds=None,
        callback=None
    ):
        kwargs = {
            "QueueUrl": queue_url,
            "MaxNumberOfMessages": number_messages,
            "MessageAttributeNames": attributes,
            "WaitTimeSeconds": wait_time_seconds,
        }
        if visibility_timeout:
            kwargs["VisibilityTimeout"] = visibility_timeout

        return self._async_sqs_request('receive_message', callback, **kwargs)

    def delete_message(self, queue_url, receipt_handle, callback=None):
        return self._async_sqs_request('delete_message', callback,
                                       QueueUrl=queue_url,
                                       ReceiptHandle=receipt_handle)
