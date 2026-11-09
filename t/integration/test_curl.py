from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Thread
from time import monotonic
from types import SimpleNamespace

import pytest

import t.skip
from kombu import Connection
from kombu.asynchronous import Hub, get_event_loop, set_event_loop
from kombu.asynchronous.http.curl import CurlClient

pycurl = pytest.importorskip('pycurl')
pytestmark = t.skip.if_pypy


@pytest.fixture
def stalled_http_server():
    release_stalled = Event()
    requests = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def do_POST(self):
            self.rfile.read(int(self.headers.get('Content-Length', '0')))
            self.do_GET()

        def do_GET(self):
            requests.append(self.path)
            if len(requests) == 2:
                # Keep the socket open until fixture teardown releases it.
                release_stalled.wait()
                return
            self.send_response(200)
            self.send_header('Content-Type', 'application/x-amz-json-1.0')
            self.send_header('Content-Length', '2')
            self.end_headers()
            self.wfile.write(b'{}')
            self.wfile.flush()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield SimpleNamespace(endpoint=f'http://127.0.0.1:{server.server_port}',
                              requests=requests)
    finally:
        release_stalled.set()
        server.shutdown()
        server.server_close()
        thread.join(2)


@pytest.fixture
def curl_hub():
    previous_hub = get_event_loop()
    hub = Hub()
    set_event_loop(hub)
    # Bound run_once even when no request callback ever fires.
    tick = hub.call_repeatedly(0.01, lambda: None)
    try:
        yield hub
    finally:
        tick.cancel()
        hub.close()
        set_event_loop(previous_hub)


def drain_until(hub, predicate):
    deadline = monotonic() + 4
    while not predicate() and monotonic() < deadline:
        hub.run_once()
    assert predicate(), 'HTTP callbacks did not complete before the deadline'


def test_sqs_polling_resumes_after_stalled_response(curl_hub, stalled_http_server, monkeypatch):
    """Time out a stalled request, reuse its curl handle and resume SQS polling."""
    boto3 = pytest.importorskip('boto3')
    from kombu.asynchronous.aws.connection import AsyncHTTPSConnection
    from kombu.asynchronous.aws.sqs.connection import AsyncSQSConnection

    endpoint = stalled_http_server.endpoint
    sqs_client = boto3.client(
        'sqs', endpoint_url=endpoint, region_name='us-east-1',
        aws_access_key_id='dummy', aws_secret_access_key='dummy',
    )
    responses = []
    try:
        with CurlClient(curl_hub, max_clients=1) as client, Connection(
            'sqs://dummy:dummy@',
            transport_options={
                'region': 'us-east-1',
                'wait_time_seconds': 0,
                'polling_interval': 0.01,
                'predefined_queues': {'probe': {'url': f'{endpoint}/dummy/probe'}},
            },
        ) as broker:
            channel = broker.channel()
            connection = AsyncSQSConnection(sqs_client, http_client=client)
            # Shorten the test deadline without modifying curl or on_prepare.
            monkeypatch.setattr(connection, 'get_http_connection', lambda: AsyncHTTPSConnection(
                timeout=0.2, http_client=client,
            ))
            monkeypatch.setattr(channel, 'asynsqs', lambda **kwargs: connection)
            parse_response = connection._on_list_ready

            def on_list_ready(*args):
                response = args[-1]
                responses.append(response)
                if len(responses) == 3:
                    channel._active_queues.clear()
                return parse_response(*args)

            monkeypatch.setattr(connection, '_on_list_ready', on_list_ready)
            channel._active_queues = ['probe']
            channel.basic_qos(prefetch_count=1)
            channel._schedule_queue('probe')
            drain_until(curl_hub, lambda: len(responses) == 3)
            assert [response.status for response in responses] == [200, 599, 200]
            assert responses[1].response.error.errno == pycurl.E_OPERATION_TIMEDOUT
            assert len(stalled_http_server.requests) == 3
            assert not client._pending
            assert len(client._free_list) == 1
            assert not client._fds
            assert channel.qos.can_consume()
    finally:
        sqs_client.close()
