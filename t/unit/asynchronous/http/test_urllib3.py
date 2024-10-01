from __future__ import annotations

from io import BytesIO
from unittest.mock import Mock, patch

import pytest

import t.skip
from kombu.asynchronous.http.urllib3_client import Urllib3Client


@t.skip.if_pypy
@pytest.mark.usefixtures('hub')
class test_Urllib3Client:
    class Client(Urllib3Client):
        Urllib3 = Mock(name='Urllib3')

    def test_max_clients_set(self):
        x = self.Client(max_clients=303)
        assert x.max_clients == 303

    def test_init(self):
        with patch(
                'kombu.asynchronous.http.urllib3_client.urllib3.PoolManager'
        ) as _PoolManager:
            x = self.Client()
            assert x._http is not None
            assert x._pending is not None
            assert x._timeout_check_tref

            _PoolManager.assert_called_with(maxsize=x.max_clients)

    def test_close(self):
        with patch(
                'kombu.asynchronous.http.urllib3_client.urllib3.PoolManager'
        ):
            x = self.Client()
            x._timeout_check_tref = Mock(name='timeout_check_tref')
            x.close()
            x._timeout_check_tref.cancel.assert_called_with()
            x._http.clear.assert_called_with()

    def test_add_request(self):
        with patch(
                'kombu.asynchronous.http.urllib3_client.urllib3.PoolManager'
        ):
            x = self.Client()
            x._process_queue = Mock(name='_process_queue')
            request = Mock(name='request')
            x.add_request(request)
            assert request in x._pending
            x._process_queue.assert_called_with()

    def test_timeout_check(self):
        with patch(
                'kombu.asynchronous.http.urllib3_client.urllib3.PoolManager'
        ):
            hub = Mock(name='hub')
            x = self.Client(hub)
            x._process_pending_requests = Mock(name='process_pending')
            x._timeout_check()
            x._process_pending_requests.assert_called_with()

    def test_process_request(self):
        with (patch(
                'kombu.asynchronous.http.urllib3_client.urllib3.PoolManager'
        ) as _PoolManager):
            x = self.Client()
            request = Mock(
                name='request',
                method='GET',
                url='http://example.com',
                headers={},
                body=None
            )
            response = Mock(
                name='response',
                status=200,
                headers={},
                data=b'content'
            )
            response.geturl.return_value = 'http://example.com'
            _PoolManager.return_value.request.return_value = response

            x._process_request(request)
            response_obj = x.Response(
                request=request,
                code=200,
                headers={},
                buffer=BytesIO(b'content'),
                effective_url='http://example.com',
                error=None
            )
            request.on_ready.assert_called()
            called_response = request.on_ready.call_args[0][0]
            assert called_response.code == response_obj.code
            assert called_response.headers == response_obj.headers
            assert (
                    called_response.buffer.getvalue() ==
                    response_obj.buffer.getvalue()
            )
            assert called_response.effective_url == response_obj.effective_url
            assert called_response.error == response_obj.error

    def test_on_readable_on_writable(self):
        x = self.Client()
        x.on_readable(Mock(name='fd'))
        x.on_writable(Mock(name='fd'))
