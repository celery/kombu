from __future__ import annotations

from io import BytesIO
from unittest.mock import Mock, patch

import pytest
import urllib3

import t.skip
from kombu.asynchronous.http.urllib3_client import (Urllib3Client,
                                                    _get_pool_key_parts)


@t.skip.if_pypy
@pytest.mark.usefixtures('hub')
class test_Urllib3Client:
    class Client(Urllib3Client):
        urllib3 = Mock(name='urllib3')

    def test_max_clients_set(self):
        x = self.Client(max_clients=303)
        assert x.max_clients == 303

    def test_init(self):
        x = self.Client()
        assert x._pools is not None
        assert x._pending is not None
        assert x._timeout_check_tref

    def test_close(self):
        with patch(
                'kombu.asynchronous.http.urllib3_client.urllib3.PoolManager'
        ):
            x = self.Client()
            x._timeout_check_tref = Mock(name='timeout_check_tref')
            x.close()
            x._timeout_check_tref.cancel.assert_called_with()
            for pool in x._pools.values():
                pool.close.assert_called_with()

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
        with patch(
                'kombu.asynchronous.http.urllib3_client.urllib3.PoolManager'
        ) as _pool_manager:
            x = self.Client()
            request = Mock(
                name='request',
                method='GET',
                url='http://example.com',
                headers={},
                body=None,
                follow_redirects=True,
                auth_username=None,
                auth_password=None,
                user_agent=None,
                use_gzip=False,
                network_interface=None,
                validate_cert=True,
                ca_certs=None,
                client_cert=None,
                client_key=None,
                proxy_host=None,
                proxy_port=None,
                proxy_username=None,
                proxy_password=None,
                on_ready=Mock(name='on_ready')
            )
            response = Mock(
                name='response',
                status=200,
                headers={},
                data=b'content'
            )
            response.geturl.return_value = 'http://example.com'
            _pool_manager.return_value.request.return_value = response

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

    def test_process_request_with_error(self):
        with patch(
                'kombu.asynchronous.http.urllib3_client.urllib3.PoolManager'
        ) as _pool_manager:
            x = self.Client()
            x.close()
            request = Mock(
                name='request',
                method='GET',
                url='http://example.com',
                headers={},
                body=None,
                follow_redirects=True,
                auth_username=None,
                auth_password=None,
                user_agent=None,
                use_gzip=False,
                network_interface=None,
                validate_cert=True,
                ca_certs=None,
                client_cert=None,
                client_key=None,
                proxy_host=None,
                proxy_port=None,
                proxy_username=None,
                proxy_password=None,
                on_ready=Mock(name='on_ready')
            )
            _pool_manager.return_value.request.side_effect = urllib3.exceptions.HTTPError("Test Error")

            x._process_request(request)
            request.on_ready.assert_called()
            called_response = request.on_ready.call_args[0][0]
            assert called_response.code == 599
            assert called_response.error is not None
            assert called_response.error.message == "Test Error"

    def test_on_readable_on_writable(self):
        x = self.Client()
        x.on_readable(Mock(name='fd'))
        x.on_writable(Mock(name='fd'))

    def test_get_pool_with_proxy(self):
        with patch(
                'kombu.asynchronous.http.urllib3_client.urllib3.ProxyManager'
        ) as _proxy_manager:
            x = self.Client()
            request = Mock(
                name='request',
                proxy_host='proxy.example.com',
                proxy_port=8080,
                proxy_username='user',
                proxy_password='pass'
            )
            x.get_pool(request)
            _proxy_manager.assert_called_with(
                proxy_url='proxy.example.com:8080',
                num_pools=x.max_clients,
                proxy_headers=urllib3.make_headers(
                    proxy_basic_auth="user:pass"
                )
            )

    def test_get_pool_without_proxy(self):
        with patch(
                'kombu.asynchronous.http.urllib3_client.urllib3.PoolManager'
        ) as _pool_manager:
            x = self.Client()
            request = Mock(name='request', proxy_host=None)
            x.get_pool(request)
            _pool_manager.assert_called_with(num_pools=x.max_clients)

    def test_process_request_with_proxy(self):
        with patch(
                'kombu.asynchronous.http.urllib3_client.urllib3.ProxyManager'
        ) as _proxy_manager:
            x = self.Client()
            request = Mock(
                name='request',
                method='GET',
                url='http://example.com',
                headers={},
                body=None,
                follow_redirects=True,
                proxy_host='proxy.example.com',
                proxy_port=8080,
                proxy_username='user',
                proxy_password='pass',
                on_ready=Mock(name='on_ready')
            )
            response = Mock(
                name='response',
                status=200,
                headers={},
                data=b'content'
            )
            response.geturl.return_value = 'http://example.com'
            _proxy_manager.return_value.request.return_value = response

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
                    called_response.buffer.getvalue()
                    == response_obj.buffer.getvalue()
            )
            assert called_response.effective_url == response_obj.effective_url
            assert called_response.error == response_obj.error

    def test_pool_key_parts(self):
        request = Mock(
            name='request',
            method='GET',
            url='http://example.com',
            headers={},
            body=None,
            network_interface='test',
            validate_cert=False,
            ca_certs='test0.pem',
            client_cert='test1.pem',
            client_key='some_key',
        )
        pool_key = _get_pool_key_parts(request)
        assert pool_key == [
            "interface=test",
            "validate_cert=False",
            "ca_certs=test0.pem",
            "client_cert=test1.pem",
            "client_key=some_key"
        ]
