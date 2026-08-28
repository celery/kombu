from __future__ import annotations

import sys
from unittest.mock import MagicMock, Mock, patch

import pytest

from kombu.asynchronous.http.urllib3_client import Urllib3Client

pytest.importorskip('urllib3')


class test_Urllib3Client:

    def setup_method(self):
        self.hub = Mock(name='hub')
        self.hub.call_repeatedly.return_value = Mock()

        # Patch ThreadPoolExecutor in urllib3_client's own namespace to prevent
        # actual thread creation.  Patching 'concurrent.futures.ThreadPoolExecutor'
        # would NOT work because urllib3_client already captured the reference via
        # `from concurrent.futures import ThreadPoolExecutor` at import time.
        self.executor_patcher = patch(
            'kombu.asynchronous.http.urllib3_client.ThreadPoolExecutor'
        )
        self.mock_executor_cls = self.executor_patcher.start()
        self.mock_executor = Mock()
        self.mock_executor_cls.return_value = self.mock_executor

        # Create the client
        self.client = Urllib3Client(self.hub)

    def teardown_method(self):
        self.executor_patcher.stop()
        self.client.close()

    def test_client_creation(self):
        assert self.client.hub is self.hub
        assert self.client.max_clients == 10
        assert isinstance(self.client._pending, type(self.client._pending))
        assert isinstance(self.client._active_requests, dict)
        assert self.hub.call_repeatedly.called
        # Verify that the executor was created via the mock (not a real thread pool)
        self.mock_executor_cls.assert_called_once_with(max_workers=10)

    def _setup_pool_mock(self):
        """Helper to set up a pool mock that can be used across tests"""
        response_mock = Mock()
        response_mock.status = 200
        response_mock.headers = {'Content-Type': 'text/plain'}
        response_mock.data = b'OK'
        response_mock.geturl = lambda: 'http://example.com/redirected'

        pool_mock = Mock()
        pool_mock.request.return_value = response_mock

        return pool_mock

    @pytest.mark.parametrize('use_gzip', [True, False])
    def test_add_request(self, use_gzip):
        pool_mock = self._setup_pool_mock()

        with patch.object(self.client, '_get_pool', return_value=pool_mock):
            request = Mock()
            request.method = 'GET'
            request.url = 'http://example.com'
            request.headers = {}
            request.body = None
            request.proxy_host = None
            request.proxy_port = None
            request.network_interface = None
            request.validate_cert = True
            request.ca_certs = None
            request.client_cert = None
            request.client_key = None
            request.auth_username = None
            request.auth_password = None
            request.use_gzip = use_gzip
            request.follow_redirects = True

            # Add request and directly execute it
            self.client.add_request(request)

            # Execute the request directly
            with patch.object(self.client, '_request_complete'):
                self.client._execute_request(request)

            # Check that the request was processed
            pool_mock.request.assert_called_once()
            request.on_ready.assert_called_once()

    def test_request_with_auth(self):
        pool_mock = self._setup_pool_mock()

        with patch.object(self.client, '_get_pool', return_value=pool_mock):
            request = Mock()
            request.method = 'GET'
            request.url = 'http://example.com'
            request.headers = {}
            request.body = None
            request.proxy_host = None
            request.proxy_port = None
            request.network_interface = None
            request.validate_cert = True
            request.ca_certs = None
            request.client_cert = None
            request.client_key = None
            request.auth_username = 'user'
            request.auth_password = 'pass'
            request.use_gzip = False
            request.follow_redirects = True

            # Process the request
            self.client.add_request(request)
            with patch.object(self.client, '_request_complete'):
                self.client._execute_request(request)

            # Verify authentication was added
            call_args = pool_mock.request.call_args[1]
            assert 'headers' in call_args

            # Check for basic auth in headers
            headers = call_args['headers']
            auth_header_present = False
            for header, value in headers.items():
                if header.lower() == 'authorization' and 'basic' in value.lower():
                    auth_header_present = True
                    break

            # If we can't find it directly, look for auth in header creation
            if not auth_header_present:
                with patch('kombu.asynchronous.http.urllib3_client.make_headers') as mock_make_headers:
                    mock_make_headers.return_value = {'Authorization': 'Basic dXNlcjpwYXNz'}
                    self.client._execute_request(request)
                    # Check if basic_auth was used in make_headers
                    for call_args in mock_make_headers.call_args_list:
                        if 'basic_auth' in call_args[1]:
                            assert 'user:pass' in call_args[1]['basic_auth']
                            auth_header_present = True

            assert auth_header_present, "No authentication header was added"

    def test_request_with_proxy(self):
        pool_mock = self._setup_pool_mock()

        # We need to patch ProxyManager specifically
        with patch('urllib3.ProxyManager', return_value=pool_mock):
            request = Mock()
            request.method = 'GET'
            request.url = 'http://example.com'
            request.headers = {}
            request.body = None
            request.proxy_host = 'proxy.example.com'
            request.proxy_port = 8080
            request.proxy_username = 'proxyuser'
            request.proxy_password = 'proxypass'
            request.network_interface = None
            request.validate_cert = True
            request.ca_certs = None
            request.client_cert = None
            request.client_key = None
            request.auth_username = None
            request.use_gzip = False
            request.follow_redirects = True

            # Instead of patching _pools, patch _get_pool directly
            with patch.object(self.client, '_get_pool', return_value=pool_mock):
                self.client.add_request(request)
                with patch.object(self.client, '_request_complete'):
                    self.client._execute_request(request)

            # We just need to verify the pool was used
            pool_mock.request.assert_called()

    def test_request_error_handling(self):
        pool_mock = Mock()
        pool_mock.request.side_effect = Exception("Connection error")

        with patch.object(self.client, '_get_pool', return_value=pool_mock):
            request = Mock()
            request.method = 'GET'
            request.url = 'http://example.com'
            request.headers = {}
            request.body = None
            request.proxy_host = None
            request.proxy_port = None
            request.network_interface = None
            request.validate_cert = True
            request.ca_certs = None
            request.client_cert = None
            request.client_key = None
            request.auth_username = None
            request.use_gzip = False
            request.follow_redirects = True

            self.client.add_request(request)
            # Reset on_ready mock to clear any previous calls
            request.on_ready.reset_mock()

            with patch.object(self.client, '_request_complete'):
                self.client._execute_request(request)

            # Verify error response was created
            request.on_ready.assert_called_once()
            response = request.on_ready.call_args[0][0]
            assert response.code == 599
            assert response.error is not None

    def test_max_clients_limit(self):
        # Create a client with low max_clients to test capacity limiting.
        # The executor patcher from setup_method already patches the module-level
        # ThreadPoolExecutor, so this second instantiation also gets the mock.
        client = Urllib3Client(self.hub, max_clients=2)
        client._timeout_check_tref = Mock()

        # Mock _execute_request to avoid actual execution
        with patch.object(client, '_execute_request'):
            # Add multiple requests but patch _process_queue to control behavior
            # original_process_queue = client._process_queue

            def controlled_process_queue():
                # Custom queue processing logic for testing
                with client._request_lock:
                    # Move only 2 requests from pending to active
                    while client._pending and len(client._active_requests) < 2:
                        request = client._pending.popleft()
                        request_id = id(request)
                        client._active_requests[request_id] = request

            client._process_queue = controlled_process_queue

            # Create and add test requests
            requests = [Mock() for _ in range(5)]

            # Add the first 2 requests - these should become active
            for i in range(2):
                client.add_request(requests[i])

            # Check state: 2 active, 0 pending
            assert len(client._active_requests) == 2
            assert len(client._pending) == 0

            # Add 3 more requests - these should remain pending
            for i in range(2, 5):
                client.add_request(requests[i])

            # Check state: 2 active, 3 pending
            assert len(client._active_requests) == 2
            assert len(client._pending) == 3

            # Simulate completion of a request
            req_id = next(iter(client._active_requests.keys()))
            client._request_complete(req_id)

            # After completing one request and processing queue,
            # we should have 2 active and 2 pending
            assert len(client._active_requests) <= 2
            assert len(client._pending) <= 3
            assert len(client._active_requests) + len(client._pending) == 4

        client.close()

    def test_import_error_without_urllib3(self):
        """Test that Urllib3Client raises ImportError when urllib3 is not available."""
        import kombu.asynchronous.http.urllib3_client as mod
        original = mod.urllib3
        mod.urllib3 = None
        try:
            with pytest.raises(ImportError, match='urllib3'):
                Urllib3Client(self.hub)
        finally:
            mod.urllib3 = original

    def test_get_pool_with_network_interface(self):
        """Test _get_pool uses source_address when network_interface is set."""
        import urllib3 as urllib3_mod

        request = Mock()
        request.url = 'http://example.com'
        request.network_interface = '10.0.0.1'
        request.validate_cert = False
        request.ca_certs = None
        request.client_cert = None
        request.client_key = None
        request.proxy_host = None

        with patch.object(urllib3_mod, 'connection_from_url') as mock_conn:
            mock_conn.return_value = Mock()
            self.client._get_pool(request)
            call_kwargs = mock_conn.call_args[1]
            assert call_kwargs['source_address'] == ('10.0.0.1', 0)

    def test_get_pool_with_custom_ca_certs(self):
        """Test _get_pool uses custom ca_certs when provided."""
        import urllib3 as urllib3_mod

        request = Mock()
        request.url = 'http://example.com'
        request.network_interface = None
        request.validate_cert = True
        request.ca_certs = '/path/to/cacerts.pem'
        request.client_cert = None
        request.client_key = None
        request.proxy_host = None

        with patch.object(urllib3_mod, 'connection_from_url') as mock_conn:
            mock_conn.return_value = Mock()
            self.client._get_pool(request)
            call_kwargs = mock_conn.call_args[1]
            assert call_kwargs['ca_certs'] == '/path/to/cacerts.pem'

    def test_get_pool_with_certifi_fallback(self):
        """Test _get_pool falls back to certifi when validate_cert=True and no ca_certs."""
        import urllib3 as urllib3_mod

        request = Mock()
        request.url = 'http://example.com'
        request.network_interface = None
        request.validate_cert = True
        request.ca_certs = None
        request.client_cert = None
        request.client_key = None
        request.proxy_host = None

        certifi_mock = MagicMock()
        certifi_mock.where.return_value = '/certifi/cacert.pem'

        with patch.object(urllib3_mod, 'connection_from_url') as mock_conn:
            mock_conn.return_value = Mock()
            with patch.dict(sys.modules, {'certifi': certifi_mock}):
                self.client._get_pool(request)
            call_kwargs = mock_conn.call_args[1]
            assert call_kwargs['ca_certs'] == '/certifi/cacert.pem'

    def test_get_pool_with_certifi_not_available(self):
        """Test _get_pool proceeds gracefully when certifi is not available."""
        import urllib3 as urllib3_mod

        request = Mock()
        request.url = 'http://example.com'
        request.network_interface = None
        request.validate_cert = True
        request.ca_certs = None
        request.client_cert = None
        request.client_key = None
        request.proxy_host = None

        with patch.object(urllib3_mod, 'connection_from_url') as mock_conn:
            mock_conn.return_value = Mock()
            with patch.dict(sys.modules, {'certifi': None}):
                self.client._get_pool(request)
            # Should succeed without ca_certs
            mock_conn.assert_called_once()

    def test_get_pool_with_client_cert_and_key(self):
        """Test _get_pool sets cert_file and key_file when client_cert/key provided."""
        import urllib3 as urllib3_mod

        request = Mock()
        request.url = 'http://example.com'
        request.network_interface = None
        request.validate_cert = False
        request.ca_certs = None
        request.client_cert = '/path/to/client.crt'
        request.client_key = '/path/to/client.key'
        request.proxy_host = None

        with patch.object(urllib3_mod, 'connection_from_url') as mock_conn:
            mock_conn.return_value = Mock()
            self.client._get_pool(request)
            call_kwargs = mock_conn.call_args[1]
            assert call_kwargs['cert_file'] == '/path/to/client.crt'
            assert call_kwargs['key_file'] == '/path/to/client.key'

    def test_get_pool_with_proxy_and_credentials(self):
        """Test _get_pool sets proxy headers when proxy credentials provided."""
        import urllib3 as urllib3_mod

        request = Mock()
        request.url = 'http://example.com'
        request.network_interface = None
        request.validate_cert = False
        request.ca_certs = None
        request.client_cert = None
        request.client_key = None
        request.proxy_host = 'proxy.example.com'
        request.proxy_port = 3128
        request.proxy_username = 'proxyuser'
        request.proxy_password = 'proxypass'

        with patch.object(urllib3_mod, 'connection_from_url') as mock_conn:
            mock_conn.return_value = Mock()
            self.client._get_pool(request)
            call_kwargs = mock_conn.call_args[1]
            assert '_proxy' in call_kwargs
            assert '_proxy_headers' in call_kwargs

    def test_get_pool_with_proxy_no_credentials(self):
        """Test _get_pool sets proxy without headers when no proxy credentials provided."""
        import urllib3 as urllib3_mod

        request = Mock()
        request.url = 'http://example.com'
        request.network_interface = None
        request.validate_cert = False
        request.ca_certs = None
        request.client_cert = None
        request.client_key = None
        request.proxy_host = 'proxy.example.com'
        request.proxy_port = 3128
        request.proxy_username = None  # No credentials

        with patch.object(urllib3_mod, 'connection_from_url') as mock_conn:
            mock_conn.return_value = Mock()
            self.client._get_pool(request)
            call_kwargs = mock_conn.call_args[1]
            assert '_proxy' in call_kwargs
            assert '_proxy_headers' not in call_kwargs

    def test_timeout_check_calls_process_queue(self):
        """Test that _timeout_check triggers _process_queue."""
        with patch.object(self.client, '_process_queue') as mock_pq:
            self.client._timeout_check()
            mock_pq.assert_called_once()

    def test_request_complete_missing_id(self):
        """Test _request_complete does not raise when request_id is not tracked."""
        # Should not raise even when request_id is not in _active_requests
        self.client._request_complete(99999)

    def test_execute_request_with_string_body(self):
        """Test _execute_request encodes string body to bytes for POST."""
        pool_mock = self._setup_pool_mock()

        with patch.object(self.client, '_get_pool', return_value=pool_mock):
            request = Mock()
            request.method = 'POST'
            request.url = 'http://example.com'
            request.headers = {}
            request.body = 'hello world'  # String body, not bytes
            request.proxy_host = None
            request.network_interface = None
            request.validate_cert = False
            request.ca_certs = None
            request.client_cert = None
            request.client_key = None
            request.auth_username = None
            request.use_gzip = False
            request.follow_redirects = True
            request.user_agent = None

            with patch.object(self.client, '_request_complete'):
                self.client._execute_request(request)

            call_kwargs = pool_mock.request.call_args[1]
            assert call_kwargs['body'] == b'hello world'
            request.on_ready.assert_called_once()

    def test_execute_request_with_empty_post_body(self):
        """Test _execute_request sends explicit empty bytes for empty POST bodies."""
        pool_mock = self._setup_pool_mock()

        with patch.object(self.client, '_get_pool', return_value=pool_mock):
            request = Mock()
            request.method = 'POST'
            request.url = 'http://example.com'
            request.headers = {}
            request.body = None
            request.proxy_host = None
            request.network_interface = None
            request.validate_cert = False
            request.ca_certs = None
            request.client_cert = None
            request.client_key = None
            request.auth_username = None
            request.use_gzip = False
            request.follow_redirects = True
            request.user_agent = None

            with patch.object(self.client, '_request_complete'):
                self.client._execute_request(request)

            call_kwargs = pool_mock.request.call_args[1]
            assert call_kwargs['body'] == b''
            request.on_ready.assert_called_once()

    @pytest.mark.parametrize(
        ('follow_redirects', 'expected_redirect_retries'),
        [(True, 5), (False, 0)],
    )
    def test_execute_request_redirects_follow_flag(self, follow_redirects, expected_redirect_retries):
        """Test _execute_request maps follow_redirects to urllib3 retry redirect policy."""
        pool_mock = self._setup_pool_mock()

        with patch.object(self.client, '_get_pool', return_value=pool_mock):
            request = Mock()
            request.method = 'GET'
            request.url = 'http://example.com'
            request.headers = {}
            request.body = None
            request.proxy_host = None
            request.network_interface = None
            request.validate_cert = False
            request.ca_certs = None
            request.client_cert = None
            request.client_key = None
            request.auth_username = None
            request.use_gzip = False
            request.follow_redirects = follow_redirects
            request.user_agent = None

            with patch.object(self.client, '_request_complete'):
                self.client._execute_request(request)

            call_kwargs = pool_mock.request.call_args[1]
            assert call_kwargs['redirect'] is follow_redirects
            assert call_kwargs['retries'].redirect == expected_redirect_retries
            request.on_ready.assert_called_once()

    def test_execute_request_urllib3_http_error(self):
        """Test _execute_request handles urllib3.exceptions.HTTPError."""
        import urllib3.exceptions

        pool_mock = Mock()
        pool_mock.request.side_effect = urllib3.exceptions.HTTPError('connection failed')

        with patch.object(self.client, '_get_pool', return_value=pool_mock):
            request = Mock()
            request.method = 'GET'
            request.url = 'http://example.com'
            request.headers = {}
            request.body = None
            request.proxy_host = None
            request.network_interface = None
            request.validate_cert = False
            request.ca_certs = None
            request.client_cert = None
            request.client_key = None
            request.auth_username = None
            request.use_gzip = False
            request.follow_redirects = True
            request.user_agent = None

            with patch.object(self.client, '_request_complete'):
                self.client._execute_request(request)

            request.on_ready.assert_called_once()
            response = request.on_ready.call_args[0][0]
            assert response.code == 599
            assert response.error is not None
            assert 'connection failed' in str(response.error)

    def test_on_readable(self):
        """Test on_readable is a no-op compatibility method."""
        # Should not raise
        self.client.on_readable(5)

    def test_on_writable(self):
        """Test on_writable is a no-op compatibility method."""
        # Should not raise
        self.client.on_writable(5)
