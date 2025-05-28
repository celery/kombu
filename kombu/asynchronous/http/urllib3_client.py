"""HTTP Client using urllib3"""

from __future__ import annotations

import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

try:
    import urllib3
except ImportError:  # pragma: no cover
    urllib3 = None
else:
    from urllib3.util import Timeout, Url, make_headers

from kombu.asynchronous.hub import Hub, get_event_loop
from kombu.exceptions import HttpError

from .base import BaseClient

__all__ = ('Urllib3Client',)

DEFAULT_USER_AGENT = 'Mozilla/5.0 (compatible; urllib3)'
EXTRA_METHODS = frozenset(['DELETE', 'OPTIONS', 'PATCH'])


class Urllib3Client(BaseClient):
    """Urllib3 HTTP Client (using urllib3 with thread pool)."""

    def __init__(self, hub: Hub | None = None, max_clients: int = 10):
        if urllib3 is None:
            raise ImportError('The urllib3 client requires the urllib3 library.')
        hub = hub or get_event_loop()
        super().__init__(hub)
        self.max_clients = max_clients

        # Thread pool for concurrent requests
        self._executor = ThreadPoolExecutor(max_workers=max_clients)
        self._pending = deque()
        self._active_requests = {}  # Track active requests
        self._request_lock = threading.RLock()  # Thread safety

        self._timeout_check_tref = self.hub.call_repeatedly(
            1.0, self._timeout_check,
        )

    def close(self):
        """Close the client and all connection pools."""
        self._timeout_check_tref.cancel()
        self._executor.shutdown(wait=False)

    def add_request(self, request):
        """Add a request to the pending queue."""
        with self._request_lock:
            self._pending.append(request)
        self._process_queue()
        return request

    def _get_pool(self, request):
        """Get or create a connection pool for the request."""
        # Prepare connection kwargs
        conn_kwargs = {}

        # Network Interface
        if request.network_interface:
            conn_kwargs['source_address'] = (request.network_interface, 0)

        # SSL Verification
        conn_kwargs['cert_reqs'] = 'CERT_REQUIRED' if request.validate_cert else 'CERT_NONE'

        # CA Certificates
        if request.ca_certs is not None:
            conn_kwargs['ca_certs'] = request.ca_certs
        elif request.validate_cert is True:
            try:
                import certifi
                conn_kwargs['ca_certs'] = certifi.where()
            except ImportError:
                pass

        # Client Certificates
        if request.client_cert is not None:
            conn_kwargs['cert_file'] = request.client_cert
        if request.client_key is not None:
            conn_kwargs['key_file'] = request.client_key

        # Handle proxy configuration
        if request.proxy_host:
            conn_kwargs['_proxy'] = Url(
                scheme=None,
                host=request.proxy_host,
                port=request.proxy_port,
            ).url

            if request.proxy_username:
                conn_kwargs['_proxy_headers'] = make_headers(
                    proxy_basic_auth=f"{request.proxy_username}:{request.proxy_password or ''}"
                )

        pool = urllib3.connection_from_url(request.url, **conn_kwargs)
        return pool

    def _timeout_check(self):
        """Check for timeouts and process pending requests."""
        self._process_queue()

    def _process_queue(self):
        """Process the request queue in a thread-safe manner."""
        with self._request_lock:
            # Only process if we have pending requests and available capacity
            if not self._pending or len(self._active_requests) >= self.max_clients:
                return

            # Process as many pending requests as we have capacity for
            while self._pending and len(self._active_requests) < self.max_clients:
                request = self._pending.popleft()
                request_id = id(request)
                self._active_requests[request_id] = request
                # Submit the request to the thread pool
                future = self._executor.submit(self._execute_request, request)
                future.add_done_callback(
                    lambda f, req_id=request_id: self._request_complete(req_id)
                )

    def _request_complete(self, request_id):
        """Mark a request as complete and process the next pending request."""
        with self._request_lock:
            if request_id in self._active_requests:
                del self._active_requests[request_id]

        # Process more requests if available
        self._process_queue()

    def _execute_request(self, request):
        """Execute a single request using urllib3"""
        # Prepare headers
        headers = dict(request.headers)
        headers.update(
            make_headers(
                user_agent=request.user_agent or DEFAULT_USER_AGENT,
                accept_encoding=request.use_gzip,
            )
        )

        # Authentication
        if request.auth_username is not None:
            auth_header = make_headers(
                basic_auth=f"{request.auth_username}:{request.auth_password or ''}"
            )
            headers.update(auth_header)

        # Process request body
        body = None
        if request.method in ('POST', 'PUT') and request.body:
            body = request.body if isinstance(request.body, bytes) else request.body.encode('utf-8')

        # Make the request using urllib3
        try:
            pool = self._get_pool(request)

            # Execute the request
            response = pool.request(
                method=request.method,
                url=request.url,
                headers=headers,
                body=body,
                preload_content=True,  # We want to preload content for compatibility
                redirect=request.follow_redirects,
                retries=False,  # Handle redirects manually to match pycurl behavior
            )

            # Process response
            buffer = BytesIO(response.data)
            response_obj = self.Response(
                request=request,
                code=response.status,
                headers=response.headers,
                buffer=buffer,
                effective_url=response.geturl() if hasattr(response, 'geturl') else request.url,
                error=None
            )
        except Exception as e:
            # Handle any errors
            response_obj = self.Response(
                request=request,
                code=599,
                headers={},
                buffer=None,
                effective_url=None,
                error=HttpError(599, str(e))
            )

        # Notify request completion
        request.on_ready(response_obj)

    def on_readable(self, fd):
        """Compatibility method for the event loop."""
        pass

    def on_writable(self, fd):
        """Compatibility method for the event loop."""
        pass
