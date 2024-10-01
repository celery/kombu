from __future__ import annotations

from collections import deque
from io import BytesIO

import urllib3

from kombu.asynchronous.hub import Hub, get_event_loop
from kombu.exceptions import HttpError

from .base import BaseClient

__all__ = ('Urllib3Client',)

DEFAULT_USER_AGENT = 'Mozilla/5.0 (compatible; urllib3)'
EXTRA_METHODS = frozenset(['DELETE', 'OPTIONS', 'PATCH'])


class Urllib3Client(BaseClient):
    """Urllib3 HTTP Client."""

    def __init__(self, hub: Hub | None = None, max_clients: int = 10):
        hub = hub or get_event_loop()
        super().__init__(hub)
        self.max_clients = max_clients
        self._http = urllib3.PoolManager(maxsize=max_clients)
        self._pending = deque()
        self._timeout_check_tref = self.hub.call_repeatedly(
            1.0, self._timeout_check,
        )

    def close(self):
        self._timeout_check_tref.cancel()
        self._http.clear()

    def add_request(self, request):
        self._pending.append(request)
        self._process_queue()
        return request

    def _timeout_check(self):
        self._process_pending_requests()

    def _process_pending_requests(self):
        while self._pending:
            request = self._pending.popleft()
            self._process_request(request)

    def _process_request(self, request):
        method = request.method
        url = request.url
        headers = request.headers
        body = request.body

        try:
            response = self._http.request(
                method,
                url,
                headers=headers,
                body=body,
                preload_content=False
            )
            buffer = BytesIO(response.data)
            response_obj = self.Response(
                request=request,
                code=response.status,
                headers=response.headers,
                buffer=buffer,
                effective_url=response.geturl(),
                error=None
            )
        except urllib3.exceptions.HTTPError as e:
            response_obj = self.Response(
                request=request,
                code=599,
                headers={},
                buffer=None,
                effective_url=None,
                error=HttpError(599, str(e))
            )

        request.on_ready(response_obj)

    def _process_queue(self):
        self._process_pending_requests()

    def on_readable(self, fd):
        pass

    def on_writable(self, fd):
        pass

    def _setup_request(self, curl, request, buffer, headers):
        pass
