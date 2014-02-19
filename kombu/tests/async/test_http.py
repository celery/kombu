from __future__ import absolute_import

from amqp import promise
from kombu.async import Hub
from kombu.async import http

from kombu.tests.case import Case, Mock


class test_Request(Case):

    def test_init(self):
        x = http.Request('http://foo', method='POST')
        self.assertEqual(x.url, 'http://foo')
        self.assertEqual(x.method, 'POST')

        x = http.Request('x', max_redirects=100)
        self.assertEqual(x.max_redirects, 100)

        self.assertIsInstance(x.headers, http.Headers)
        h = http.Headers()
        x = http.Request('x', headers=h)
        self.assertIs(x.headers, h)
        self.assertIsInstance(x.on_ready, promise)

    def test_then(self):
        callback = Mock()
        x = http.Request('http://foo')
        x.then(callback)

        x.on_ready(1)
        callback.assert_called_with(1)


class test_Client(Case):

    def test_get_request(self):
        hub = Hub()
        callback = Mock(name='callback')

        def on_ready(response):
            print('{0.effective_url} -> {0.code}'.format(response))
        on_ready = promise(on_ready)
        on_ready.then(callback)
        requests = [
            http.Request(
                'http://localhost:8000/README.rst',
                on_ready=on_ready,
            ),
            http.Request(
                'http://localhost:8000/AUTHORS',
                on_ready=on_ready,
            ),
            http.Request(
                'http://localhost:8000/pavement.py',
                on_ready=on_ready,
            ),
            http.Request(
                'http://localhost:8000/setup.py',
                on_ready=on_ready,
            ),
        ]
        client = http.Client(hub)
        for request in requests:
            request.then(promise(callback))
            client.perform(request)

        print('START PERFORM')
        while callback.call_count < len(requests):
            hub.run_once()
        print('-END PERFORM')
