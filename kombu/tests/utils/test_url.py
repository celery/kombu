from __future__ import absolute_import, unicode_literals

from kombu.utils.url import as_url, parse_url, maybe_sanitize_url

from kombu.tests.case import Case


class test_parse_url(Case):

    def test_parse_url(self):
        result = parse_url('amqp://user:pass@localhost:5672/my/vhost')
        self.assertDictEqual(result, {
            'transport': 'amqp',
            'userid': 'user',
            'password': 'pass',
            'hostname': 'localhost',
            'port': 5672,
            'virtual_host': 'my/vhost',
        })


class test_as_url(Case):

    def test_as_url(self):
        self.assertEqual(as_url('https'), 'https:///')
        self.assertEqual(as_url('https', 'e.com'), 'https://e.com/')
        self.assertEqual(as_url('https', 'e.com', 80), 'https://e.com:80/')
        self.assertEqual(
            as_url('https', 'e.com', 80, 'u'), 'https://u@e.com:80/',
        )
        self.assertEqual(
            as_url('https', 'e.com', 80, 'u', 'p'), 'https://u:p@e.com:80/',
        )
        self.assertEqual(
            as_url('https', 'e.com', 80, None, 'p'), 'https://:p@e.com:80/',
        )
        self.assertEqual(
            as_url('https', 'e.com', 80, None, 'p', '/foo'),
            'https://:p@e.com:80//foo',
        )


class test_maybe_sanitize_url(Case):

    def test_maybe_sanitize_url(self):
        self.assertEqual(maybe_sanitize_url('foo'), 'foo')
        self.assertEqual(
            maybe_sanitize_url('http://u:p@e.com//foo'),
            'http://u:**@e.com//foo',
        )
