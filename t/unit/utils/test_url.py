from __future__ import absolute_import, unicode_literals

try:
    from urllib.parse import urlencode

except ImportError:
    from urllib import urlencode

import ssl

import pytest

import kombu.utils.url
from kombu.utils.url import as_url, parse_url, maybe_sanitize_url


def test_parse_url():
    assert parse_url('amqp://user:pass@localhost:5672/my/vhost') == {
        'transport': 'amqp',
        'userid': 'user',
        'password': 'pass',
        'hostname': 'localhost',
        'port': 5672,
        'virtual_host': 'my/vhost',
    }


@pytest.mark.parametrize('urltuple,expected', [
    (('https',), 'https:///'),
    (('https', 'e.com'), 'https://e.com/'),
    (('https', 'e.com', 80), 'https://e.com:80/'),
    (('https', 'e.com', 80, 'u'), 'https://u@e.com:80/'),
    (('https', 'e.com', 80, 'u', 'p'), 'https://u:p@e.com:80/'),
    (('https', 'e.com', 80, None, 'p'), 'https://:p@e.com:80/'),
    (('https', 'e.com', 80, None, 'p', '/foo'), 'https://:p@e.com:80//foo'),
])
def test_as_url(urltuple, expected):
    assert as_url(*urltuple) == expected


@pytest.mark.parametrize('url,expected', [
    ('foo', 'foo'),
    ('http://u:p@e.com//foo', 'http://u:**@e.com//foo'),
])
def test_maybe_sanitize_url(url, expected):
    assert maybe_sanitize_url(url) == expected
    assert (maybe_sanitize_url('http://u:p@e.com//foo') ==
            'http://u:**@e.com//foo')


def test_ssl_parameters():
    url = 'rediss://user:password@host:6379/0?'
    querystring = urlencode({
        'ssl_cert_reqs': 'CERT_REQUIRED',
        'ssl_ca_certs': '/var/ssl/myca.pem',
        'ssl_certfile': '/var/ssl/server-cert.pem',
        'ssl_keyfile': '/var/ssl/priv/worker-key.pem',
    })
    kwargs = parse_url(url + querystring)
    assert kwargs['transport'] == 'rediss'
    assert kwargs['ssl']['ssl_cert_reqs'] == ssl.CERT_REQUIRED
    assert kwargs['ssl']['ssl_ca_certs'] == '/var/ssl/myca.pem'
    assert kwargs['ssl']['ssl_certfile'] == '/var/ssl/server-cert.pem'
    assert kwargs['ssl']['ssl_keyfile'] == '/var/ssl/priv/worker-key.pem'

    kombu.utils.url.ssl_available = False

    kwargs = parse_url(url + querystring)
    assert kwargs['ssl']['ssl_cert_reqs'] is None

    kombu.utils.url.ssl_available = True
