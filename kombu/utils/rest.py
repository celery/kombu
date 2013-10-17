"""
kombu.utils.rest
================

REST utilities.

"""

__author__ = """\
Tal Liron <tal.liron@threecrickets.com>
"""

import urllib2, jsonpickle, anyjson

import kombu.serialization


REST_DRIVER = 'urllib2'


def rest_request(url_template, payload=None, method=None, **kwargs):
    url = url_template % kwargs
    
    if payload is not None:
        payload=jsonpickle.encode(payload)

    request = urllib2.Request(url, data=payload, headers={'Content-Type': 'application/json'})

    if method is not None:
        # Note: if payload is present, method will default to POST
        request.get_method = lambda: method

    try:
        connection = urllib2.urlopen(request)
        response = connection.read()
        if response:
			return jsonpickle.decode(response)
    except urllib2.HTTPError, e:
        if e.code == 404:
            return None
        else:
            raise RESTError(e)
    except urllib2.URLError, e:
        raise RESTError(e)


def register_raw(make_default=False):
	kombu.serialization.register('raw', lambda x: x, lambda x: x, content_type='application/data', content_encoding='utf-8')
	if make_default:
		kombu.serialization.registry._set_default_serializer('raw')


class RESTError(Exception):
	def __init__(self, cause):
		Exception.__init__(self, cause.message)
		self.cause = cause


class RawCodec(object):
    """
    Sends data as is.
    """
    def encode(self, s):
        return s
    def decode(self, s):
        return s


class ReverseJSONCodec(object):
    """
    Message bodies are expected to be encoded JSON strings, which we will
    then 'encode' into the original unpacked data structure.
    
    This is useful in conjunction with CELERY_TASK_SERIALIZER='json',
    which indeed sends Kombu encoded JSON strings.
    """
    def encode(self, s):
        return anyjson.loads(s)
    def decode(self, s):
        return anyjson.dumps(s)
