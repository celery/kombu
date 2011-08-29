#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

from nose import SkipTest

from kombu.serialization import registry, register, SerializerNotInstalled, \
                                raw_encode, register_yaml, register_msgpack, \
                                decode, bytes_type, pickle, \
                                unregister, register_pickle

from kombu.tests.utils import unittest
from kombu.tests.utils import mask_modules, skip_if_not_module

# For content_encoding tests
unicode_string = u'abcdé\u8463'
unicode_string_as_utf8 = unicode_string.encode('utf-8')
latin_string = u'abcdé'
latin_string_as_latin1 = latin_string.encode('latin-1')
latin_string_as_utf8 = latin_string.encode('utf-8')


# For serialization tests
py_data = {"string": "The quick brown fox jumps over the lazy dog",
        "int": 10,
        "float": 3.14159265,
        "unicode": u"Thé quick brown fox jumps over thé lazy dog",
        "list": ["george", "jerry", "elaine", "cosmo"],
}

# JSON serialization tests
json_data = ('{"int": 10, "float": 3.1415926500000002, '
             '"list": ["george", "jerry", "elaine", "cosmo"], '
             '"string": "The quick brown fox jumps over the lazy '
             'dog", "unicode": "Th\\u00e9 quick brown fox jumps over '
             'th\\u00e9 lazy dog"}')

# Pickle serialization tests
pickle_data = pickle.dumps(py_data)

# YAML serialization tests
yaml_data = ('float: 3.1415926500000002\nint: 10\n'
             'list: [george, jerry, elaine, cosmo]\n'
             'string: The quick brown fox jumps over the lazy dog\n'
             'unicode: "Th\\xE9 quick brown fox '
             'jumps over th\\xE9 lazy dog"\n')


msgpack_py_data = dict(py_data)
# msgpack only supports tuples
msgpack_py_data["list"] = tuple(msgpack_py_data["list"])
# Unicode chars are lost in transmit :(
msgpack_py_data["unicode"] = 'Th quick brown fox jumps over th lazy dog'
msgpack_data = ('\x85\xa3int\n\xa5float\xcb@\t!\xfbS\xc8\xd4\xf1\xa4list'
                '\x94\xa6george\xa5jerry\xa6elaine\xa5cosmo\xa6string\xda'
                '\x00+The quick brown fox jumps over the lazy dog\xa7unicode'
                '\xda\x00)Th quick brown fox jumps over th lazy dog')


def say(m):
    sys.stderr.write("%s\n" % (m, ))


class test_Serialization(unittest.TestCase):

    def test_content_type_decoding(self):
        self.assertEquals(unicode_string,
                          registry.decode(
                              unicode_string_as_utf8,
                              content_type='plain/text',
                              content_encoding='utf-8'))
        self.assertEquals(latin_string,
                          registry.decode(
                              latin_string_as_latin1,
                              content_type='application/data',
                              content_encoding='latin-1'))

    def test_content_type_binary(self):
        self.assertIsInstance(registry.decode(unicode_string_as_utf8,
                                              content_type='application/data',
                                              content_encoding='binary'),
                              bytes_type)

        self.assertEquals(unicode_string_as_utf8,
                          registry.decode(
                              unicode_string_as_utf8,
                              content_type='application/data',
                              content_encoding='binary'))

    def test_content_type_encoding(self):
        # Using the "raw" serializer
        self.assertEquals(unicode_string_as_utf8,
                          registry.encode(
                              unicode_string, serializer="raw")[-1])
        self.assertEquals(latin_string_as_utf8,
                          registry.encode(
                              latin_string, serializer="raw")[-1])
        # And again w/o a specific serializer to check the
        # code where we force unicode objects into a string.
        self.assertEquals(unicode_string_as_utf8,
                            registry.encode(unicode_string)[-1])
        self.assertEquals(latin_string_as_utf8,
                            registry.encode(latin_string)[-1])

    def test_json_decode(self):
        self.assertEquals(py_data,
                          registry.decode(
                              json_data,
                              content_type='application/json',
                              content_encoding='utf-8'))

    def test_json_encode(self):
        self.assertEquals(registry.decode(
                              registry.encode(py_data, serializer="json")[-1],
                              content_type='application/json',
                              content_encoding='utf-8'),
                          registry.decode(
                              json_data,
                              content_type='application/json',
                              content_encoding='utf-8'))

    @skip_if_not_module('msgpack')
    def test_msgpack_decode(self):
        register_msgpack()
        self.assertEquals(msgpack_py_data,
                          registry.decode(
                              msgpack_data,
                              content_type='application/x-msgpack',
                              content_encoding='binary'))

    @skip_if_not_module('msgpack')
    def test_msgpack_encode(self):
        register_msgpack()
        self.assertEquals(registry.decode(
                registry.encode(msgpack_py_data, serializer="msgpack")[-1],
                content_type='application/x-msgpack',
                content_encoding='binary'),
                registry.decode(
                    msgpack_data,
                    content_type='application/x-msgpack',
                    content_encoding='binary'))

    @skip_if_not_module('yaml')
    def test_yaml_decode(self):
        register_yaml()
        self.assertEquals(py_data,
                          registry.decode(
                              yaml_data,
                              content_type='application/x-yaml',
                              content_encoding='utf-8'))

    @skip_if_not_module('yaml')
    def test_yaml_encode(self):
        register_yaml()
        self.assertEquals(registry.decode(
                              registry.encode(py_data, serializer="yaml")[-1],
                              content_type='application/x-yaml',
                              content_encoding='utf-8'),
                          registry.decode(
                              yaml_data,
                              content_type='application/x-yaml',
                              content_encoding='utf-8'))

    def test_pickle_decode(self):
        self.assertEquals(py_data,
                          registry.decode(
                              pickle_data,
                              content_type='application/x-python-serialize',
                              content_encoding='binary'))

    def test_pickle_encode(self):
        self.assertEquals(pickle_data,
                          registry.encode(py_data,
                              serializer="pickle")[-1])

    def test_register(self):
        register(None, None, None, None)

    def test_unregister(self):
        self.assertRaises(SerializerNotInstalled,
                          unregister, "nonexisting")
        registry.encode("foo", serializer="pickle")
        unregister("pickle")
        self.assertRaises(SerializerNotInstalled,
                          registry.encode, "foo", serializer="pickle")
        register_pickle()

    def test_set_default_serializer_missing(self):
        self.assertRaises(SerializerNotInstalled,
                          registry._set_default_serializer, "nonexisting")

    def test_encode_missing(self):
        self.assertRaises(SerializerNotInstalled,
                          registry.encode, "foo", serializer="nonexisting")

    def test_raw_encode(self):
        self.assertTupleEqual(raw_encode("foo".encode("utf-8")),
                              ("application/data", "binary",
                                  "foo".encode("utf-8")))

    @mask_modules("yaml")
    def test_register_yaml__no_yaml(self):
        register_yaml()
        self.assertRaises(SerializerNotInstalled,
                          decode, "foo", "application/x-yaml", "utf-8")

    @mask_modules("msgpack")
    def test_register_msgpack__no_msgpack(self):
        register_msgpack()
        self.assertRaises(SerializerNotInstalled,
                          decode, "foo", "application/x-msgpack", "utf-8")
