.. _guide-serialization:

===============
 Serialization
===============

.. _serializers:

Serializers
===========

By default every message is encoded using `JSON`_, so sending
Python data structures like dictionaries and lists works.
`YAML`_, `msgpack`_ and Python's built-in `pickle` module is also supported,
and if needed you can register any custom serialization scheme you
want to use.

.. _`JSON`: http://www.json.org/
.. _`YAML`: http://yaml.org/
.. _`msgpack`: http://msgpack.sourceforge.net/

Each option has its advantages and disadvantages.

`json` -- JSON is supported in many programming languages, is now
    a standard part of Python (since 2.6), and is fairly fast to
    decode using the modern Python libraries such as `cjson` or
    `simplejson`.

    The primary disadvantage to `JSON` is that it limits you to
    the following data types: strings, Unicode, floats, boolean,
    dictionaries, and lists.  Decimals and dates are notably missing.

    Also, binary data will be transferred using Base64 encoding, which
    will cause the transferred data to be around 34% larger than an
    encoding which supports native binary types.

    However, if your data fits inside the above constraints and
    you need cross-language support, the default setting of `JSON`
    is probably your best choice.

`pickle` -- If you have no desire to support any language other than
    Python, then using the `pickle` encoding will gain you
    the support of all built-in Python data types (except class instances),
    smaller messages when sending binary files, and a slight speedup
    over `JSON` processing.

`yaml` -- YAML has many of the same characteristics as `json`,
    except that it natively supports more data types (including dates,
    recursive references, etc.)

    However, the Python libraries for YAML are a good bit slower
    than the libraries for JSON.

    If you need a more expressive set of data types and need to maintain
    cross-language compatibility, then `YAML` may be a better fit
    than the above.

To instruct `Kombu` to use an alternate serialization method,
use one of the following options.

    1.  Set the serialization option on a per-producer basis::

            >>> producer = Producer(channel,
            ...                     exchange=exchange,
            ...                     serializer="yaml")

    2.  Set the serialization option per message::

            >>> producer.publish(message, routing_key=rkey,
            ...                  serializer="pickle")

Note that a `Consumer` do not need the serialization method specified.
They can auto-detect the serialization method as the
content-type is sent as a message header.

.. _sending-raw-data:

Sending raw data without Serialization
======================================

In some cases, you don't need your message data to be serialized. If you
pass in a plain string or Unicode object as your message, then `Kombu` will
not waste cycles serializing/deserializing the data.

You can optionally specify a `content_type` and `content_encoding`
for the raw data::

    >>> with open("~/my_picture.jpg", "rb") as fh:
    ...     producer.publish(fh.read(),
                             content_type="image/jpeg",
                             content_encoding="binary",
                             routing_key=rkey)

The `Message` object returned by the `Consumer` class will have a
`content_type` and `content_encoding` attribute.
