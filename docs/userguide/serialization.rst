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

    By default Kombu uses pickle protocol 2, but this can be changed
    using the :envvar:`PICKLE_PROTOCOL` environment variable or by changing
    the global :data:`kombu.serialization.pickle_protocol` flag.

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

.. _serialization-entrypoints:

Creating extensions using Setuptools entry-points
=================================================

A package can also register new serializers using Setuptools
entry-points.

The entry-point must provide the name of the serializer along
with the path to a tuple providing the rest of the args:
``decoder_function, encoder_function, content_type, content_encoding``.

An example entrypoint could be:

.. code-block:: python

    from setuptools import setup

    setup(
        entry_points={
            'kombu.serializers': [
                'my_serializer = my_module.serializer:register_args'
            ]
        }
    )


Then the module ``my_module.serializer`` would look like:

.. code-block:: python

    register_args = (my_decoder, my_encoder, 'application/x-mimetype', 'utf-8')


When this package is installed the new 'my_serializer' serializer will be
supported by Kombu.


.. admonition:: Buffer Objects

    The decoder function of custom serializer must support both strings
    and Python's old-style buffer objects.

    Python pickle and json modules usually don't do this via its ``loads``
    function, but you can easily add support by making a wrapper around the
    ``load`` function that takes file objects instead of strings.

    Here's an example wrapping :func:`pickle.loads` in such a way:

    .. code-block:: python

        import pickle
        from kombu.serialization import BytesIO, register


        def loads(s):
            return pickle.load(BytesIO(s))

        register('my_pickle', loads, pickle.dumps,
                content_type='application/x-pickle2',
                content_encoding='binary')
