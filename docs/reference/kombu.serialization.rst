========================================
 Message Serialization - ``kombu``
========================================

.. currentmodule:: kombu.serialization

.. automodule:: kombu.serialization

    .. contents::
        :local:

    Overview
    --------

    Centralized support for encoding/decoding of data structures.
    Contains json, pickle, msgpack, and yaml serializers.

    Optionally installs support for YAML if the `PyYAML`_ package
    is installed.

    Optionally installs support for `msgpack`_ if the `msgpack-python`_
    package is installed.


    Exceptions
    ----------

    .. autoexception:: SerializerNotInstalled

    Serialization
    -------------

    .. autofunction:: encode

    .. autofunction:: decode

    .. autofunction:: raw_encode

    Registry
    --------

    .. autofunction:: register

    .. autodata:: registry

.. _`cjson`: https://pypi.python.org/pypi/python-cjson/
.. _`simplejson`: https://github.com/simplejson/simplejson
.. _`Python 2.7+`: https://docs.python.org/library/json.html
.. _`PyYAML`: https://pyyaml.org/
.. _`msgpack`: https://msgpack.org/
.. _`msgpack-python`: https://pypi.python.org/pypi/msgpack-python/
