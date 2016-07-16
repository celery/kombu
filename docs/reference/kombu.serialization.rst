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

.. _`cjson`: http://pypi.python.org/pypi/python-cjson/
.. _`simplejson`: http://code.google.com/p/simplejson/
.. _`Python 2.6+`: http://docs.python.org/library/json.html
.. _`PyYAML`: http://pyyaml.org/
.. _`msgpack`: http://msgpack.sourceforge.net/
.. _`msgpack-python`: http://pypi.python.org/pypi/msgpack-python/
