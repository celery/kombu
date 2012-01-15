from __future__ import absolute_import

import anyjson

from ..exceptions import VersionMismatch

# avoid json implementation inconsistencies.
try:
    import json  # noqa
    anyjson.force_implementation("json")
except ImportError:
    anyjson.force_implementation("simplejson")

moduleindex = ("kombu.abstract",
               "kombu.clocks",
               "kombu.common",
               "kombu.compat",
               "kombu.compression",
               "kombu.connection",
               "kombu.entity",
               "kombu.exceptions",
               "kombu.log",
               "kombu.messaging",
               "kombu.mixins",
               "kombu.pidbox",
               "kombu.pools",
               "kombu.serialization",
               "kombu.simple",
               "kombu.utils",
               "kombu.utils.compat",
               "kombu.utils.debug",
               "kombu.utils.encoding",
               "kombu.utils.functional",
               "kombu.utils.limits",
               "kombu.transport",
               "kombu.transport.amqplib",
               "kombu.transport.base",
               "kombu.transport.beanstalk",
               "kombu.transport.couchdb",
               "kombu.transport.django",
               "kombu.transport.django.managers",
               "kombu.transport.django.models",
               "kombu.transport.memory",
               "kombu.transport.mongodb",
               "kombu.transport.pika",
               "kombu.transport.redis",
               "kombu.transport.SQS",
               "kombu.transport.sqlalchemy",
               "kombu.transport.virtual",
               "kombu.transport.virtual.exchange",
               "kombu.transport.virtual.scheduling")


def setup():
    # so coverage sees all our modules.
    for module in moduleindex:
        print("preimporting %r for coverage..." % (module, ))
        try:
            __import__(module)
        except (ImportError, VersionMismatch):
            pass
