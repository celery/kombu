from __future__ import absolute_import

from ..exceptions import VersionMismatch

moduleindex = ("kombu.abstract",
               "kombu.compat",
               "kombu.common",
               "kombu.clocks",
               "kombu.compression",
               "kombu.connection",
               "kombu.entity",
               "kombu.exceptions",
               "kombu.messaging",
               "kombu.pidbox",
               "kombu.pools",
               "kombu.serialization",
               "kombu.simple",
               "kombu.utils",
               "kombu.utils.compat",
               "kombu.transport",
               "kombu.transport.base",
               "kombu.transport.beanstalk",
               "kombu.transport.memory",
               "kombu.transport.mongodb",
               "kombu.transport.amqplib",
               "kombu.transport.couchdb",
               "kombu.transport.pika",
               "kombu.transport.redis",
               "kombu.transport.SQS",
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
