from kombu.exceptions import VersionMismatch

moduleindex = ("kombu.abstract",
               "kombu.compat",
               "kombu.compression",
               "kombu.connection",
               "kombu.entity",
               "kombu.exceptions",
               "kombu.messaging",
               "kombu.pidbox",
               "kombu.serialization",
               "kombu.simple",
               "kombu.utils",
               "kombu.utils.compat",
               "kombu.utils.functional",
               "kombu.transport",
               "kombu.transport.base",
               "kombu.transport.beanstalk",
               "kombu.transport.memory",
               "kombu.transport.mongodb",
               "kombu.transport.pyamqplib",
               "kombu.transport.pycouchdb",
               "kombu.transport.pypika",
               "kombu.transport.pyredis",
               "kombu.transport.virtual",
               "kombu.transport.virtual.exchange",
               "kombu.transport.virtual.scheduling")


def setup():
    # so coverage sees all our modules.
    for module in moduleindex:
        try:
            __import__(module)
        except (ImportError, VersionMismatch):
            pass
