import logging

from kombu.utils.functional import wraps
from kombu.utils.log import get_logger


def setup_logging(loglevel=logging.DEBUG, loggers=["kombu.connection",
                                                   "kombu.channel"]):
    for logger in loggers:
        l = get_logger(logger)
        if not l.handlers:
            l.addHandler(logging.StreamHandler())
        l.setLevel(loglevel)


class Logwrapped(object):

    def __init__(self, instance, logger=None, ident=None):
        self.instance = instance
        self.logger = get_logger(logger)
        self.ident = ident

    def __getattr__(self, key):
        meth = getattr(self.instance, key)

        if not callable(meth):
            return meth

        @wraps(meth)
        def __wrapped(*args, **kwargs):
            info = ""
            if self.ident:
                info += self.ident % vars(self.instance)
            info += "%s(" % (meth.__name__, )
            if args:
                info += ", ".join(map(repr, args))
            if kwargs:
                if args:
                    info += ", "
                info += ", ".join("%s=%r" % (key, value)
                                    for key, value in kwargs.iteritems())
            info += ")"
            self.logger.debug(info)
            return meth(*args, **kwargs)

        return __wrapped

    def __repr__(self):
        return repr(self.instance)

    def __dir__(self):
        return dir(self.instance)
