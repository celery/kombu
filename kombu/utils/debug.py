"""Debugging support."""
import logging

from typing import Any, Optional, Sequence, Union

from vine.utils import wraps

from kombu.log import get_logger

__all__ = ['setup_logging', 'Logwrapped']

DEFAULT_LOGGERS = ['kombu.connection', 'kombu.channel']

LoggerArg = Union[str, logging.Logger]


def setup_logging(loglevel: int=logging.DEBUG,
                  loggers: Sequence[LoggerArg]=DEFAULT_LOGGERS) -> None:
    for logger in loggers:
        l = get_logger(logger)
        l.addHandler(logging.StreamHandler())
        l.setLevel(loglevel)


class Logwrapped:
    __ignore = ('__enter__', '__exit__')

    def __init__(self, instance: Any,
                 logger: Optional[LoggerArg]=None,
                 ident: Optional[str]=None) -> None:
        self.instance = instance
        self.logger = get_logger(logger)  # type: logging.Logger
        self.ident = ident

    def __getattr__(self, key: str) -> Any:
        meth = getattr(self.instance, key)

        if not callable(meth) or key in self.__ignore:
            return meth

        @wraps(meth)
        def __wrapped(*args, **kwargs) -> Any:
            info = ''
            if self.ident:
                info += self.ident.format(self.instance)
            info += '{0.__name__}('.format(meth)
            if args:
                info += ', '.join(map(repr, args))
            if kwargs:
                if args:
                    info += ', '
                info += ', '.join('{k}={v!r}'.format(k=key, v=value)
                                  for key, value in kwargs.items())
            info += ')'
            self.logger.debug(info)
            return meth(*args, **kwargs)

        return __wrapped

    def __repr__(self) -> str:
        return repr(self.instance)

    def __dir__(self) -> Sequence[str]:
        return dir(self.instance)
