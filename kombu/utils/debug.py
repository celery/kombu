"""Debugging support."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from vine.utils import wraps

from kombu.log import get_logger

if TYPE_CHECKING:
    from logging import Logger
    from typing import Any, Callable, Dict, List, Optional

    from kombu.transport.base import Transport

__all__ = ('setup_logging', 'Logwrapped')


def setup_logging(
    loglevel: Optional[int] = logging.DEBUG,
    loggers: Optional[List[str]] = None
) -> None:
    """Setup logging to stdout."""
    loggers = ['kombu.connection', 'kombu.channel'] if not loggers else loggers
    for logger_name in loggers:
        logger = get_logger(logger_name)
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(loglevel)


class Logwrapped:
    """Wrap all object methods, to log on call."""

    __ignore = ('__enter__', '__exit__')

    def __init__(
        self,
        instance: Transport,
        logger: Optional[Logger] = None,
        ident: Optional[str] = None
    ):
        self.instance = instance
        self.logger = get_logger(logger)
        self.ident = ident

    def __getattr__(self, key: str) -> Callable:
        meth = getattr(self.instance, key)

        if not callable(meth) or key in self.__ignore:
            return meth

        @wraps(meth)
        def __wrapped(*args: List[Any], **kwargs: Dict[str, Any]) -> Callable:
            info = ''
            if self.ident:
                info += self.ident.format(self.instance)
            info += f'{meth.__name__}('
            if args:
                info += ', '.join(map(repr, args))
            if kwargs:
                if args:
                    info += ', '
                info += ', '.join(f'{key}={value!r}'
                                  for key, value in kwargs.items())
            info += ')'
            self.logger.debug(info)
            return meth(*args, **kwargs)

        return __wrapped

    def __repr__(self) -> str:
        return repr(self.instance)

    def __dir__(self) -> List[str]:
        return dir(self.instance)
