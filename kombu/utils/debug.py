"""Debugging support."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from vine.utils import wraps

from kombu.log import get_logger

if TYPE_CHECKING:
    from logging import Logger
    from types import TracebackType
    from typing import Any, Callable

    from kombu.transport.base import Transport

__all__ = ('setup_logging', 'Logwrapped')


def setup_logging(
    loglevel: int | None = logging.DEBUG,
    loggers: list[str] | None = None
) -> None:
    """Setup logging to stdout."""
    loggers = ['kombu.connection', 'kombu.channel'] if not loggers else loggers
    for logger_name in loggers:
        logger = get_logger(logger_name)
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(loglevel)


class Logwrapped:
    """Wrap all object methods, to log on call."""

    def __init__(
        self,
        instance: Transport,
        logger: Logger | None = None,
        ident: str | None = None
    ):
        self.instance = instance
        self.logger = get_logger(logger)
        self.ident = ident

    def __getattr__(self, key: str) -> Callable:
        meth = getattr(self.instance, key)

        if not callable(meth):
            return meth

        @wraps(meth)
        def __wrapped(*args: list[Any], **kwargs: dict[str, Any]) -> Callable:
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

    def __enter__(self) -> Logwrapped:
        self.instance.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None
    ) -> None:
        self.instance.__exit__(exc_type, exc_val, exc_tb)

    def __repr__(self) -> str:
        return repr(self.instance)

    def __dir__(self) -> list[str]:
        return dir(self.instance)
