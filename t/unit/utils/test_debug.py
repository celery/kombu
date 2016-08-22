from __future__ import absolute_import, unicode_literals

import logging

from case import Mock, patch

from kombu.five import bytes_if_py2
from kombu.utils.debug import Logwrapped, setup_logging


class test_setup_logging:

    def test_adds_handlers_sets_level(self):
        with patch('kombu.utils.debug.get_logger') as get_logger:
            logger = get_logger.return_value = Mock()
            setup_logging(loggers=['kombu.test'])

            get_logger.assert_called_with('kombu.test')

            logger.addHandler.assert_called()
            logger.setLevel.assert_called_with(logging.DEBUG)


class test_Logwrapped:

    def test_wraps(self):
        with patch('kombu.utils.debug.get_logger') as get_logger:
            logger = get_logger.return_value = Mock()

            W = Logwrapped(Mock(), 'kombu.test')
            get_logger.assert_called_with('kombu.test')
            assert W.instance is not None
            assert W.logger is logger

            W.instance.__repr__ = lambda s: bytes_if_py2('foo')
            assert repr(W) == 'foo'
            W.instance.some_attr = 303
            assert W.some_attr == 303

            W.instance.some_method.__name__ = bytes_if_py2('some_method')
            W.some_method(1, 2, kw=1)
            W.instance.some_method.assert_called_with(1, 2, kw=1)

            W.some_method()
            W.instance.some_method.assert_called_with()

            W.some_method(kw=1)
            W.instance.some_method.assert_called_with(kw=1)

            W.ident = 'ident'
            W.some_method(kw=1)
            logger.debug.assert_called()
            assert 'ident' in logger.debug.call_args[0][0]

            assert dir(W) == dir(W.instance)
