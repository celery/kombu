from __future__ import annotations

import logging
from unittest.mock import Mock, patch

import pytest

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

            W.instance.__repr__ = lambda s: 'foo'
            assert repr(W) == 'foo'
            W.instance.some_attr = 303
            assert W.some_attr == 303

            W.instance.some_method.__name__ = 'some_method'
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

    def test_context_manager(self):
        """Test that Logwrapped supports the context manager protocol.

        Regression test for https://github.com/celery/kombu/issues/2422
        """
        with patch('kombu.utils.debug.get_logger'):
            instance = Mock()
            instance.__enter__ = Mock(return_value=instance)
            instance.__exit__ = Mock(return_value=None)

            W = Logwrapped(instance, 'kombu.test')

            with W as ctx:
                assert ctx is W

            instance.__enter__.assert_called_once()
            instance.__exit__.assert_called_once_with(None, None, None)

    def test_context_manager_propagates_exception(self):
        """Test that Logwrapped propagates exceptions through __exit__."""
        with patch('kombu.utils.debug.get_logger'):
            instance = Mock()
            instance.__enter__ = Mock(return_value=instance)
            instance.__exit__ = Mock(return_value=False)

            W = Logwrapped(instance, 'kombu.test')

            class TestError(Exception):
                pass

            with pytest.raises(TestError):
                with W:
                    raise TestError("test")

            instance.__exit__.assert_called_once()
            args = instance.__exit__.call_args[0]
            assert args[0] is TestError
            assert isinstance(args[1], TestError)
