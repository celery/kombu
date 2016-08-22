from __future__ import absolute_import, unicode_literals

import logging
import sys

from case import ANY, Mock, patch

from kombu.log import (
    get_logger,
    get_loglevel,
    safeify_format,
    Log,
    LogMixin,
    setup_logging,
)


class test_get_logger:

    def test_when_string(self):
        l = get_logger('foo')

        assert l is logging.getLogger('foo')
        h1 = l.handlers[0]
        assert isinstance(h1, logging.NullHandler)

    def test_when_logger(self):
        l = get_logger(logging.getLogger('foo'))
        h1 = l.handlers[0]
        assert isinstance(h1, logging.NullHandler)

    def test_with_custom_handler(self):
        l = logging.getLogger('bar')
        handler = logging.NullHandler()
        l.addHandler(handler)

        l = get_logger('bar')
        assert l.handlers[0] is handler

    def test_get_loglevel(self):
        assert get_loglevel('DEBUG') == logging.DEBUG
        assert get_loglevel('ERROR') == logging.ERROR
        assert get_loglevel(logging.INFO) == logging.INFO


def test_safe_format():
    fmt = 'The %r jumped %x over the %s'
    args = ['frog', 'foo', 'elephant']

    res = list(safeify_format(fmt, args))
    assert [x.strip('u') for x in res] == ["'frog'", 'foo', 'elephant']


class test_LogMixin:

    def setup(self):
        self.log = Log('Log', Mock())
        self.logger = self.log.logger

    def test_debug(self):
        self.log.debug('debug')
        self.logger.log.assert_called_with(logging.DEBUG, 'Log - debug')

    def test_info(self):
        self.log.info('info')
        self.logger.log.assert_called_with(logging.INFO, 'Log - info')

    def test_warning(self):
        self.log.warn('warning')
        self.logger.log.assert_called_with(logging.WARN, 'Log - warning')

    def test_error(self):
        self.log.error('error', exc_info='exc')
        self.logger.log.assert_called_with(
            logging.ERROR, 'Log - error', exc_info='exc',
        )

    def test_critical(self):
        self.log.critical('crit', exc_info='exc')
        self.logger.log.assert_called_with(
            logging.CRITICAL, 'Log - crit', exc_info='exc',
        )

    def test_error_when_DISABLE_TRACEBACKS(self):
        from kombu import log
        log.DISABLE_TRACEBACKS = True
        try:
            self.log.error('error')
            self.logger.log.assert_called_with(logging.ERROR, 'Log - error')
        finally:
            log.DISABLE_TRACEBACKS = False

    def test_get_loglevel(self):
        assert self.log.get_loglevel('DEBUG') == logging.DEBUG
        assert self.log.get_loglevel('ERROR') == logging.ERROR
        assert self.log.get_loglevel(logging.INFO) == logging.INFO

    def test_is_enabled_for(self):
        self.logger.isEnabledFor.return_value = True
        assert self.log.is_enabled_for('DEBUG')
        self.logger.isEnabledFor.assert_called_with(logging.DEBUG)

    def test_LogMixin_get_logger(self):
        assert LogMixin().get_logger() is logging.getLogger('LogMixin')

    def test_Log_get_logger(self):
        assert Log('test_Log').get_logger() is logging.getLogger('test_Log')

    def test_log_when_not_enabled(self):
        self.logger.isEnabledFor.return_value = False
        self.log.debug('debug')
        self.logger.log.assert_not_called()

    def test_log_with_format(self):
        self.log.debug('Host %r removed', 'example.com')
        self.logger.log.assert_called_with(
            logging.DEBUG, 'Log - Host %s removed', ANY,
        )
        assert self.logger.log.call_args[0][2].strip('u') == "'example.com'"


class test_setup_logging:

    @patch('logging.getLogger')
    def test_set_up_default_values(self, getLogger):
        logger = logging.getLogger.return_value = Mock()
        logger.handlers = []
        setup_logging()

        logger.setLevel.assert_called_with(logging.ERROR)
        logger.addHandler.assert_called()
        ah_args, _ = logger.addHandler.call_args
        handler = ah_args[0]
        assert isinstance(handler, logging.StreamHandler)
        assert handler.stream is sys.__stderr__

    @patch('logging.getLogger')
    @patch('kombu.log.WatchedFileHandler')
    def test_setup_custom_values(self, getLogger, WatchedFileHandler):
        logger = logging.getLogger.return_value = Mock()
        logger.handlers = []
        setup_logging(loglevel=logging.DEBUG, logfile='/var/logfile')

        logger.setLevel.assert_called_with(logging.DEBUG)
        logger.addHandler.assert_called()
        WatchedFileHandler.assert_called()

    @patch('logging.getLogger')
    def test_logger_already_setup(self, getLogger):
        logger = logging.getLogger.return_value = Mock()
        logger.handlers = [Mock()]
        setup_logging()

        logger.setLevel.assert_not_called()
