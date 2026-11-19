"""Tests for kombu.utils.eventio."""
from __future__ import annotations

import errno
from unittest.mock import MagicMock, patch

import pytest

import kombu.utils.eventio as eventio
from kombu.utils.eventio import READ

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_oserror(err_no):
    exc = OSError()
    exc.errno = err_no
    return exc


# ---------------------------------------------------------------------------
# _epoll
# ---------------------------------------------------------------------------

class test__epoll:

    @pytest.fixture()
    def mock_epoll_cls(self):
        with patch.object(eventio, 'epoll', create=True) as mock_cls:
            mock_cls.return_value = MagicMock(name='select.epoll-instance')
            yield mock_cls

    @pytest.fixture()
    def ep(self, mock_epoll_cls):
        return eventio._epoll()

    def test_register_success(self, ep):
        fd = ep.register(5, READ)
        ep._epoll.register.assert_called_once_with(5, READ)
        assert fd == 5

    def test_register_eexist_is_ignored(self, ep):
        exc = _make_oserror(errno.EEXIST)
        ep._epoll.register.side_effect = exc
        ep.register(5, READ)

    def test_register_other_oserror_is_raised(self, ep):
        exc = _make_oserror(errno.EACCES)
        ep._epoll.register.side_effect = exc
        with pytest.raises(OSError):
            ep.register(5, READ)

    def test_unregister_success(self, ep):
        ep.unregister(5)
        ep._epoll.unregister.assert_called_once_with(5)

    @pytest.mark.parametrize('exc_type', [ValueError, KeyError, TypeError])
    def test_unregister_silences_value_key_type_errors(self, ep, exc_type):
        ep._epoll.unregister.side_effect = exc_type('oops')
        ep.unregister(5)

    @pytest.mark.parametrize('err_no', [errno.ENOENT, errno.EPERM])
    def test_unregister_silences_enoent_and_eperm(self, ep, err_no):
        ep._epoll.unregister.side_effect = _make_oserror(err_no)
        ep.unregister(5)

    def test_unregister_reraises_other_oserror(self, ep):
        ep._epoll.unregister.side_effect = _make_oserror(errno.EACCES)
        with pytest.raises(OSError) as exc_info:
            ep.unregister(5)
        assert exc_info.value.errno == errno.EACCES

    def test_unregister_oserror_without_errno_is_reraised(self, ep):
        """OSError with no errno attr must also propagate."""
        ep._epoll.unregister.side_effect = OSError('bare error')
        with pytest.raises(OSError):
            ep.unregister(5)

    def test_poll_returns_events(self, ep):
        ep._epoll.poll.return_value = [(3, READ)]
        result = ep.poll(1.0)
        ep._epoll.poll.assert_called_once_with(1.0)
        assert result == [(3, READ)]

    def test_poll_none_timeout_passes_minus_one(self, ep):
        ep._epoll.poll.return_value = []
        ep.poll(None)
        ep._epoll.poll.assert_called_once_with(-1)

    def test_poll_eintr_returns_none(self, ep):
        ep._epoll.poll.side_effect = _make_oserror(errno.EINTR)
        assert ep.poll(1.0) is None

    def test_poll_other_exception_is_reraised(self, ep):
        ep._epoll.poll.side_effect = _make_oserror(errno.EACCES)
        with pytest.raises(OSError):
            ep.poll(1.0)

    def test_close(self, ep):
        ep.close()
        ep._epoll.close.assert_called_once()


# ---------------------------------------------------------------------------
# _poll
# ---------------------------------------------------------------------------

class test__poll:

    @pytest.fixture()
    def mock_poll_cls(self):
        with patch.object(eventio, 'xpoll', create=True) as mock_cls:
            mock_cls.return_value = MagicMock(name='select.poll-instance')
            yield mock_cls

    @pytest.fixture()
    def p(self, mock_poll_cls):
        return eventio._poll()

    def test_poll_none_timeout_passes_minus_one(self, p):
        """poll(None) must block indefinitely, not return immediately.

        Regression test: the timeout conversion used to turn None into 0,
        so drain_events() busy-spun on platforms without epoll.
        """
        p._poller.poll.return_value = []
        p.poll(None)
        p._poller.poll.assert_called_once_with(-1)

    def test_poll_timeout_is_converted_to_milliseconds(self, p):
        p._poller.poll.return_value = []
        p.poll(0.5)
        p._poller.poll.assert_called_once_with(500)

    def test_poll_zero_timeout_stays_zero(self, p):
        p._poller.poll.return_value = []
        p.poll(0)
        p._poller.poll.assert_called_once_with(0)

    def test_poll_negative_timeout_becomes_zero(self, p):
        p._poller.poll.return_value = []
        p.poll(-1)
        p._poller.poll.assert_called_once_with(0)

    def test_poll_none_timeout_blocks_until_event(self):
        """End-to-end check with a real poller and a socketpair."""
        import socket
        import threading
        import time

        r, w = socket.socketpair()
        try:
            poller = eventio._poll()
            fd = poller.register(r, READ)
            threading.Timer(0.3, w.send, (b'x',)).start()
            start = time.monotonic()
            result = poller.poll(None)
            elapsed = time.monotonic() - start
            assert result == [(fd, READ)]
            assert elapsed >= 0.25
        finally:
            r.close()
            w.close()
