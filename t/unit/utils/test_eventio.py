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
