from unittest.mock import Mock

from kombu.exceptions import HttpError


class test_HttpError:

    def test_str(self):
        assert str(HttpError(200, 'msg', Mock(name='response')))
