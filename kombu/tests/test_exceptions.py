
from kombu.exceptions import HttpError

from kombu.tests.case import Case, Mock


class test_HttpError(Case):

    def test_str(self):
        self.assertTrue(str(HttpError(200, 'msg', Mock(name='response'))))
