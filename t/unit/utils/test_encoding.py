import sys

from contextlib import contextmanager

from case import patch

from kombu.utils.encoding import (
    get_default_encoding_file, safe_str,
    set_default_encoding_file, default_encoding,
)


@contextmanager
def clean_encoding():
    old_encoding = sys.modules.pop('kombu.utils.encoding', None)
    import kombu.utils.encoding
    try:
        yield kombu.utils.encoding
    finally:
        if old_encoding:
            sys.modules['kombu.utils.encoding'] = old_encoding


class test_default_encoding:

    def test_set_default_file(self):
        prev = get_default_encoding_file()
        try:
            set_default_encoding_file('/foo.txt')
            assert get_default_encoding_file() == '/foo.txt'
        finally:
            set_default_encoding_file(prev)

    @patch('sys.getfilesystemencoding')
    def test_default(self, getdefaultencoding):
        getdefaultencoding.return_value = 'ascii'
        with clean_encoding() as encoding:
            enc = encoding.default_encoding()
            if sys.platform.startswith('java'):
                assert enc == 'utf-8'
            else:
                assert enc == 'ascii'
                getdefaultencoding.assert_called_with()


class newbytes(bytes):
    """Mock class to simulate python-future newbytes class"""

    def __repr__(self):
        return 'b' + super().__repr__()

    def __str__(self):
        return 'b' + f"'{super().__str__()}'"


class newstr(str):
    """Mock class to simulate python-future newstr class"""

    def encode(self, encoding=None, errors=None):
        return newbytes(super().encode(encoding, errors))


class test_safe_str:

    def setup(self):
        self._encoding = self.patching('sys.getfilesystemencoding')
        self._encoding.return_value = 'ascii'

    def test_when_bytes(self):
        assert safe_str('foo') == 'foo'

    def test_when_newstr(self):
        """Simulates using python-future package under 2.7"""
        assert str(safe_str(newstr('foo'))) == 'foo'

    def test_when_unicode(self):
        assert isinstance(safe_str('foo'), str)

    def test_when_encoding_utf8(self):
        self._encoding.return_value = 'utf-8'
        assert default_encoding() == 'utf-8'
        s = 'The quiæk fåx jømps øver the lazy dåg'
        res = safe_str(s)
        assert isinstance(res, str)

    def test_when_containing_high_chars(self):
        self._encoding.return_value = 'ascii'
        s = 'The quiæk fåx jømps øver the lazy dåg'
        res = safe_str(s)
        assert isinstance(res, str)
        assert len(s) == len(res)

    def test_when_not_string(self):
        o = object()
        assert safe_str(o) == repr(o)

    def test_when_unrepresentable(self):

        class UnrepresentableObject:

            def __repr__(self):
                raise KeyError('foo')

        assert '<Unrepresentable' in safe_str(UnrepresentableObject())
