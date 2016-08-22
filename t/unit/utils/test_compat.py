from __future__ import absolute_import, unicode_literals

from case import Mock, mock, patch

from kombu.utils.compat import entrypoints, maybe_fileno


class test_entrypoints:

    @mock.mask_modules('pkg_resources')
    def test_without_pkg_resources(self):
        assert list(entrypoints('kombu.test')) == []

    @mock.module_exists('pkg_resources')
    def test_with_pkg_resources(self):
        with patch('pkg_resources.iter_entry_points', create=True) as iterep:
            eps = iterep.return_value = [Mock(), Mock()]

            assert list(entrypoints('kombu.test'))
            iterep.assert_called_with('kombu.test')
            eps[0].load.assert_called_with()
            eps[1].load.assert_called_with()


def test_maybe_fileno():
    assert maybe_fileno(3) == 3
    f = Mock(name='file')
    assert maybe_fileno(f) is f.fileno()
    f.fileno.side_effect = ValueError()
    assert maybe_fileno(f) is None
