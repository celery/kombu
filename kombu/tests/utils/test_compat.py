from __future__ import absolute_import, unicode_literals

from kombu.utils.compat import entrypoints, maybe_fileno

from kombu.tests.case import Case, Mock, mock, patch


class test_entrypoints(Case):

    @mock.mask_modules('pkg_resources')
    def test_without_pkg_resources(self):
        self.assertListEqual(list(entrypoints('kombu.test')), [])

    @mock.module_exists('pkg_resources')
    def test_with_pkg_resources(self):
        with patch('pkg_resources.iter_entry_points', create=True) as iterep:
            eps = iterep.return_value = [Mock(), Mock()]

            self.assertTrue(list(entrypoints('kombu.test')))
            iterep.assert_called_with('kombu.test')
            eps[0].load.assert_called_with()
            eps[1].load.assert_called_with()


class test_maybe_fileno(Case):

    def test_maybe_fileno(self):
        self.assertEqual(maybe_fileno(3), 3)
        f = Mock(name='file')
        self.assertIs(maybe_fileno(f), f.fileno())
        f.fileno.side_effect = ValueError()
        self.assertIsNone(maybe_fileno(f))
