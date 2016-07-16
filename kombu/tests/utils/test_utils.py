from __future__ import absolute_import, unicode_literals

from kombu import version_info_t
from kombu.utils.text import version_string_as_tuple

from kombu.tests.case import Case


class test_kombu_module(Case):

    def test_dir(self):
        import kombu
        self.assertTrue(dir(kombu))


class test_version_string_as_tuple(Case):

    def test_versions(self):
        self.assertTupleEqual(
            version_string_as_tuple('3'),
            version_info_t(3, 0, 0, '', ''),
        )
        self.assertTupleEqual(
            version_string_as_tuple('3.3'),
            version_info_t(3, 3, 0, '', ''),
        )
        self.assertTupleEqual(
            version_string_as_tuple('3.3.1'),
            version_info_t(3, 3, 1, '', ''),
        )
        self.assertTupleEqual(
            version_string_as_tuple('3.3.1a3'),
            version_info_t(3, 3, 1, 'a3', ''),
        )
        self.assertTupleEqual(
            version_string_as_tuple('3.3.1a3-40c32'),
            version_info_t(3, 3, 1, 'a3', '40c32'),
        )
        self.assertEqual(
            version_string_as_tuple('3.3.1.a3.40c32'),
            version_info_t(3, 3, 1, 'a3', '40c32'),
        )
