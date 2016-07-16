from __future__ import absolute_import, unicode_literals

from kombu import Exchange
from kombu.utils.imports import symbol_by_name

from kombu.tests.case import Case, Mock


class test_symbol_by_name(Case):

    def test_instance_returns_instance(self):
        instance = object()
        self.assertIs(symbol_by_name(instance), instance)

    def test_returns_default(self):
        default = object()
        self.assertIs(
            symbol_by_name('xyz.ryx.qedoa.weq:foz', default=default),
            default,
        )

    def test_no_default(self):
        with self.assertRaises(ImportError):
            symbol_by_name('xyz.ryx.qedoa.weq:foz')

    def test_imp_reraises_ValueError(self):
        imp = Mock()
        imp.side_effect = ValueError()
        with self.assertRaises(ValueError):
            symbol_by_name('kombu.Connection', imp=imp)

    def test_package(self):
        self.assertIs(
            symbol_by_name('.entity:Exchange', package='kombu'),
            Exchange,
        )
        self.assertTrue(symbol_by_name(':Consumer', package='kombu'))
