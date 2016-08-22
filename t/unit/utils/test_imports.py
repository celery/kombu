from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock

from kombu import Exchange
from kombu.utils.imports import symbol_by_name


class test_symbol_by_name:

    def test_instance_returns_instance(self):
        instance = object()
        assert symbol_by_name(instance) is instance

    def test_returns_default(self):
        default = object()
        assert symbol_by_name(
            'xyz.ryx.qedoa.weq:foz', default=default) is default

    def test_no_default(self):
        with pytest.raises(ImportError):
            symbol_by_name('xyz.ryx.qedoa.weq:foz')

    def test_imp_reraises_ValueError(self):
        imp = Mock()
        imp.side_effect = ValueError()
        with pytest.raises(ValueError):
            symbol_by_name('kombu.Connection', imp=imp)

    def test_package(self):
        assert symbol_by_name('.entity:Exchange', package='kombu') is Exchange
        assert symbol_by_name(':Consumer', package='kombu')
