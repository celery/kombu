from __future__ import absolute_import, unicode_literals

from kombu.utils.objects import cached_property


class test_cached_property:

    def test_deleting(self):

        class X(object):
            xx = False

            @cached_property
            def foo(self):
                return 42

            @foo.deleter  # noqa
            def foo(self, value):
                self.xx = value

        x = X()
        del(x.foo)
        assert not x.xx
        x.__dict__['foo'] = 'here'
        del(x.foo)
        assert x.xx == 'here'

    def test_when_access_from_class(self):

        class X(object):
            xx = None

            @cached_property
            def foo(self):
                return 42

            @foo.setter  # noqa
            def foo(self, value):
                self.xx = 10

        desc = X.__dict__['foo']
        assert X.foo is desc

        assert desc.__get__(None) is desc
        assert desc.__set__(None, 1) is desc
        assert desc.__delete__(None) is desc
        assert desc.setter(1)

        x = X()
        x.foo = 30
        assert x.xx == 10

        del(x.foo)
