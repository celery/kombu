from __future__ import absolute_import, unicode_literals

from kombu.utils.objects import cached_property

from kombu.tests.case import Case


class test_cached_property(Case):

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
        self.assertFalse(x.xx)
        x.__dict__['foo'] = 'here'
        del(x.foo)
        self.assertEqual(x.xx, 'here')

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
        self.assertIs(X.foo, desc)

        self.assertIs(desc.__get__(None), desc)
        self.assertIs(desc.__set__(None, 1), desc)
        self.assertIs(desc.__delete__(None), desc)
        self.assertTrue(desc.setter(1))

        x = X()
        x.foo = 30
        self.assertEqual(x.xx, 10)

        del(x.foo)
