from __future__ import absolute_import, unicode_literals

import pickle

from io import StringIO, BytesIO

from kombu.utils.div import emergency_dump_state

from kombu.tests.case import Case, mock


class MyStringIO(StringIO):

    def close(self):
        pass


class MyBytesIO(BytesIO):

    def close(self):
        pass


class test_emergency_dump_state(Case):

    @mock.stdouts
    def test_dump(self, stdout, stderr):
        fh = MyBytesIO()

        emergency_dump_state(
            {'foo': 'bar'}, open_file=lambda n, m: fh)
        self.assertDictEqual(
            pickle.loads(fh.getvalue()), {'foo': 'bar'})
        self.assertTrue(stderr.getvalue())
        self.assertFalse(stdout.getvalue())

    @mock.stdouts
    def test_dump_second_strategy(self, stdout, stderr):
        fh = MyStringIO()

        def raise_something(*args, **kwargs):
            raise KeyError('foo')

        emergency_dump_state(
            {'foo': 'bar'},
            open_file=lambda n, m: fh, dump=raise_something
        )
        self.assertIn('foo', fh.getvalue())
        self.assertIn('bar', fh.getvalue())
        self.assertTrue(stderr.getvalue())
        self.assertFalse(stdout.getvalue())
