import pickle

from io import StringIO, BytesIO

from kombu.utils.div import emergency_dump_state


class MyStringIO(StringIO):

    def close(self):
        pass


class MyBytesIO(BytesIO):

    def close(self):
        pass


class test_emergency_dump_state:

    def test_dump(self, stdouts):
        fh = MyBytesIO()
        stderr = StringIO()
        emergency_dump_state(
            {'foo': 'bar'}, open_file=lambda n, m: fh, stderr=stderr)
        assert pickle.loads(fh.getvalue()) == {'foo': 'bar'}
        assert stderr.getvalue()
        assert not stdouts.stdout.getvalue()

    def test_dump_second_strategy(self, stdouts):
        fh = MyStringIO()
        stderr = StringIO()

        def raise_something(*args, **kwargs):
            raise KeyError('foo')

        emergency_dump_state(
            {'foo': 'bar'},
            open_file=lambda n, m: fh,
            dump=raise_something,
            stderr=stderr,
        )
        assert 'foo' in fh.getvalue()
        assert 'bar' in fh.getvalue()
        assert stderr.getvalue()
        assert not stdouts.stdout.getvalue()
