from __future__ import annotations

import pickle
from io import BytesIO, StringIO
from pathlib import Path
from pprint import pformat

import pytest

from kombu.utils.div import emergency_dump_state


class MyBytesIO(BytesIO):

    def close(self):
        pass


class MyStringIO(StringIO):

    def close(self):
        pass


class test_emergency_dump_state:

    @pytest.mark.parametrize('partial_write', [False, True])
    def test_dump_text_file(self, partial_write):
        state = {'task': 'rétry', 'payload': b'\x00\xff'}

        def open_text(name, mode):
            return open(name, 'w', encoding='utf-8')

        def partial_dump(state, fh, **kwargs):
            fh.write('partial dump that must not remain in the file' * 10)
            raise TypeError('cannot pickle state')

        path = Path(emergency_dump_state(
            state, open_file=open_text,
            dump=partial_dump if partial_write else None,
        ))
        try:
            assert path.read_text(encoding='utf-8') == pformat(state)
        finally:
            path.unlink()

    def test_dump_text_stream(self):
        state = {'task': 'rétry'}
        fh = MyStringIO()
        path = Path(emergency_dump_state(state, open_file=lambda n, m: fh))
        try:
            assert fh.getvalue() == pformat(state)
        finally:
            path.unlink()

    def test_dump_file(self):
        state = {'task': 'rétry', 'payload': b'\x00\xff'}
        path = Path(emergency_dump_state(state))
        try:
            assert pickle.loads(path.read_bytes()) == state
        finally:
            path.unlink()

    def test_dump_file_fallback(self):
        state = {'task': 'rétry'}

        def raise_something(state, fh, **kwargs):
            fh.write(b'partial pickle data')
            raise TypeError('cannot pickle state')

        path = Path(emergency_dump_state(state, dump=raise_something))
        try:
            assert path.read_text(encoding='utf-8') == pformat(state)
        finally:
            path.unlink()

    def test_dump(self, stdouts):
        fh = MyBytesIO()
        stderr = StringIO()
        emergency_dump_state(
            {'foo': 'bar'}, open_file=lambda n, m: fh, stderr=stderr)
        assert pickle.loads(fh.getvalue()) == {'foo': 'bar'}
        assert stderr.getvalue()
        assert not stdouts.stdout.getvalue()

    def test_dump_second_strategy(self, stdouts):
        fh = MyBytesIO()
        stderr = StringIO()

        def raise_something(*args, **kwargs):
            raise KeyError('foo')

        emergency_dump_state(
            {'foo': 'bar'},
            open_file=lambda n, m: fh,
            dump=raise_something,
            stderr=stderr,
        )
        assert b'foo' in fh.getvalue()
        assert b'bar' in fh.getvalue()
        assert stderr.getvalue()
        assert not stdouts.stdout.getvalue()

    def test_dump_logging(self, caplog):
        fh = MyBytesIO()
        emergency_dump_state(
            {'foo': 'bar'}, open_file=lambda n, m: fh, stderr=None)
        assert pickle.loads(fh.getvalue()) == {'foo': 'bar'}
        assert "EMERGENCY DUMP STATE TO FILE" in caplog.text

    def test_dump_logging_exception(self, caplog):
        fh = MyBytesIO()

        def raise_something(*args, **kwargs):
            raise KeyError('foo')

        emergency_dump_state(
            {'foo': 'bar'},
            open_file=lambda n, m: fh,
            dump=raise_something,
            stderr=None,
        )
        assert b'foo' in fh.getvalue()
        assert b'bar' in fh.getvalue()
        assert "Cannot pickle state. Falling back to pformat." in caplog.text
