from __future__ import annotations

import sqlite3
from base64 import b64decode
from contextlib import contextmanager, suppress
from unittest.mock import Mock, patch

import pytest

from kombu import Connection, Queue
from kombu.exceptions import OperationalError

pytest.importorskip('sqlalchemy')


@contextmanager
def sqlite_write_lock(db_path):
    """Hold the SQLite write lock from another connection.

    Yields a callable that releases the lock early.
    """
    other = sqlite3.connect(str(db_path), isolation_level=None)

    def release(*args):
        if other.in_transaction:
            other.execute('COMMIT')

    try:
        other.execute('BEGIN IMMEDIATE')
        yield release
        release()
    finally:
        other.close()


class test_SqlAlchemy:

    def test_url_parser(self):
        with patch('kombu.transport.sqlalchemy.Channel._open'):
            url = 'sqlalchemy+sqlite:///celerydb.sqlite'
            Connection(url).connect()

            url = 'sqla+sqlite:///celerydb.sqlite'
            Connection(url).connect()

            url = 'sqlb+sqlite:///celerydb.sqlite'
            with pytest.raises(KeyError):
                Connection(url).connect()

    def test_simple_queueing(self):
        conn = Connection(
            'sqlalchemy+sqlite:///:memory:',
            transport_options={
                "callback": Mock(),
                "errback": Mock(),
                "max_retries": 20,
                "interval_start": 1,
                "interval_step": 2,
                "interval_max": 30,
                "retry_errors": (OperationalError,)
            })
        conn.connect()
        try:
            channel = conn.channel()
            assert channel.queue_cls.__table__.name == 'kombu_queue'
            assert channel.message_cls.__table__.name == 'kombu_message'

            assert channel._size('celery') == 0
            channel._put('celery', 'DATA_SIMPLE_QUEUEING')
            assert channel._size('celery') == 1
            assert channel._get('celery') == 'DATA_SIMPLE_QUEUEING'
        finally:
            conn.release()

    def test_purge_counts_only_pending_messages(self):
        conn = Connection('sqlalchemy+sqlite:///:memory:')
        try:
            channel = conn.channel()
            for i in range(3):
                channel._put('purge', f'DATA_{i}')
            channel._get('purge')

            assert channel._size('purge') == 2
            assert channel._purge('purge') == 2
            assert channel._size('purge') == 0
            assert channel._query_all('purge').count() == 0
        finally:
            conn.release()

    def _locked_db_connection(self, tmp_path):
        db_path = tmp_path / 'broker.sqlite'
        # Short busy timeout so a held write lock fails fast.
        conn = Connection(
            f'sqlalchemy+sqlite:///{db_path}',
            transport_options={'connect_args': {'timeout': 0.05}},
        )
        return conn, db_path

    def test_put_raises_when_commit_fails(self, tmp_path):
        from sqlalchemy.exc import OperationalError as SAOperationalError

        conn, db_path = self._locked_db_connection(tmp_path)
        try:
            channel = conn.channel()
            channel._new_queue('locked')
            with sqlite_write_lock(db_path):
                with pytest.raises(SAOperationalError, match='locked'):
                    channel._put('locked', 'DATA_LOST')
            assert channel._size('locked') == 0
        finally:
            conn.release()

    def test_publish_raises_when_put_fails(self, tmp_path):
        from sqlalchemy.exc import OperationalError as SAOperationalError

        conn, db_path = self._locked_db_connection(tmp_path)
        try:
            channel = conn.channel()
            channel._new_queue('locked')
            producer = conn.Producer(channel)
            with sqlite_write_lock(db_path):
                with pytest.raises(SAOperationalError, match='locked'):
                    producer.publish({'job': 1}, routing_key='locked')
            assert channel._size('locked') == 0
        finally:
            conn.release()

    def test_publish_retry_stores_message_after_failed_put(self, tmp_path):
        conn, db_path = self._locked_db_connection(tmp_path)
        try:
            channel = conn.channel()
            channel._new_queue('locked')
            producer = conn.Producer()
            with sqlite_write_lock(db_path) as release:
                # The errback runs after the first failed attempt, before
                # the retry, so the retry finds the lock released.
                errback = Mock(side_effect=release)
                producer.publish(
                    {'job': 1}, routing_key='locked', retry=True,
                    retry_policy={'max_retries': 1, 'errback': errback},
                )
            errback.assert_called_once()
            assert conn.channel()._size('locked') == 1
        finally:
            conn.release()

    def test_requeue_keeps_message_unacked_when_put_fails(self, tmp_path):
        from sqlalchemy.exc import OperationalError as SAOperationalError

        conn, db_path = self._locked_db_connection(tmp_path)
        try:
            channel = conn.channel()
            queue = Queue('locked')(channel)
            queue.declare()
            conn.Producer(channel).publish({'job': 1}, routing_key='locked')
            message = queue.get(no_ack=False)
            assert message.payload == {'job': 1}
            with sqlite_write_lock(db_path):
                with pytest.raises(SAOperationalError, match='locked'):
                    message.requeue()
            # Still unacked, so closing the channel restores it.
            channel.close()
            assert conn.channel()._size('locked') == 1
        finally:
            conn.release()

    def test_close_dumps_message_when_restore_fails(self, tmp_path):
        conn, db_path = self._locked_db_connection(tmp_path)
        try:
            channel = conn.channel()
            queue = Queue('locked')(channel)
            queue.declare()
            conn.Producer(channel).publish({'job': 1}, routing_key='locked')
            message = queue.get(no_ack=False)
            assert message.payload == {'job': 1}
            dump = 'kombu.transport.virtual.base.emergency_dump_state'
            with patch(dump) as emergency_dump_state:
                with sqlite_write_lock(db_path):
                    channel.close()
            # The unacked message could not be restored, so it is
            # dumped instead of being silently dropped.
            emergency_dump_state.assert_called_once()
            dumped = emergency_dump_state.call_args[0][0]
            assert len(dumped) == 1
            assert dumped[0]['properties']['body_encoding'] == 'base64'
            assert b64decode(dumped[0]['body']) == message.body
            assert (dumped[0]['properties']['delivery_tag'] ==
                    message.delivery_tag)
        finally:
            conn.release()

    def test_put_after_failed_commit_succeeds(self, tmp_path):
        from sqlalchemy.exc import OperationalError as SAOperationalError

        conn, db_path = self._locked_db_connection(tmp_path)
        try:
            channel = conn.channel()
            channel._new_queue('locked')
            with sqlite_write_lock(db_path):
                with suppress(SAOperationalError):
                    channel._put('locked', 'DATA_LOST')
            # The failed commit was rolled back; the session still works.
            channel._put('locked', 'DATA_KEPT')
            assert channel._size('locked') == 1
            assert channel._get('locked') == 'DATA_KEPT'
        finally:
            conn.release()

    def test_clone(self):
        hostname = 'sqlite:///celerydb.sqlite'
        x = Connection('+'.join(['sqla', hostname]))
        try:
            assert x.uri_prefix == 'sqla'
            assert x.hostname == hostname
            clone = x.clone()
            try:
                assert clone.hostname == hostname
                assert clone.uri_prefix == 'sqla'
            finally:
                clone.release()
        finally:
            x.release()

    @pytest.mark.parametrize('dialect_name, expected', [
        ('mssql', 'BIGINT'),
        ('postgresql', 'INTEGER'),
        ('mysql', 'INTEGER'),
        ('sqlite', 'INTEGER'),
    ])
    def test_id_column_types(self, dialect_name, expected):
        from sqlalchemy.dialects import registry

        conn = Connection('sqlalchemy+sqlite:///:memory:')
        try:
            channel = conn.channel()
            dialect = registry.load(dialect_name)()
            columns = [
                channel.queue_cls.__table__.c.id,
                channel.message_cls.__table__.c.id,
                channel.message_cls.__table__.c.queue_id,
            ]
            for column in columns:
                assert column.type.compile(dialect=dialect) == expected
        finally:
            conn.release()
