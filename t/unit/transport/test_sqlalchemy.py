import pytest
from case import patch, skip

from kombu import Connection


@skip.unless_module('sqlalchemy')
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
        conn = Connection('sqlalchemy+sqlite:///:memory:')
        conn.connect()
        try:
            channel = conn.channel()
            assert channel.queue_cls.__table__.name == 'kombu_queue'
            assert channel.message_cls.__table__.name == 'kombu_message'

            channel._put('celery', 'DATA_SIMPLE_QUEUEING')
            assert channel._get('celery') == 'DATA_SIMPLE_QUEUEING'
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
