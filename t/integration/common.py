from __future__ import absolute_import, unicode_literals

from contextlib import closing

import kombu


class BasicFunctionality(object):

    def test_connect(self, connection):
        connection.connect()
        connection.close()

    def test_publish_consume(self, connection):
        test_queue = kombu.Queue('test', routing_key='test')

        def callback(body, message):
            assert body == {'hello': 'world'}
            assert message.content_type == 'application/x-python-serialize'
            message.delivery_info['routing_key'] == 'test'
            message.delivery_info['exchange'] == ''
            message.ack()
            assert message.payload == body

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                producer.publish(
                    {'hello': 'world'},
                    retry=True,
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    declare=[test_queue],
                    serializer='pickle'
                )

                consumer = kombu.Consumer(
                    conn, [test_queue], accept=['pickle']
                )
                consumer.register_callback(callback)
                with consumer:
                    conn.drain_events(timeout=1)

    def test_simple_queue_publish_consume(self, connection):
        with connection as conn:
            with closing(conn.SimpleQueue('simple_queue_test')) as queue:
                queue.put({'Hello': 'World'}, headers={'k1': 'v1'})
                message = queue.get(timeout=1)
                assert message.payload == {'Hello': 'World'}
                assert message.content_type == 'application/json'
                assert message.content_encoding == 'utf-8'
                assert message.headers == {'k1': 'v1'}
                message.ack()

    def test_simple_buffer_publish_consume(self, connection):
        with connection as conn:
            with closing(conn.SimpleBuffer('simple_buffer_test')) as buf:
                buf.put({'Hello': 'World'}, headers={'k1': 'v1'})
                message = buf.get(timeout=1)
                assert message.payload == {'Hello': 'World'}
                assert message.content_type == 'application/json'
                assert message.content_encoding == 'utf-8'
                assert message.headers == {'k1': 'v1'}
                message.ack()
