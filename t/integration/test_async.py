import os
import pytest
from kombu import Connection, Exchange, Queue, Producer, Consumer
from kombu.pools import producers

BROKER_URL = os.environ.get('BROKER_URL', 'pyamqp://')

queue1 = Queue('testq32', Exchange('testq32', 'direct'), 'testq32')


@pytest.fixture()
def connection():
    return Connection(BROKER_URL)


@pytest.mark.asyncio
async def test_queue_declare(connection):
    async with connection:
        async with connection.default_channel as channel:
            ret = await queue1(channel).declare()
            assert ret == queue1.name


@pytest.mark.asyncio
async def test_produce_consume(connection):

    messages_received = [0]

    async def on_message(message):
        messages_received[0] += 1
        await message.ack()

    async with connection as connection:
        async with Consumer(connection,
                            queues=[queue1],
                            on_message=on_message) as consumer:
            async with connection.clone() as w_connection:
                await w_connection.connect()
                assert w_connection._connection
                async with producers[w_connection].acquire() as producer:
                    for i in range(10):
                        await producer.publish(
                            str(i),
                            exchange=queue1.exchange,
                            routing_key=queue1.routing_key,
                            retry=False,
                            declare=[queue1],
                        )
            while messages_received[0] < 10:
                await connection.drain_events()
    assert messages_received[0] == 10
