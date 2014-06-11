from amqp import barrier, promise
from kombu import Connection, Exchange, Queue, Consumer
from kombu.async import Hub

TEST_QUEUE = Queue('test3', Exchange('test3'))

program_finished = promise()
active_consumers = set()
closing_channels = set()


def on_channel_closed(channel):
    print('closed channel: %r' % (channel.id, ))
    program_finished()

def on_consumer_cancelled(consumer):
    print('Consumer cancelled: %r' % (consumer, ))
    if consumer.channel not in closing_channels:
        closing_channels.add(consumer.channel)
        channel.close(callback=on_channel_closed)

def stop():
    print('on program finished')
    for consumer in active_consumers:
        consumer.cancel(callback=on_consumer_cancelled)


def on_ack_sent(message):
    print('ACK SENT FOR %r' % (message.delivery_tag, ))
    if message.delivery_tag >= 10:
        stop()

def on_message(message):
    message.ack(callback=promise(on_ack_sent, (message, )))

def on_qos_applied(consumer):
    consumer.consume()
    print('-' * 76)

def consume_messages(channel, queue):
    consumer = Consumer(channel, [queue], on_message=on_message,
                        auto_declare=False)
    active_consumers.add(consumer)
    consumer.qos(prefetch_count=10, callback=on_qos_applied)

def on_queue_declared(queue):
    print('queue and exchange declared: {0}'.format(queue))
    return consume_messages(queue.channel, queue)

def on_channel_open(channel):
    print('channel open: {0}'.format(channel))
    return TEST_QUEUE(channel).declare().then(on_queue_declared)

def on_connected(connection):
    print('connected: {0}'.format(connection))
    return connection.channel().then(on_channel_open)

loop = Hub()
connection = Connection('pyamqp://')
connection.then(on_connected)

#def declare_and_publish():
#    connection = yield Connection('pyamqp://')
#    channel = yield connection.channel()
#    queue = yield TEST_QUEUE(channel).declare()

connection.register_with_event_loop(loop)
while not program_finished.ready:
    loop.run_once()

