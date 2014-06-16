from amqp import promise
from kombu import Connection, Exchange, Queue, Consumer
from kombu.async import Hub

TEST_QUEUE = Queue('test3', Exchange('test3'))

program_finished = promise()

def on_ack_sent(message):
    print('ACK SENT FOR %r' % (message.delivery_tag, ))
    if message.delivery_tag >= 10:
        consumer.stop(program_finished, close_channel=True)

def on_message(message):
    print('RECEIVED MESSAGE %r' % (message.delivery_tag, ))
    message.ack(callback=on_ack_sent)

def on_consumer_ready(consumer):
    print('CONSUMER READY!!!')

loop = Hub()
connection = Connection('pyamqp://')
consumer = Consumer(connection, [TEST_QUEUE],
                    on_message=on_message, prefetch_count=10)
consumer.then(on_consumer_ready)

#def declare_and_publish():
#    connection = yield Connection('pyamqp://')
#    channel = yield connection.channel()
#    queue = yield TEST_QUEUE(channel).declare()

connection.register_with_event_loop(loop)
while not program_finished.ready:
    loop.run_once()

