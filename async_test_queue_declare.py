from amqp import promise
from kombu import Connection, Exchange, Queue, Producer
from kombu.async import Hub

TEST_QUEUE = Queue('test3', Exchange('test3'))

program_finished = promise()


def publish_message(channel, message, **options):
    print('sending message')
    return Producer(channel).publish(message, **options)

def on_queue_declared(queue):
    print('queue and exchange declared: {0}'.format(queue))
    return publish_message(queue.channel, {'hello': 'world'},
                           exchange=queue.exchange,
                           routing_key=queue.routing_key,
                           on_sent=program_finished)

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

