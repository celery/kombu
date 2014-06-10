from amqp import promise
from kombu import Connection, Exchange, Queue, Producer
from kombu.async import Hub

KEY = 'asynasynasyn'
q = Queue(KEY, Exchange(KEY), KEY)

program_finished = promise()


def on_queue_declared(queue):
    print('QUEUE DECLARED %r :))))' % (queue, ))
    program_finished()

def on_channel_created(channel):
    p = q(channel).declare(callback=on_queue_declared)

def on_connected(connection):
    print('ON CONNECtED')
    connection.channel().then(on_channel_created)

h = Hub()
c = Connection('pyamqp://')
c.connection.then(on_connected)
c.register_with_event_loop(h)
while not program_finished.ready:
    h.run_once()

