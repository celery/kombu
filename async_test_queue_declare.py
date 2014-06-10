from kombu import Connection, Exchange, Queue, Producer
from kombu.async import Hub

h = Hub()
c = Connection('pyamqp://')
print('+CONNECT')
c.connect()
print('-CONNECT')

channel = c.default_channel

c.register_with_event_loop(h)

KEY = 'asynasynasyn'
q = Queue(KEY, Exchange(KEY), KEY)

def on_queue_declared(self, queue):
    print('QUEUE DECLARED %r :))))' % (queue, ))

q = q(channel)
p = q.declare(callback=on_queue_declared)

while not p.ready:
    h.run_once()

