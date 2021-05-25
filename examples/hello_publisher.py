import datetime

from kombu import Connection


with Connection('amqp://guest:guest@localhost:5672//') as conn:
    simple_queue = conn.SimpleQueue('simple_queue')
    message = f'helloworld, sent at {datetime.datetime.today()}'
    simple_queue.put(message)
    print(f'Sent: {message}')
    simple_queue.close()
