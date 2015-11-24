import nsq
import time
import logging
from nsq.async import AsyncConn
from kombu.async.tornado import TornadoHub
from kombu.five import monotonic
from kombu.log import setup_logging

setup_logging(loglevel=logging.INFO)

hub = TornadoHub()

count = 0
finished = 0
N = 10000

writer = nsq.Writer(['localhost:4150'], io_loop=hub)

def pub_message(i):
    writer.pub('cstress', str(i) , finish_pub)

def finish_pub(conn, data):
    global finished
    finished += 1
    if finished >= N:
        print('TIME: %r' % (monotonic() - time_start,))


def on_it():
    global count
    global time_start
    if count == 0:
        time_start = monotonic()
    if count > N + 1:
        return
    count += 1
    pub_message(count)
    hub.call_soon(on_it)

hub.call_later(1, on_it)
#hub.call_repeatedly(1, pub_message)
hub.run_forever()

