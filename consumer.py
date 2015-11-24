import nsq
import time
import logging
from nsq.async import AsyncConn
from kombu.async.tornado import TornadoHub
from kombu.log import setup_logging

setup_logging(loglevel=logging.DEBUG)

hub = TornadoHub()

def handler(message):
    print(message.body)
    return True

reader = nsq.Reader(
    message_handler=handler,
    nsqd_tcp_addresses=['localhost:4150'],
    #lookupd_http_addresses=['http://localhost:4161'],
    io_loop=hub,
    topic='c.stress',
    channel='c.stress',
    max_in_flight=9,
    lookupd_poll_interval=1)
hub.run_forever()

