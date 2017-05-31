""" Poll prototype for redis-cluster
"""

import time
import sys

from redis import Redis
from redis.connection import ConnectionPool

from rediscluster.connection import ClusterConnectionPool
from rediscluster.exceptions import MovedError
from rediscluster import RedisCluster

from kombu.utils.eventio import poll, READ, ERR

eventflags = READ | ERR

keys = [
    'asynt',
    'asynt1',
    'asynt2',
    'asynt3',
    'asynt4',
    'asynt5',
    'asynt6',
    'asynt7'
]


def register(conns, poller):
    for _, conn in conns:
        conn.connect()
        poller.register(conn._sock, eventflags)


def unregister(conns, poller):
    print "unregister .."
    for _, conn in conns:
        poller.unregister(conn._sock)
        conn.disconnect()


def start_poll(conns, poller, cli):
    while 1:
        _m = {}

        for key, conn in conns:
            _m.setdefault(conn.port, []).append(key)

        for _, conn in conns:
            conn.send_command('BRPOP', *_m[conn.port]+[1])

        start = time.time()
        events = poller.poll(10)
        print time.time() - start
        if events:
            for fileno, event in events or []:
                if event | READ:
                    for key, conn in conns:
                        if fileno == conn._sock.fileno():
                            try:
                                print(
                                    "key: ", key, "resp: ",
                                    cli.parse_response(conn, 'BRPOP', **{})
                                )
                            except MovedError as e:
                                print "MovedError: ", e
                            break


def cluster_poll():
    startup = [
        {'host': '127.0.0.1', 'port': 30001},
        {'host': '127.0.0.1', 'port': 30002},
        {'host': '127.0.0.1', 'port': 30003},
        {'host': '127.0.0.1', 'port': 30004},
        {'host': '127.0.0.1', 'port': 30005},
        {'host': '127.0.0.1', 'port': 30006},
    ]
    pool = ClusterConnectionPool(startup_nodes=startup)
    cli = RedisCluster(connection_pool=pool)
    poller = poll()
    conns = [
        (key, cli.connection_pool.get_connection_by_key(key))
        for key in keys
    ]
    register(conns, poller)
    try:
        start_poll(conns, poller, cli)
    except KeyboardInterrupt:
        unregister(conns, poller)


def normal_poll():
    pool = ConnectionPool(host='127.0.0.1', port=6379, db=0)
    cli = Redis(connection_pool=pool)
    poller = poll()
    conns = [
        (key, cli.connection_pool.get_connection('_'))
        for key in keys
    ]
    register(conns, poller)
    try:
        start_poll(conns, poller, cli)
    except KeyboardInterrupt:
        unregister(conns, poller)


if __name__ == '__main__':

    if sys.argv[1] == 'cluster':
        print "Start cluster mode, press Ctrl+C to stop"
        cluster_poll()
    else:
        normal_poll()
