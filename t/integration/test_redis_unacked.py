"""A Redis consumer records a message in ``unacked`` as it pops it (#305).

On main, BRPOP took a message and a second command recorded it, so a
consumer killed in between lost the message.  The properties at the end
carry most of the coverage: a state machine checks that no message is ever
lost, and a property checks the record the pop script writes.  The example
tests before them pin behavior the properties can't reach.
"""

from __future__ import annotations

import json
import os
import select
import signal
import socket
import subprocess
import sys
import threading
from collections import Counter
from queue import Empty
from time import monotonic, sleep
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
import redis
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import (RuleBasedStateMachine, initialize, invariant,
                                 rule, run_state_machine_as_test)

import kombu
from kombu.asynchronous.hub import Hub
from kombu.transport.redis import Transport
from kombu.utils.json import dumps, loads

pytestmark = pytest.mark.env('redis')

HOST = os.environ.get('REDIS_HOST', 'localhost')
PORT = int(os.environ.get('REDIS_6379_TCP', '6379'))
EXAMPLES = int(os.environ.get('KOMBU_UNACKED_EXAMPLES', 50))


def connect(name, prefix='', **options):
    return kombu.Connection(f'redis://{HOST}:{PORT}', transport_options={
        'global_keyprefix': prefix, 'unacked_key': f'{name}_unacked',
        'unacked_index_key': f'{name}_unacked_index',
        'unacked_mutex_key': f'{name}_unacked_mutex', **options})


def legacy_put(channel):
    """Publish as kombu releases without the wake list do."""
    def _put(queue, message, **kwargs):
        pri = channel._get_message_priority(message, reverse=False)
        with channel.conn_or_acquire() as client:
            client.lpush(channel._q_for_pri(queue, pri), dumps(message))
    return _put


class Setup:
    """Names, keys and a direct client for one test."""

    def __init__(self, prefix):
        self.name = f'unacked_{uuid4().hex}'
        self.prefix = f'{self.name}:' if prefix else ''
        self.redis = redis.Redis(HOST, PORT, decode_responses=True)
        self.queue = kombu.Queue(self.name, routing_key=self.name)
        self.unacked = f'{self.prefix}{self.name}_unacked'

    def connect(self, **options):
        return connect(self.name, self.prefix, **options)

    def restore_all(self, channel):
        """Restore every unacked message, as the visibility timeout would."""
        index = f'{self.prefix}{self.name}_unacked_index'
        for tag in self.redis.zrange(index, 0, -1):
            self.redis.zadd(index, {tag: 0})
        channel.qos.restore_visible(num=None, interval=1)


@pytest.fixture(params=[False, True], ids=['', 'global_keyprefix'])
def setup(request):
    s = Setup(request.param)
    yield s
    for key in s.redis.scan_iter(f'*{s.name}*'):
        s.redis.delete(key)


def drain(conn, until, limit=5, hub=None):
    deadline = monotonic() + limit
    while not until() and monotonic() < deadline:
        if hub is None:
            try:
                conn.drain_events(timeout=0.2)
            except socket.timeout:
                pass
            continue
        for tick in hub.on_tick:
            tick()
        for fd, _ in hub.poller.poll(0.05):
            callback, args = hub.readers[fd]
            try:
                callback(*args)
            except Empty:
                pass


@pytest.mark.parametrize('path', ['drain_events', 'event_loop'])
@pytest.mark.parametrize('published', ['before', 'while_waiting'])
def test_consumer_killed_right_after_the_pop(setup, path, published):
    hub = Hub() if path == 'event_loop' else None
    try:
        with setup.connect() as conn:
            with conn.Consumer(setup.queue, callbacks=[Mock()]):
                if hub:
                    conn.register_with_event_loop(hub)
                if published == 'while_waiting':
                    drain(conn, lambda: False, 0.3, hub)
                kombu.Producer(conn.clone()).publish(
                    {'n': 1}, routing_key=setup.name, priority=9)
                # The kill: right after the consumer read the message.
                with patch.object(conn.transport, '_deliver',
                                  side_effect=KeyboardInterrupt):
                    with pytest.raises(KeyboardInterrupt):
                        drain(conn, lambda: False, 5, hub)
            (tag, raw), = setup.redis.hgetall(setup.unacked).items()
            payload, exchange, routing_key = loads(raw)
            assert payload['properties']['delivery_tag'] == tag
            assert (exchange, routing_key) == ('', setup.name)
            conn.default_channel.qos._delivered.clear()
    finally:
        if hub:
            hub.close()
    # A new consumer, idle, restores it once the visibility timeout passed.
    received = []
    with setup.connect(visibility_timeout=1,
                       unacked_restore_throttle=1) as conn:
        with conn.Consumer(setup.queue, callbacks=[
                lambda body, message: (received.append(message),
                                       message.ack())]):
            drain(conn, lambda: received, 15)
    assert [m.payload for m in received] == [{'n': 1}]
    assert received[0].headers['redelivered'] is True


#: A consumer in its own process, which prints ``waiting`` once it waits.
CONSUMER = """
import json, sys
import kombu
url, options, queue = sys.argv[1:]
with kombu.Connection(url, transport_options=json.loads(options)) as conn:
    with conn.Consumer(kombu.Queue(queue), callbacks=[lambda b, m: None]):
        try:
            conn.drain_events(timeout=0.5)
        except TimeoutError:
            print('waiting', flush=True)
        conn.drain_events(timeout=30)
"""


def test_consumer_process_paused_while_waiting_then_killed(setup):
    # The wait stays in flight while the process is paused.
    options = {'global_keyprefix': setup.prefix, 'polling_interval': 10}
    env = {**os.environ, 'PYTHONPATH': os.path.dirname(
        os.path.dirname(kombu.__file__))}
    with setup.connect() as conn:
        setup.queue(conn.default_channel).declare()
        proc = subprocess.Popen(
            [sys.executable, '-c', CONSUMER, f'redis://{HOST}:{PORT}',
             json.dumps(options), setup.name],
            stdout=subprocess.PIPE, env=env, text=True)
        try:
            assert proc.stdout.readline().strip() == 'waiting'
            # Paused (a long GC pause, a starved CPU), then killed (the OOM
            # killer). On main its BRPOP had taken the message.
            proc.send_signal(signal.SIGSTOP)
            kombu.Producer(conn).publish({'n': 1}, routing_key=setup.name)
        finally:
            proc.kill()
            proc.wait()
            proc.stdout.close()
        message = setup.queue(conn.default_channel).get(no_ack=True)
        assert message.payload == {'n': 1}


@pytest.mark.parametrize('publisher', ['current', 'older'])
def test_idle_consumer_and_publishers(setup, publisher):
    received = []
    # Waits on the wake list for up to 3 s.
    with setup.connect(polling_interval=3) as conn:
        with conn.Consumer(setup.queue, callbacks=[
                lambda body, message: received.append(
                    setup.redis.hexists(setup.unacked,
                                        message.delivery_tag))]):
            drain(conn, lambda: False, 0.3)
            producer = kombu.Producer(conn.clone())
            put = (producer.channel._put if publisher == 'current'
                   else legacy_put(producer.channel))
            with patch.object(producer.channel, '_put', put):
                started = monotonic()
                producer.publish({'n': 1}, routing_key=setup.name)
                drain(conn, lambda: received, 10)
            took = monotonic() - started
            producer.connection.release()
            conn.default_channel.qos._delivered.clear()
    assert received == [True]
    # A publisher without the wake list pushes no token: the wait times out.
    assert took < (1 if publisher == 'current' else 3 + 2)


def test_idle_consumers_share_a_burst(setup):
    received = {i: [] for i in range(3)}
    stop = threading.Event()

    def consume(i, conn):
        def on_message(body, message):
            received[i].append(body['n'])
            sleep(0.02)
            message.ack()
        with conn.Consumer(setup.queue, callbacks=[on_message],
                           prefetch_count=1):
            drain(conn, stop.is_set, 30)

    # Only wake-up tokens make this fast: the wait itself lasts 10 s.
    with patch.object(Transport, 'brpop_timeout', 10):
        conns = [setup.connect() for _ in range(3)]
        threads = [threading.Thread(target=consume, args=(i, c))
                   for i, c in enumerate(conns)]
        for t in threads:
            t.start()
        sleep(0.5)
        try:
            with setup.connect() as publisher:
                producer = kombu.Producer(publisher)
                started = monotonic()
                with producer.batch():
                    for n in range(30):
                        producer.publish({'n': n}, routing_key=setup.name)
                while (sum(map(len, received.values())) < 30
                       and monotonic() < started + 5):
                    sleep(0.05)
                took = monotonic() - started
                wake = publisher.default_channel._wake_key(setup.name)
                assert setup.redis.llen(f'{setup.prefix}{wake}') <= 1
        finally:
            stop.set()
            for t in threads:
                t.join(15)
            for c in conns:
                c.release()
    assert sorted(n for r in received.values() for n in r) == list(range(30))
    assert took < 3
    assert all(len(r) >= 3 for r in received.values()), received


def test_idle_consumer_sends_few_commands(setup):
    with setup.connect() as conn:
        channel = conn.default_channel
        producer = kombu.Producer(conn)
        for n in range(100):
            producer.publish({'n': n}, routing_key=setup.name)
        wake = f'{setup.prefix}{channel._wake_key(setup.name)}'
        assert setup.redis.llen(wake) == 1
        # The messages go elsewhere: a token is left behind.
        assert setup.queue(channel).purge() == 100
        producer.publish({'n': 100}, routing_key=setup.name)
        setup.queue(channel).get(no_ack=True)
        assert setup.redis.llen(wake) == 1
        with conn.Consumer(setup.queue, callbacks=[Mock()]):
            conn.transport.cycle.on_poll_start()
            sent = Mock(wraps=channel.client.connection.send_command)
            with patch.object(channel.client.connection, 'send_command',
                              sent):
                drain(conn, lambda: False, 3)
    # A pop and a wait per brpop_timeout (1 s), and the token's.
    assert sent.call_count <= 2 * 3 + 4


@pytest.mark.parametrize('end', ['delete', 'expire'])
def test_no_keys_left_behind(setup, end):
    exchange = kombu.Exchange(setup.name, 'direct')
    queue = kombu.Queue(setup.name, exchange, routing_key=setup.name,
                        expires=1 if end == 'expire' else None)
    got = []
    with setup.connect() as conn:
        queue(conn.default_channel).declare()
        producer = kombu.Producer(conn)
        for n in range(6):
            producer.publish({'n': n}, exchange=exchange,
                             routing_key=setup.name, priority=n % 2 * 9)
        with conn.Consumer(queue, callbacks=[
                lambda body, message: (got.append(body), message.ack())]):
            drain(conn, lambda: len(got) >= 3)
        queue(conn.default_channel).get(no_ack=False).ack()
        if end == 'delete':
            queue(conn.default_channel).delete()
        else:
            sleep(1.5)
    left = {key for key in setup.redis.scan_iter(f'*{setup.name}*')
            if not key.endswith('_unacked_mutex')}
    if end == 'expire':
        # Binding tables outlive an expired queue on main too, until a
        # sweep removes them.
        left = {key for key in left if '_kombu.binding' not in key}
    assert left == set()


def test_delivery_order(setup):
    exchange = kombu.Exchange(setup.name, 'direct')
    queues = [kombu.Queue(f'{setup.name}_{q}', exchange, routing_key=q)
              for q in 'ab']
    received = []

    def on_message(body, message):
        received.append((body['q'], body['n']))
        if body['n'] == 1 and not message.headers.get('redelivered'):
            message.reject(requeue=True)
        else:
            message.ack()

    with setup.connect() as conn:
        for q in queues:
            q(conn.default_channel).declare()
        producer = kombu.Producer(conn)
        for n, (q, priority) in enumerate([('a', 9), ('a', 0), ('a', 0),
                                           ('b', 9), ('b', 0), ('a', 3)]):
            producer.publish({'q': q, 'n': n}, exchange=exchange,
                             routing_key=q, priority=priority)
        with conn.Consumer(queues, callbacks=[on_message]):
            drain(conn, lambda: len(received) >= 7)
    # As on main: kombu's priority 0 first, first in first out within a
    # priority, queues in turn, a requeued message at the end of its
    # priority.
    assert received == [('a', 1), ('b', 4), ('a', 2), ('a', 1), ('a', 5),
                        ('b', 3), ('a', 0)]


@pytest.mark.skipif('PYTEST_XDIST_WORKER' in os.environ,
                    reason='changes the configuration of the whole server')
@pytest.mark.parametrize('condition', ['noeviction', 'script_flush'])
def test_server_wide_condition(condition):
    s = Setup(prefix=False)
    received = []
    config = s.redis.config_get('maxmemory*')
    try:
        with s.connect() as conn:
            with conn.Consumer(s.queue, callbacks=[
                    lambda body, message: received.append(body)]):
                kombu.Producer(conn).publish({'n': 1}, routing_key=s.name)
                if condition == 'noeviction':
                    # On main the record was refused, and the message lost.
                    s.redis.config_set('maxmemory-policy', 'noeviction')
                    s.redis.config_set('maxmemory', s.redis.info(
                        'memory')['used_memory'] - 100_000)
                    with pytest.raises(redis.exceptions.OutOfMemoryError):
                        s.redis.set(f'{s.name}_junk', 'x')
                else:
                    # As after a restart or a failover.
                    s.redis.script_flush()
                drain(conn, lambda: received, 10)
                stored = s.redis.hlen(s.unacked)
            conn.default_channel.qos._delivered.clear()
    finally:
        s.redis.config_set('maxmemory', config['maxmemory'])
        s.redis.config_set('maxmemory-policy', config['maxmemory-policy'])
        for key in s.redis.scan_iter(f'*{s.name}*'):
            s.redis.delete(key)
    assert received == [{'n': 1}]
    assert stored == 1


#: Seconds a blocking pop waits in the state machine: longer than any
#: example, so it returns only once something wakes it, or ``time_out``.
BLOCK = 60
ACK_QUEUES = ['a', 'b']
NO_ACK_QUEUE = 'n'
CONSUMERS = st.integers(0, 1)


class Killed(BaseException):
    """The consumer process dies."""


class Consumer:
    """One consumer process: a connection consuming every queue."""

    def __init__(self, machine):
        self.conn = machine.connect()
        self.channel = channel = self.conn.default_channel
        self.held = []
        channel.basic_qos(0, 2, False)
        for name in ACK_QUEUES:
            channel.basic_consume(
                machine.queue_name(name), False, self.held.append, name)
        channel.basic_consume(
            machine.queue_name(NO_ACK_QUEUE), True, machine.acked_on_receipt,
            NO_ACK_QUEUE)
        # The connection the poller would use for the blocking pop.
        self.conn.transport.cycle._client_registered(
            channel, channel.client, 'BRPOP')
        self.client_id = channel.client.client_id()

    def settle(self, client):
        """Wait until Redis ran the pop sent: it replied, or it blocks."""
        sock = self.channel.client.connection._sock
        for _ in range(500):
            if select.select([sock], [], [], 0.01)[0]:
                return
            info = client.client_list(client_id=[self.client_id])
            if info and 'b' in info[0]['flags']:
                return
        raise AssertionError('the pop neither replied nor blocked')

    def read(self, wait, machine):
        sock = self.channel.client.connection._sock
        if select.select([sock], [], [], wait)[0]:
            before = len(self.held) + len(machine.acked)
            try:
                self.channel._brpop_read()
            except Empty:
                pass
            # One message per reply at most.
            assert len(self.held) + len(machine.acked) - before <= 1

    def kill(self, client):
        # Whatever the process held, in memory or unread on its socket, is
        # gone with it.
        self.channel.client.connection.disconnect()
        self.channel._in_poll = None
        self.channel.qos._delivered.clear()
        self.held.clear()
        self.conn.release()
        # Until Redis sees the connection close, a pop it blocks in can
        # still take a message; wait for that, so runs are repeatable.
        while client.client_list(client_id=[self.client_id]):
            sleep(0.005)


class UnackedMachine(RuleBasedStateMachine):
    """Publish, poll, time out, kill, ack, reject and restore, in any order.

    Without ``token``, a publish is one from a kombu before the wake list.
    """

    @initialize(prefixed=st.booleans())
    def start(self, prefixed):
        self.name = f'unacked_property_{uuid4().hex}'
        self.prefix = f'{self.name}:' if prefixed else ''
        self.redis = redis.Redis(HOST, PORT)
        self.published = {}
        self.acked = set()
        self.exchange = kombu.Exchange(self.name, 'direct')
        self.publisher = self.connect()
        self.producer = kombu.Producer(self.publisher)
        for name in [*ACK_QUEUES, NO_ACK_QUEUE]:
            kombu.Queue(self.queue_name(name), self.exchange,
                        routing_key=name)(self.publisher.default_channel
                                          ).declare()
        self.janitor = self.connect().default_channel
        self.consumers = [Consumer(self), Consumer(self)]

    def connect(self):
        return connect(self.name, self.prefix)

    def queue_name(self, name):
        return f'{self.name}_{name}'

    def acked_on_receipt(self, message):
        assert message.headers['id'] not in self.acked, 'no_ack twice'
        self.acked.add(message.headers['id'])

    @rule(queue=st.sampled_from([*ACK_QUEUES, NO_ACK_QUEUE]),
          priority=st.sampled_from([0, 3, 9]), token=st.booleans())
    def publish(self, queue, priority, token):
        mid = len(self.published)
        self.published[mid] = queue
        put = (self.producer.channel._put if token
               else legacy_put(self.producer.channel))
        with patch.object(self.producer.channel, '_put', put):
            self.producer.publish(
                mid, exchange=self.exchange, routing_key=queue,
                priority=priority, headers={'id': mid})

    @rule(c=CONSUMERS)
    def send(self, c):
        """Send the next pop, as the poller does when it has credit."""
        channel = self.consumers[c].channel
        if not channel._in_poll and channel.qos.can_consume():
            channel._brpop_start(timeout=BLOCK)
            self.consumers[c].settle(self.redis)

    @rule(c=CONSUMERS)
    def poll(self, c):
        self.send(c)
        if self.consumers[c].channel._in_poll:
            self.consumers[c].read(0.05, self)

    @rule(c=CONSUMERS)
    def time_out(self, c):
        """End a blocking pop, as its timeout would."""
        if self.consumers[c].channel._in_poll:
            self.redis.client_unblock(self.consumers[c].client_id)

    @rule(c=CONSUMERS)
    def kill_while_delivering(self, c):
        """Kill the consumer right after it read a message from Redis."""
        consumer = self.consumers[c]
        self.send(c)
        if not consumer.channel._in_poll:
            return
        with patch.object(consumer.conn.transport, '_deliver',
                          side_effect=Killed):
            try:
                consumer.read(0.05, self)
            except Killed:
                self.kill(c)

    @rule(c=CONSUMERS)
    def kill(self, c):
        self.consumers[c].kill(self.redis)
        self.consumers[c] = Consumer(self)

    @rule(c=CONSUMERS, i=st.integers(0, 1), how=st.sampled_from(
        ['ack', 'reject', 'requeue']))
    def settle_message(self, c, i, how):
        held = self.consumers[c].held
        if held:
            message = held.pop(i % len(held))
            if how == 'ack':
                message.ack()
            else:
                message.reject(requeue=how == 'requeue')
            if how != 'requeue':
                self.acked.add(message.headers['id'])

    @rule()
    def restore_visible(self):
        """Restore the messages of killed consumers, as their visibility
        timeout passes."""
        for consumer in self.consumers:
            # Replies already sent belong to live consumers.
            if consumer.channel._in_poll:
                consumer.read(0.2, self)
        held = {m.delivery_tag for c in self.consumers for m in c.held}
        index = f'{self.prefix}{self.name}_unacked_index'
        for tag in self.redis.zrange(index, 0, -1):
            if tag.decode() not in held:
                self.redis.zadd(index, {tag: 0})
        self.janitor.qos.restore_visible(num=None, interval=1)

    @invariant()
    def every_message_is_in_one_place(self):
        """In a queue list, in unacked, or acked: exactly one of them."""
        found = Counter()
        for name in [*ACK_QUEUES, NO_ACK_QUEUE]:
            for pri in self.janitor.priority_steps:
                key = self.janitor._q_for_pri(self.queue_name(name), pri)
                for raw in self.redis.lrange(f'{self.prefix}{key}', 0, -1):
                    found[loads(raw)['headers']['id']] += 1
        for raw in self.redis.hvals(f'{self.prefix}{self.name}_unacked'):
            mid = loads(raw)[0]['headers']['id']
            assert self.published[mid] != NO_ACK_QUEUE, 'no_ack recorded'
            found[mid] += 1
        for mid in self.acked:
            found[mid] += 1
        lost = {mid: found[mid] for mid, queue in self.published.items()
                if queue != NO_ACK_QUEUE and found[mid] != 1}
        assert not lost, f'messages not in exactly one place: {lost}'

    @invariant()
    def unacked_and_its_index_hold_the_same_tags(self):
        tags = self.redis.hkeys(f'{self.prefix}{self.name}_unacked')
        index = self.redis.zrange(
            f'{self.prefix}{self.name}_unacked_index', 0, -1)
        assert sorted(tags) == sorted(index)

    @invariant()
    def consumers_keep_to_prefetch_and_hold_no_extra_state(self):
        for consumer in self.consumers:
            qos = consumer.channel.qos
            assert len(consumer.held) <= 2
            assert len(qos._delivered) - len(qos._dirty) == len(
                consumer.held)
            assert not getattr(qos, '_recorded', ())

    def teardown(self):
        if not hasattr(self, 'consumers'):
            return
        for consumer in self.consumers:
            if consumer.channel._in_poll:
                self.redis.client_unblock(consumer.client_id)
            consumer.kill(self.redis)
        self.janitor.connection.client.release()
        self.publisher.release()
        for key in self.redis.scan_iter(f'*{self.name}*'):
            self.redis.delete(key)
        self.redis.close()


def test_every_message_is_in_one_place():
    run_state_machine_as_test(UnackedMachine, settings=settings(
        max_examples=EXAMPLES, stateful_step_count=50, deadline=None,
        database=None, suppress_health_check=[HealthCheck.too_slow]))


#: Strings that look like the parts of a message the script searches for.
TRICKY = ['delivery_tag', 'properties', 'delivery_info', 'exchange',
          'routing_key', '"properties": {', '"delivery_tag": "x"', '}}',
          '{', '"', '\\', '\\"', ', "properties": {"delivery_tag": "x"}}']
KEYS = st.one_of(st.sampled_from(TRICKY), st.text(max_size=8))
TEXT = st.one_of(st.sampled_from(TRICKY), st.text(max_size=12))
NAMES = st.one_of(st.sampled_from(['', 'celery', 'a.b-c:d', 'x/y', 'é', '"']),
                  st.text(max_size=6))


def json_values(floats):
    return st.recursive(
        st.none() | st.booleans() | st.integers(-2 ** 64, 2 ** 64)
        | floats | TEXT,
        lambda inner: st.lists(inner, max_size=3)
        | st.dictionaries(KEYS, inner, max_size=3),
        max_leaves=8)


@st.composite
def messages(draw):
    """A message as kombu writes it, or in some other layout, or not one."""
    kind = draw(st.sampled_from(['kombu', 'kombu', 'other layout',
                                 'json', 'bytes']))
    if kind == 'bytes':
        return draw(st.binary(max_size=64))
    if kind == 'json':
        return dumps(draw(json_values(st.floats()))).encode()
    props = {'delivery_info': {'exchange': draw(NAMES),
                               'routing_key': draw(NAMES)},
             'priority': draw(st.integers(0, 9)),
             'body_encoding': 'base64',
             'delivery_tag': draw(st.one_of(st.uuids().map(str), NAMES))}
    props.update(draw(st.dictionaries(
        KEYS.filter(lambda k: k not in props),
        json_values(st.floats(allow_nan=False, allow_infinity=False)),
        max_size=3)))
    order = draw(st.permutations(list(props)))
    message = {'body': draw(TEXT), 'content-encoding': 'utf-8',
               'content-type': 'application/json',
               'headers': draw(st.dictionaries(KEYS, json_values(st.floats()),
                                               max_size=4)),
               'properties': {k: props[k] for k in order}}
    if kind == 'other layout':
        # Including keys that end like the one the script looks for, with
        # a value that looks like properties.
        decoy = {**props, 'delivery_tag': 'decoy'}
        extra = draw(st.dictionaries(
            st.one_of(KEYS, st.sampled_from(['x"properties', 'x\\"properties',
                                             'x\\'])).filter(
                lambda k: k not in message),
            st.one_of(json_values(st.floats()), st.just(decoy)), max_size=2))
        keys = draw(st.one_of(st.just([*message, *extra]),
                              st.permutations([*message, *extra])))
        message = {k: {**message, **extra}[k] for k in keys}
    return dumps(message).encode()


def printable(value):
    return isinstance(value, str) and all(' ' <= c <= '~' for c in value)


@pytest.fixture(scope='module')
def popping():
    name = f'unacked_pop_script_{uuid4().hex}'
    client = redis.Redis(HOST, PORT)
    with connect(name) as conn:
        channel = conn.default_channel
        keys, args = channel._pop_unacked_args([name], [True])

        def pop(raw):
            client.delete(*keys)
            client.lpush(name, raw)
            reply = channel._pop_unacked(keys=keys, args=args, client=client)
            return reply, client.hgetall(keys[0]), client.zrange(keys[1], 0, -1)
        yield pop
        client.delete(*keys)


@given(raw=messages())
@settings(max_examples=EXAMPLES * 6, deadline=None, database=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_pop_script_records_what_qos_append_would(popping, raw):
    (_, item, tag, _), stored, index = popping(raw)
    assert item == raw
    try:
        message = loads(raw)
        props = message['properties']
        tag_wanted = props['delivery_tag']
        exchange = props['delivery_info']['exchange']
        routing_key = props['delivery_info']['routing_key']
    except Exception:
        recordable = False
    else:
        recordable = all(map(printable, [tag_wanted, exchange, routing_key])
                         ) and tag_wanted != ''
    assert list(stored) == index
    if not tag:
        assert not stored
        # Every message kombu writes, with printable names, is recorded.
        assert not (recordable and raw == dumps(message).encode()
                    and list(message)[-1] == 'properties')
        return
    assert recordable
    assert tag.decode() == tag_wanted
    assert index == [tag]
    # The value QoS.append writes, byte for byte, when the message is laid
    # out as kombu's json writes it; else the same value once loaded.
    if raw == dumps(message).encode():
        assert stored[tag] == dumps([message, exchange, routing_key]).encode()
    assert loads(stored[tag]) == [message, exchange, routing_key]
