"""Compare ordinary and batched Redis publication with simulated latency."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from contextlib import nullcontext
from unittest.mock import patch

import redis

from kombu import Connection, Producer


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='redis://localhost:6379/0')
    parser.add_argument('--messages', type=int, default=100)
    parser.add_argument('--latency-ms', type=float, default=10)
    parser.add_argument('--rounds', type=int, default=7)
    parser.add_argument('--warmups', type=int, default=1)
    parser.add_argument('--max-size', type=int, default=1000)
    parser.add_argument('--json', action='store_true', dest='as_json')
    return parser.parse_args()


def validate_args(args):
    for name in ('messages', 'rounds', 'max_size'):
        if getattr(args, name) <= 0:
            raise SystemExit(f'--{name.replace("_", "-")} must be positive')
    if args.warmups < 0:
        raise SystemExit('--warmups cannot be negative')
    if args.latency_ms < 0:
        raise SystemExit('--latency-ms cannot be negative')


def benchmark(args):
    queue = 'kombu_publish_batch_benchmark'
    delay = args.latency_ms / 1000
    original_send = redis.connection.Connection.send_packed_command

    def delayed_send(connection, command, check_health=True):
        time.sleep(delay)
        return original_send(
            connection,
            command,
            check_health=check_health,
        )

    samples = {'ordinary': [], 'batched': []}
    client = redis.Redis.from_url(args.url)
    with Connection(args.url) as connection:
        with connection.channel() as channel:
            producer = Producer(channel, serializer='json')

            def run(mode):
                client.delete(queue)
                context = (
                    producer.batch(max_size=args.max_size)
                    if mode == 'batched'
                    else nullcontext()
                )
                started = time.perf_counter()
                with patch.object(
                    redis.connection.Connection,
                    'send_packed_command',
                    delayed_send,
                ):
                    with context:
                        for index in range(args.messages):
                            producer.publish(
                                {'index': index},
                                exchange='',
                                routing_key=queue,
                            )
                elapsed = time.perf_counter() - started
                queued = client.llen(queue)
                if queued != args.messages:
                    raise RuntimeError(
                        f'expected {args.messages} messages, found {queued}',
                    )
                return elapsed

            for _ in range(args.warmups):
                run('ordinary')
                run('batched')

            for round_number in range(args.rounds):
                modes = (
                    ('ordinary', 'batched')
                    if round_number % 2 == 0
                    else ('batched', 'ordinary')
                )
                for mode in modes:
                    samples[mode].append(run(mode))

            client.delete(queue)

    ordinary_median = statistics.median(samples['ordinary'])
    batched_median = statistics.median(samples['batched'])
    return {
        'url': args.url,
        'messages': args.messages,
        'simulated_latency_ms': args.latency_ms,
        'rounds': args.rounds,
        'max_size': args.max_size,
        'ordinary_seconds': samples['ordinary'],
        'batched_seconds': samples['batched'],
        'ordinary_median_seconds': ordinary_median,
        'batched_median_seconds': batched_median,
        'speedup': ordinary_median / batched_median,
    }


def main():
    args = parse_args()
    validate_args(args)
    result = benchmark(args)
    if args.as_json:
        print(json.dumps(result, indent=2))
        return

    print(
        f"{result['messages']} messages, "
        f"{result['simulated_latency_ms']:.1f} ms simulated latency, "
        f"{result['rounds']} rounds",
    )
    print(
        'ordinary median: '
        f"{result['ordinary_median_seconds']:.4f} seconds",
    )
    print(
        'batched median:  '
        f"{result['batched_median_seconds']:.4f} seconds",
    )
    print(f"speedup:         {result['speedup']:.1f}x")


if __name__ == '__main__':
    main()
