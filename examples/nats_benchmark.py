#!/usr/bin/env python3
"""Kombu NATS transport benchmark — default-mode vs raw-body mode.

Measures publish throughput, consume throughput, and per-message round-trip
latency for three payload sizes (small / medium / large).  Each mode uses an
isolated JetStream stream stored in memory so results are not contaminated by
disk I/O or by messages left from previous runs.

Usage
-----
    # Run against a local NATS server with JetStream enabled:
    nats-server -js &
    python examples/nats_benchmark.py

    # Custom options:
    python examples/nats_benchmark.py --server demo.nats.io --count 1000
    python examples/nats_benchmark.py --sizes small medium --count 2000
    python examples/nats_benchmark.py --no-cleanup   # leave streams for inspection

Requirements
------------
    nats-py>=2.0, kombu (this repo)
"""

from __future__ import annotations

import argparse
import logging
import statistics
import time
from dataclasses import dataclass, field

from nats.js.api import StorageType

from kombu import Connection, Consumer, Exchange, Producer, Queue

# Silence nats-py's internal read-loop error logs that fire during
# normal channel drain/close teardown (e.g. DrainTimeoutError).
# Real connection errors are still surfaced via Python exceptions.
logging.getLogger("nats").setLevel(logging.CRITICAL)

# ---------------------------------------------------------------------------
# Payload sizes
# ---------------------------------------------------------------------------

PAYLOAD_SIZES: dict[str, int] = {
    "small":  100,
    "medium": 1_024,
    "large":  10 * 1_024,
}


def make_payload(size_bytes: int) -> bytes:
    return b"x" * size_bytes


# ---------------------------------------------------------------------------
# Stream / queue helpers
# ---------------------------------------------------------------------------

def _queue_tag(mode_slug: str, size_name: str, run_id: int) -> str:
    return f"bench_{mode_slug}_{size_name}_{run_id}"


def make_resources(tag: str) -> tuple[Exchange, Queue]:
    exchange = Exchange(f"x_{tag}", "direct", durable=False)
    queue = Queue(tag, exchange=exchange, routing_key=tag)
    return exchange, queue


def make_transport_options(raw_body: bool) -> dict:
    return {
        "stream_config": {"storage": StorageType.MEMORY},
        "nats_raw_body": raw_body,
    }


# ---------------------------------------------------------------------------
# Benchmark phases
# ---------------------------------------------------------------------------

def publish_n(
    url: str,
    opts: dict,
    exchange: Exchange,
    queue: Queue,
    payload: bytes,
    count: int,
) -> float:
    """Publish *count* messages and return the wall-clock seconds elapsed."""
    with Connection(url, transport_options=opts) as conn:
        with conn.channel() as ch:
            queue(ch).declare()
            producer = Producer(ch, exchange=exchange, routing_key=queue.routing_key)
            t0 = time.perf_counter()
            for _ in range(count):
                producer.publish(payload)
    return time.perf_counter() - t0


def consume_n(
    url: str,
    opts: dict,
    queue: Queue,
    count: int,
) -> float:
    """Consume exactly *count* messages and return the wall-clock seconds elapsed."""
    received = 0

    def on_message(body, message):
        nonlocal received
        received += 1
        message.ack()

    t0 = time.perf_counter()
    with Connection(url, transport_options=opts) as conn:
        with Consumer(conn, [queue], callbacks=[on_message], no_ack=False):
            while received < count:
                conn.drain_events(timeout=30)
    return time.perf_counter() - t0


def measure_latency(
    url: str,
    opts: dict,
    exchange: Exchange,
    queue: Queue,
    payload: bytes,
    samples: int,
) -> list[float]:
    """Return per-message round-trip latency samples (in seconds).

    Each sample is: time from ``producer.publish()`` returning to the
    consumer callback being invoked.
    """
    latencies: list[float] = []
    arrived = [False]

    def on_message(body, message):
        latencies.append(time.perf_counter())
        message.ack()
        arrived[0] = True

    # Declare the queue once before the loop.
    with Connection(url, transport_options=opts) as conn:
        with conn.channel() as ch:
            queue(ch).declare()

    with Connection(url, transport_options=opts) as conn:
        # Reuse a single producer channel for all iterations — avoids
        # NATS drain overhead on per-iteration channel open/close.
        producer = conn.Producer(exchange=exchange, routing_key=queue.routing_key)
        with Consumer(conn, [queue], callbacks=[on_message], no_ack=False):
            # Prime the consumer subscription; ignore timeout if no messages.
            try:
                conn.drain_events(timeout=0.1)
            except TimeoutError:
                pass
            for _ in range(samples):
                arrived[0] = False
                t_send = time.perf_counter()
                producer.publish(payload)
                while not arrived[0]:
                    conn.drain_events(timeout=5)
                latencies[-1] = latencies[-1] - t_send

    return latencies


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class ScenarioResult:
    mode_label: str
    size_name: str
    payload_bytes: int
    count: int
    pub_seconds: float
    cons_seconds: float
    latency_samples: list[float] = field(default_factory=list)

    @property
    def pub_throughput(self) -> float:
        return self.count / self.pub_seconds

    @property
    def cons_throughput(self) -> float:
        return self.count / self.cons_seconds

    @property
    def lat_mean_ms(self) -> float:
        return statistics.mean(self.latency_samples) * 1000 if self.latency_samples else 0.0

    @property
    def lat_p50_ms(self) -> float:
        return statistics.median(self.latency_samples) * 1000 if self.latency_samples else 0.0

    @property
    def lat_p95_ms(self) -> float:
        if not self.latency_samples:
            return 0.0
        s = sorted(self.latency_samples)
        idx = max(0, int(len(s) * 0.95) - 1)
        return s[idx] * 1000

    @property
    def lat_min_ms(self) -> float:
        return min(self.latency_samples) * 1000 if self.latency_samples else 0.0


# ---------------------------------------------------------------------------
# Run a single scenario
# ---------------------------------------------------------------------------

def run_scenario(
    url: str,
    mode_label: str,
    mode_slug: str,
    raw_body: bool,
    size_name: str,
    count: int,
    lat_samples: int,
    run_id: int,
) -> ScenarioResult:
    payload = make_payload(PAYLOAD_SIZES[size_name])
    opts = make_transport_options(raw_body)

    # Unique stream/queue per (mode, size, run) to avoid cross-contamination.
    tag = _queue_tag(mode_slug, size_name, run_id)
    exchange, queue = make_resources(tag)

    lat_tag = _queue_tag(f"{mode_slug}_lat", size_name, run_id)
    lat_exchange, lat_queue = make_resources(lat_tag)

    pub_s = publish_n(url, opts, exchange, queue, payload, count)
    cons_s = consume_n(url, opts, queue, count)
    lats = measure_latency(url, opts, lat_exchange, lat_queue, payload, lat_samples)

    return ScenarioResult(
        mode_label=mode_label,
        size_name=size_name,
        payload_bytes=PAYLOAD_SIZES[size_name],
        count=count,
        pub_seconds=pub_s,
        cons_seconds=cons_s,
        latency_samples=lats,
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

COL = 26  # label column width


def _row(label: str, value: str) -> None:
    print(f"    {label:<{COL}}{value}")


def _throughput_str(r: ScenarioResult, phase: str) -> str:
    tp = r.pub_throughput if phase == "pub" else r.cons_throughput
    secs = r.pub_seconds if phase == "pub" else r.cons_seconds
    return f"{tp:>10,.0f} msg/s   ({secs:.3f} s total)"


def _latency_str(r: ScenarioResult) -> str:
    return (
        f"mean={r.lat_mean_ms:.2f} ms  "
        f"p50={r.lat_p50_ms:.2f} ms  "
        f"p95={r.lat_p95_ms:.2f} ms  "
        f"min={r.lat_min_ms:.2f} ms"
    )


def _speedup_arrow(ratio: float, a_label: str, b_label: str) -> str:
    if ratio > 1.01:
        return f"  ↑ {a_label} is {ratio:.2f}× faster"
    if ratio < 0.99:
        return f"  ↑ {b_label} is {1/ratio:.2f}× faster"
    return "  ≈ equal"


def print_report(
    results: dict[str, dict[str, ScenarioResult]],
    sizes: list[str],
) -> None:
    mode_labels = list(results)
    divider = "─" * 72

    print()
    print("╔" + "═" * 70 + "╗")
    print("║{:^70}║".format("NATS Transport Benchmark  —  kombu"))
    print("╚" + "═" * 70 + "╝")

    for size_name in sizes:
        size_bytes = PAYLOAD_SIZES[size_name]
        print(f"\n  Payload: {size_bytes:,} bytes  ({size_name})")
        print(f"  {divider}")

        for label in mode_labels:
            r = results[label][size_name]
            print(f"\n  [{label}]")
            _row("Publish throughput:", _throughput_str(r, "pub"))
            _row("Consume throughput:", _throughput_str(r, "cons"))
            _row("Round-trip latency:", _latency_str(r))

        # Comparison between first two modes (if exactly two).
        if len(mode_labels) == 2:
            a = results[mode_labels[0]][size_name]
            b = results[mode_labels[1]][size_name]

            pub_ratio = a.pub_throughput / b.pub_throughput
            cons_ratio = a.cons_throughput / b.cons_throughput
            lat_ratio = b.lat_mean_ms / a.lat_mean_ms  # >1 means b is slower → a wins

            print(f"\n  [comparison: {mode_labels[0]}  vs  {mode_labels[1]}]")
            _row(
                "Publish:",
                f"{pub_ratio:.2f}×" + _speedup_arrow(pub_ratio, mode_labels[0], mode_labels[1]),
            )
            _row(
                "Consume:",
                f"{cons_ratio:.2f}×" + _speedup_arrow(cons_ratio, mode_labels[0], mode_labels[1]),
            )
            _row(
                "Latency:",
                f"ratio={lat_ratio:.2f}×" + _speedup_arrow(lat_ratio, mode_labels[0], mode_labels[1]),
            )

    print(f"\n  {divider}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

MODES: dict[str, tuple[str, bool]] = {
    # label                          slug         raw_body
    "default  (raw_body=False)": ("default", False),
    "raw-body (raw_body=True)":  ("raw",     True),
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark kombu NATS transport: default vs raw-body mode",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--server", default="localhost", metavar="HOST",
                        help="NATS server hostname (default: localhost)")
    parser.add_argument("--port", type=int, default=4222,
                        help="NATS server port (default: 4222)")
    parser.add_argument("--count", type=int, default=500, metavar="N",
                        help="Messages per throughput run (default: 500)")
    parser.add_argument("--latency-samples", type=int, default=30, metavar="N",
                        help="Messages for latency measurement (default: 30)")
    parser.add_argument(
        "--sizes", nargs="+", choices=list(PAYLOAD_SIZES), default=list(PAYLOAD_SIZES),
        help="Payload sizes to test (default: all)",
    )
    parser.add_argument("--warmup", type=int, default=50, metavar="N",
                        help="Warm-up messages published before timing (default: 50)")
    args = parser.parse_args()

    url = f"nats://{args.server}:{args.port}"
    run_id = int(time.time() * 1000) % 1_000_000

    print(f"\n  Server  : {url}")
    print(f"  Count   : {args.count} msg/run   Latency samples: {args.latency_samples}")
    print(f"  Sizes   : {', '.join(args.sizes)}")
    print(f"  Warm-up : {args.warmup} messages\n")

    # --- Warm-up: establish connections and prime JetStream ---
    if args.warmup > 0:
        print("  Warming up ...", end=" ", flush=True)
        wm_tag = f"warmup_{run_id}"
        wm_exchange, wm_queue = make_resources(wm_tag)
        wm_opts = make_transport_options(False)
        publish_n(url, wm_opts, wm_exchange, wm_queue, b"w" * 64, args.warmup)
        consume_n(url, wm_opts, wm_queue, args.warmup)
        print("done\n")

    # --- Main benchmark runs ---
    all_results: dict[str, dict[str, ScenarioResult]] = {label: {} for label in MODES}

    for label, (slug, raw_body) in MODES.items():
        for size_name in args.sizes:
            print(f"  [{label}]  {size_name} ...", end=" ", flush=True)
            result = run_scenario(
                url=url,
                mode_label=label,
                mode_slug=slug,
                raw_body=raw_body,
                size_name=size_name,
                count=args.count,
                lat_samples=args.latency_samples,
                run_id=run_id,
            )
            all_results[label][size_name] = result
            print(
                f"pub={result.pub_throughput:,.0f} msg/s  "
                f"cons={result.cons_throughput:,.0f} msg/s  "
                f"lat(mean)={result.lat_mean_ms:.1f} ms"
            )

    print_report(all_results, args.sizes)


if __name__ == "__main__":
    main()
