from __future__ import annotations

from unittest.mock import patch

import pytest

from kombu.utils import limits


class test_TokenBucket:

    def test_can_consume_does_not_double_admit_after_idle(self):
        clock = [1000.0]
        with patch.object(limits, 'monotonic', lambda: clock[0]):
            x = limits.TokenBucket(fill_rate=0.1, capacity=1)
            clock[0] = 1600.0

            # the bucket sat full for 600s, so it holds exactly one token
            first = x.can_consume()
            clock[0] = 1600.001
            second = x.can_consume()

            assert first is True
            assert second is False

    def test_idle_bucket_admits_only_its_capacity(self):
        clock = [1000.0]
        with patch.object(limits, 'monotonic', lambda: clock[0]):
            x = limits.TokenBucket(fill_rate=0.1, capacity=10)
            clock[0] = 1600.0

            # 600s of sitting at capacity adds nothing to a bucket already full
            admitted = [x.can_consume() for _ in range(12)]

            assert admitted == [True] * 10 + [False] * 2

    def test_refills_at_fill_rate(self):
        clock = [1000.0]
        with patch.object(limits, 'monotonic', lambda: clock[0]):
            x = limits.TokenBucket(fill_rate=0.1, capacity=1)
            assert x.can_consume()

            # one token per 10s, so 5s buys half a token and 10s buys a whole one
            clock[0] = 1005.0
            too_soon = x.can_consume()
            clock[0] = 1010.0
            on_time = x.can_consume()

            assert too_soon is False
            assert on_time is True
            assert x.expected_time() == pytest.approx(10.0)
