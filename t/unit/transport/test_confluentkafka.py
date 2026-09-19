from __future__ import annotations

from unittest.mock import Mock

import pytest

pytest.importorskip('confluent_kafka')

from kombu.transport import confluentkafka  # noqa: E402


class test_QoS:

    def setup_method(self):
        self.channel = Mock(name='channel')
        self.qos = confluentkafka.QoS(self.channel, prefetch_count=2)
        # ``_not_yet_acked`` is a class attribute; give each test its own.
        self.qos._not_yet_acked = {}

    def teardown_method(self):
        self.qos._on_collect.cancel()

    def test_can_consume__prefetch_limit(self):
        assert self.qos.can_consume()
        assert self.qos.can_consume_max_estimate() == 2
        self.qos.append(Mock(name='m1'), 1)
        assert self.qos.can_consume()
        assert self.qos.can_consume_max_estimate() == 1
        self.qos.append(Mock(name='m2'), 2)
        assert not self.qos.can_consume()
        assert self.qos.can_consume_max_estimate() == 0

    def test_can_consume__no_prefetch_limit(self):
        self.qos.prefetch_count = 0
        self.qos.append(Mock(name='m1'), 1)
        assert self.qos.can_consume()
        assert self.qos.can_consume_max_estimate() == 1

    def test_can_consume__guard(self):
        allow = [False]
        calls = []

        def guard(qos):
            calls.append(qos)
            return allow[0]

        self.qos.guard = guard
        assert not self.qos.can_consume()
        assert self.qos.can_consume_max_estimate() == 0

        allow[0] = True
        assert self.qos.can_consume()
        assert self.qos.can_consume_max_estimate() == 2
        assert calls == [self.qos] * 4

    def test_can_consume__guard_and_prefetch_limit(self):
        self.qos.guard = lambda qos: True
        self.qos.append(Mock(name='m1'), 1)
        self.qos.append(Mock(name='m2'), 2)
        assert not self.qos.can_consume()
        assert self.qos.can_consume_max_estimate() == 0

    def test_guard__from_constructor(self):
        qos = confluentkafka.QoS(self.channel, guard=lambda q: False)
        try:
            assert not qos.can_consume()
            assert qos.can_consume_max_estimate() == 0
        finally:
            qos._on_collect.cancel()
