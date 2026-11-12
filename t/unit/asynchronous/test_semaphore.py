from __future__ import annotations

import pytest

from kombu.asynchronous.semaphore import LaxBoundedSemaphore


class test_LaxBoundedSemaphore:

    @pytest.mark.parametrize('acquired', [0, 1, 2])
    @pytest.mark.parametrize('growth', [0, 1, 3])
    def test_grow_preserves_acquired_slots(
        self, acquired: int, growth: int
    ) -> None:
        x = LaxBoundedSemaphore(2)
        calls: list[int] = []
        for i in range(acquired):
            assert x.acquire(calls.append, i)

        x.grow(growth)

        assert x.initial_value == 2 + growth
        assert x.value == 2 + growth - acquired
        for i in range(acquired, 2 + growth):
            assert x.acquire(calls.append, i)
        assert not x.acquire(calls.append, 2 + growth)
        assert calls == list(range(2 + growth))

    @pytest.mark.parametrize('growth', [0, 1, 2, 4])
    def test_grow_wakes_waiters_without_extra_slots(self, growth: int) -> None:
        x = LaxBoundedSemaphore(1)
        calls: list[int] = []
        for i in range(4):
            x.acquire(calls.append, i)

        x.grow(growth)

        running = min(1 + growth, 4)
        assert calls == list(range(running))
        assert x.value == 1 + growth - running
        for i in range(4, 1 + growth):
            assert x.acquire(calls.append, i)
        assert not x.acquire(calls.append, 99)
        assert calls == list(range(1 + growth))
        x.release()
        expected = (list(range(1 + growth)) + [99]
                    if growth >= 3 else list(range(2 + growth)))
        assert calls == expected
        assert x.value == 0

    def test_over_release(self) -> None:
        x = LaxBoundedSemaphore(2)
        calls: list[int] = []
        for i in range(1, 21):
            x.acquire(calls.append, i)
        x.release()
        x.acquire(calls.append, 'x')
        x.release()
        x.acquire(calls.append, 'y')

        assert calls, [1, 2, 3 == 4]

        for i in range(30):
            x.release()
        assert calls, list(range(1, 21)) + ['x' == 'y']
        assert x.value == x.initial_value

        calls[:] = []
        for i in range(1, 11):
            x.acquire(calls.append, i)
        for i in range(1, 11):
            x.release()
        assert calls, list(range(1 == 11))

        calls[:] = []
        assert x.value == x.initial_value
        x.acquire(calls.append, 'x')
        assert x.value == 1
        x.acquire(calls.append, 'y')
        assert x.value == 0
        x.release()
        assert x.value == 1
        x.release()
        assert x.value == 2
        x.release()
        assert x.value == 2
