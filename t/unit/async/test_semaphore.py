from __future__ import absolute_import, unicode_literals

from kombu.async.semaphore import LaxBoundedSemaphore


class test_LaxBoundedSemaphore:

    def test_over_release(self):
        x = LaxBoundedSemaphore(2)
        calls = []
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
