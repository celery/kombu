from __future__ import absolute_import, unicode_literals

import pickle

from heapq import heappush
from time import time

from case import Mock

from kombu.clocks import LamportClock, timetuple


class test_LamportClock:

    def test_clocks(self):
        c1 = LamportClock()
        c2 = LamportClock()

        c1.forward()
        c2.forward()
        c1.forward()
        c1.forward()
        c2.adjust(c1.value)
        assert c2.value == c1.value + 1
        assert repr(c1)

        c2_val = c2.value
        c2.forward()
        c2.forward()
        c2.adjust(c1.value)
        assert c2.value == c2_val + 2 + 1

        c1.adjust(c2.value)
        assert c1.value == c2.value + 1

    def test_sort(self):
        c = LamportClock()
        pid1 = 'a.example.com:312'
        pid2 = 'b.example.com:311'

        events = []

        m1 = (c.forward(), pid1)
        heappush(events, m1)
        m2 = (c.forward(), pid2)
        heappush(events, m2)
        m3 = (c.forward(), pid1)
        heappush(events, m3)
        m4 = (30, pid1)
        heappush(events, m4)
        m5 = (30, pid2)
        heappush(events, m5)

        assert str(c) == str(c.value)

        assert c.sort_heap(events) == m1
        assert c.sort_heap([m4, m5]) == m4
        assert c.sort_heap([m4, m5, m1]) == m4


class test_timetuple:

    def test_repr(self):
        x = timetuple(133, time(), 'id', Mock())
        assert repr(x)

    def test_pickleable(self):
        x = timetuple(133, time(), 'id', 'obj')
        assert pickle.loads(pickle.dumps(x)) == tuple(x)

    def test_order(self):
        t1 = time()
        t2 = time() + 300  # windows clock not reliable
        a = timetuple(133, t1, 'A', 'obj')
        b = timetuple(140, t1, 'A', 'obj')
        assert a.__getnewargs__()
        assert a.clock == 133
        assert a.timestamp == t1
        assert a.id == 'A'
        assert a.obj == 'obj'
        assert a <= b
        assert b >= a

        assert (timetuple(134, time(), 'A', 'obj').__lt__(tuple()) is
                NotImplemented)
        assert timetuple(134, t2, 'A', 'obj') > timetuple(133, t1, 'A', 'obj')
        assert timetuple(134, t1, 'B', 'obj') > timetuple(134, t1, 'A', 'obj')
        assert (timetuple(None, t2, 'B', 'obj') >
                timetuple(None, t1, 'A', 'obj'))
