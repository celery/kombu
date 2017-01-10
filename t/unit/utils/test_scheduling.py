from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock

from kombu.utils.scheduling import FairCycle, cycle_by_name


class MyEmpty(Exception):
    pass


def consume(fun, n):
    r = []
    for i in range(n):
        r.append(fun(Mock(name='callback')))
    return r


class test_FairCycle:

    def test_cycle(self):
        resources = ['a', 'b', 'c', 'd', 'e']
        callback = Mock(name='callback')

        def echo(r, timeout=None):
            return r

        # cycle should be ['a', 'b', 'c', 'd', 'e', ... repeat]
        cycle = FairCycle(echo, resources, MyEmpty)
        for i in range(len(resources)):
            assert cycle.get(callback) == resources[i]
        for i in range(len(resources)):
            assert cycle.get(callback) == resources[i]

    def test_cycle_breaks(self):
        resources = ['a', 'b', 'c', 'd', 'e']

        def echo(r, callback):
            if r == 'c':
                raise MyEmpty(r)
            return r

        cycle = FairCycle(echo, resources, MyEmpty)
        assert consume(cycle.get, len(resources)) == [
            'a', 'b', 'd', 'e', 'a',
        ]
        assert consume(cycle.get, len(resources)) == [
            'b', 'd', 'e', 'a', 'b',
        ]
        cycle2 = FairCycle(echo, ['c', 'c'], MyEmpty)
        with pytest.raises(MyEmpty):
            consume(cycle2.get, 3)

    def test_cycle_no_resources(self):
        cycle = FairCycle(None, [], MyEmpty)
        cycle.pos = 10

        with pytest.raises(MyEmpty):
            cycle._next()

    def test__repr__(self):
        assert repr(FairCycle(lambda x: x, [1, 2, 3], MyEmpty))


def test_round_robin_cycle():
    it = cycle_by_name('round_robin')(['A', 'B', 'C'])
    assert it.consume(3) == ['A', 'B', 'C']
    it.rotate('B')
    assert it.consume(3) == ['A', 'C', 'B']
    it.rotate('A')
    assert it.consume(3) == ['C', 'B', 'A']
    it.rotate('A')
    assert it.consume(3) == ['C', 'B', 'A']
    it.rotate('C')
    assert it.consume(3) == ['B', 'A', 'C']


def test_priority_cycle():
    it = cycle_by_name('priority')(['A', 'B', 'C'])
    assert it.consume(3) == ['A', 'B', 'C']
    it.rotate('B')
    assert it.consume(3) == ['A', 'B', 'C']
    it.rotate('A')
    assert it.consume(3) == ['A', 'B', 'C']
    it.rotate('A')
    assert it.consume(3) == ['A', 'B', 'C']
    it.rotate('C')
    assert it.consume(3) == ['A', 'B', 'C']


def test_sorted_cycle():
    it = cycle_by_name('sorted')(['B', 'C', 'A'])
    assert it.consume(3) == ['A', 'B', 'C']
    it.rotate('B')
    assert it.consume(3) == ['A', 'B', 'C']
    it.rotate('A')
    assert it.consume(3) == ['A', 'B', 'C']
    it.rotate('A')
    assert it.consume(3) == ['A', 'B', 'C']
    it.rotate('C')
    assert it.consume(3) == ['A', 'B', 'C']
