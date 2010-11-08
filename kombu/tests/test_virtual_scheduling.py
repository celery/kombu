import unittest2 as unittest

from kombu.transport.virtual.scheduling import FairCycle


class MyEmpty(Exception):
    pass

def consume(fun, n):
    r = []
    for i in range(n):
        r.append(fun())
    return r


class test_FairCycle(unittest.TestCase):

    def test_cycle(self):
        resources = ["a", "b", "c", "d", "e"]

        def echo(r):
            return r

        # cycle should be ["a", "b", "c", "d", "e", ... repeat]
        cycle = FairCycle(echo, resources, MyEmpty)
        for i in range(len(resources)):
            self.assertEqual(cycle.get(), (resources[i],
                                           resources[i]))
        for i in range(len(resources)):
            self.assertEqual(cycle.get(), (resources[i],
                                           resources[i]))

    def test_cycle_breaks(self):
        resources = ["a", "b", "c", "d", "e"]

        def echo(r):
            if r == "c":
                raise MyEmpty(r)
            return r

        cycle = FairCycle(echo, resources, MyEmpty)
        self.assertEqual(consume(cycle.get, len(resources)),
                        [("a", "a"), ("b", "b"), ("d", "d"),
                         ("e", "e"), ("a", "a")])
        self.assertEqual(consume(cycle.get, len(resources)),
                        [("b", "b"), ("d", "d"), ("e", "e"),
                         ("a", "a"), ("b", "b")])
        cycle2 = FairCycle(echo, ["c", "c"], MyEmpty)
        self.assertRaises(MyEmpty, consume, cycle2.get, 3)


