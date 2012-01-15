from __future__ import absolute_import

from ..clocks import LamportClock
from .utils import TestCase


class test_LamportClock(TestCase):

    def test_clocks(self):
        c1 = LamportClock()
        c2 = LamportClock()

        c1.forward()
        c2.forward()
        c1.forward()
        c1.forward()
        c2.adjust(c1.value)
        self.assertEqual(c2.value, c1.value + 1)

        c2_val = c2.value
        c2.forward()
        c2.forward()
        c2.adjust(c1.value)
        self.assertEqual(c2.value, c2_val + 2 + 1)

        c1.adjust(c2.value)
        self.assertEqual(c1.value, c2.value + 1)
