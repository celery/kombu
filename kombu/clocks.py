"""
kombu.clocks
============

Logical Clocks and Synchronization.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

from threading import Lock

__all__ = ["LamportClock"]


class LamportClock(object):
    """Lamport's logical clock.

    From Wikipedia:

    A Lamport logical clock is a monotonically incrementing software counter
    maintained in each process.  It follows some simple rules:

        * A process increments its counter before each event in that process;
        * When a process sends a message, it includes its counter value with
          the message;
        * On receiving a message, the receiver process sets its counter to be
          greater than the maximum of its own value and the received value
          before it considers the message received.

    Conceptually, this logical clock can be thought of as a clock that only
    has meaning in relation to messages moving between processes.  When a
    process receives a message, it resynchronizes its logical clock with
    the sender.

    .. seealso::

        * `Lamport timestamps`_

        * `Lamports distributed mutex`_

    .. _`Lamport Timestamps`: http://en.wikipedia.org/wiki/Lamport_timestamps
    .. _`Lamports distributed mutex`: http://bit.ly/p99ybE

    *Usage*

    When sending a message use :meth:`forward` to increment the clock,
    when receiving a message use :meth:`adjust` to sync with
    the time stamp of the incoming message.

    """
    #: The clocks current value.
    value = 0

    def __init__(self, initial_value=0):
        self.value = initial_value
        self.mutex = Lock()

    def adjust(self, other):
        with self.mutex:
            self.value = max(self.value, other) + 1

    def forward(self):
        with self.mutex:
            self.value += 1
            return self.value
