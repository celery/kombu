from __future__ import absolute_import

from kombu.async import hub as _hub
from kombu.async.hub import Hub, get_event_loop, set_event_loop, maybe_block

from kombu.tests.case import Case, ContextMock, Mock


class test_Utils(Case):

    def setUp(self):
        self._prev_loop = get_event_loop()

    def tearDown(self):
        set_event_loop(self._prev_loop)

    def test_get_set_event_loop(self):
        set_event_loop(None)
        self.assertIsNone(_hub._current_loop)
        self.assertIsNone(get_event_loop())
        hub = Hub()
        set_event_loop(hub)
        self.assertIs(_hub._current_loop, hub)
        self.assertIs(get_event_loop(), hub)

    def test_maybe_block_without_loop(self):
        set_event_loop(None)
        with maybe_block():
            pass

    def test_maybe_block_with_loop(self):
        hub = ContextMock(name='hub')
        set_event_loop(hub)
        with maybe_block():
            pass
        hub.maybe_block.assert_called_with()


class test_Hub(Case):

    def setUp(self):
        self.hub = Hub()

    def tearDown(self):
        self.hub.close()

    def test_maybe_block(self):
        with self.hub.maybe_block():
            self.assertTrue(self.hub.in_blocking_section)
        self.assertFalse(self.hub.in_blocking_section)

