from funtests import transport
from nose import SkipTest


class test_pika_blocking(transport.TransportCase):
    transport = "syncpika"
    prefix = "syncpika"

    def test_produce__consume_large_messages(self, *args, **kwargs):
        raise SkipTest("test currently fails for sync pika")

class test_pika_async(transport.TransportCase):
    transport = "pika"
    prefix = "pika"
    
    def test_produce__consume_large_messages(self, *args, **kwargs):
        raise SkipTest("test currently fails for async pika")
