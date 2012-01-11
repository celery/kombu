from funtests import transport
from nose import SkipTest

from kombu.exceptions import VersionMismatch


class test_pika_blocking(transport.TransportCase):
    transport = "syncpika"
    prefix = "syncpika"

    def before_connect(self):
        try:
            from kombu.transport import pika
        except VersionMismatch:
            raise SkipTest("Pika version mismatch")

    def test_produce__consume_large_messages(self, *args, **kwargs):
        raise SkipTest("test currently fails for sync pika")

    def test_cyclic_reference_channel(self, *args, **kwargs):
        raise SkipTest("known memory leak")


class test_pika_async(transport.TransportCase):
    transport = "pika"
    prefix = "pika"

    def before_connect(self):
        try:
            from kombu.transport import pika
        except VersionMismatch:
            raise SkipTest("Pika version mismatch")

    def test_produce__consume_large_messages(self, *args, **kwargs):
        raise SkipTest("test currently fails for async pika")

    def test_cyclic_reference_channel(self, *args, **kwargs):
        raise SkipTest("known memory leak")
