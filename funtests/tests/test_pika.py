from funtests import transport


class test_pika_blocking(transport.TransportCase):
    transport = "syncpika"
    prefix = "syncpika"


class test_pika_async(transport.TransportCase):
    transport = "pika"
    prefix = "pika"
