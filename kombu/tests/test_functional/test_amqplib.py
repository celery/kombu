from kombu.tests.test_functional import transport


class test_amqplib(transport.TransportCase):
    transport = "amqplib"
    prefix = "amqplib"
