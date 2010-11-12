from funtests import transport


class test_amqplib(transport.TransportCase):
    transport = "amqplib"
    prefix = "amqplib"
