from funtests import transport


class test_pyamqp(transport.TransportCase):
    transport = "pyamqp"
    prefix = "pyamqp"
