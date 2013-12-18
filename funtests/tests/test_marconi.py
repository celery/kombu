from funtests import transport


class test_marconi(transport.TransportCase):
    transport = 'marconi'
    prefix = 'marconi'

    message_size_limit = 256 * 1024 // 2
