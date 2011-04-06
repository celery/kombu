from funtests import transport


class test_beanstalk(transport.TransportCase):
    transport = "beanstalk"
    prefix = "beanstalk"
    event_loop_max = 10
    message_size_limit = 47662

    def after_connect(self, connection):
        connection.channel().client
