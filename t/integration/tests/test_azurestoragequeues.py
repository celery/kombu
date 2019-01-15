from t.integration import transport

from case import skip


@skip.unless_module('azure.storage.queue')
class test_azurestoragequeues(transport.TransportCase):
    transport = 'azurestoragequeues'
    prefix = 'azurestoragequeues'
    event_loop_max = 100
    message_size_limit = 32000
    reliable_purge = False
    #: does not guarantee FIFO order, even in simple cases.
    suppress_disorder_warning = True
