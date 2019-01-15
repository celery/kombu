from t.integration import transport

from case import skip


@skip.unless_module('azure.servicebus')
class test_azureservicebus(transport.TransportCase):
    transport = 'azureservicebus'
    prefix = 'azureservicebus'
    message_size_limit = 32000
