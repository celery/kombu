from case import Mock, patch

from kombu import transport


class test_supports_librabbitmq:

    def test_eventlet(self):
        with patch('kombu.transport._detect_environment') as de:
            de.return_value = 'eventlet'
            assert not transport.supports_librabbitmq()


class test_transport:

    def test_resolve_transport(self):
        from kombu.transport.memory import Transport
        assert transport.resolve_transport(
            'kombu.transport.memory:Transport') is Transport
        assert transport.resolve_transport(Transport) is Transport

    def test_resolve_transport_alias_callable(self):
        m = transport.TRANSPORT_ALIASES['George'] = Mock(name='lazyalias')
        try:
            transport.resolve_transport('George')
            m.assert_called_with()
        finally:
            transport.TRANSPORT_ALIASES.pop('George')

    def test_resolve_transport_alias(self):
        assert transport.resolve_transport('pyamqp')
