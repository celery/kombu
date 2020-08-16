import pytest

from unittest.mock import Mock, patch

pytest.importorskip('librabbitmq')

from kombu.transport import librabbitmq  # noqa


class test_Message:

    def test_init(self):
        chan = Mock(name='channel')
        message = librabbitmq.Message(
            chan, {'prop': 42}, {'delivery_tag': 337}, 'body',
        )
        assert message.body == 'body'
        assert message.delivery_tag == 337
        assert message.properties['prop'] == 42


class test_Channel:

    def test_prepare_message(self):
        conn = Mock(name='connection')
        chan = librabbitmq.Channel(conn, 1)
        assert chan

        body = 'the quick brown fox...'
        properties = {'name': 'Elaine M.'}

        body2, props2 = chan.prepare_message(
            body, properties=properties,
            priority=999,
            content_type='ctype',
            content_encoding='cenc',
            headers={'H': 2},
        )

        assert props2['name'] == 'Elaine M.'
        assert props2['priority'] == 999
        assert props2['content_type'] == 'ctype'
        assert props2['content_encoding'] == 'cenc'
        assert props2['headers'] == {'H': 2}
        assert body2 == body

        body3, props3 = chan.prepare_message(body, priority=777)
        assert props3['priority'] == 777
        assert body3 == body


class test_Transport:

    def setup(self):
        self.client = Mock(name='client')
        self.T = librabbitmq.Transport(self.client)

    def test_driver_version(self):
        assert self.T.driver_version()

    def test_create_channel(self):
        conn = Mock(name='connection')
        chan = self.T.create_channel(conn)
        assert chan
        conn.channel.assert_called_with()

    def test_drain_events(self):
        conn = Mock(name='connection')
        self.T.drain_events(conn, timeout=1.33)
        conn.drain_events.assert_called_with(timeout=1.33)

    def test_establish_connection_SSL_not_supported(self):
        self.client.ssl = True
        with pytest.raises(NotImplementedError):
            self.T.establish_connection()

    def test_establish_connection(self):
        self.T.Connection = Mock(name='Connection')
        self.T.client.ssl = False
        self.T.client.port = None
        self.T.client.transport_options = {}

        conn = self.T.establish_connection()
        assert self.T.client.port == self.T.default_connection_params['port']
        assert conn.client == self.T.client
        assert self.T.client.drain_events == conn.drain_events

    def test_collect__no_conn(self):
        self.T.client.drain_events = 1234
        self.T._collect(None)
        assert self.client.drain_events is None
        assert self.T.client is None

    def test_collect__with_conn(self):
        self.T.client.drain_events = 1234
        conn = Mock(name='connection')
        chans = conn.channels = {1: Mock(name='chan1'), 2: Mock(name='chan2')}
        conn.callbacks = {'foo': Mock(name='cb1'), 'bar': Mock(name='cb2')}
        for i, chan in enumerate(conn.channels.values()):
            chan.connection = i

        with patch('os.close') as close:
            self.T._collect(conn)
            close.assert_called_with(conn.fileno())
        assert not conn.channels
        assert not conn.callbacks
        for chan in chans.values():
            assert chan.connection is None
        assert self.client.drain_events is None
        assert self.T.client is None

        with patch('os.close') as close:
            self.T.client = self.client
            close.side_effect = OSError()
            self.T._collect(conn)
            close.assert_called_with(conn.fileno())

    def test_collect__with_fileno_raising_value_error(self):
        conn = Mock(name='connection')
        conn.channels = {1: Mock(name='chan1'), 2: Mock(name='chan2')}
        with patch('os.close') as close:
            self.T.client = self.client
            conn.fileno.side_effect = ValueError("Socket not connected")
            self.T._collect(conn)
            close.assert_not_called()
        conn.fileno.assert_called_with()
        assert self.client.drain_events is None
        assert self.T.client is None

    def test_register_with_event_loop(self):
        conn = Mock(name='conn')
        loop = Mock(name='loop')
        self.T.register_with_event_loop(conn, loop)
        loop.add_reader.assert_called_with(
            conn.fileno(), self.T.on_readable, conn, loop,
        )

    def test_verify_connection(self):
        conn = Mock(name='connection')
        conn.connected = True
        assert self.T.verify_connection(conn)

    def test_close_connection(self):
        conn = Mock(name='connection')
        self.client.drain_events = 1234
        self.T.close_connection(conn)
        assert self.client.drain_events is None
        conn.close.assert_called_with()
