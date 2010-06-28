from kombu.backends import get_backend_cls


class BrokerConnection(object):
    port = None

    def __init__(self, hostname="localhost", userid="guest",
            password="guest", virtual_host="/", port=None, **kwargs):
        self.hostname = hostname
        self.userid = userid
        self.password = password
        self.virtual_host = virtual_host or self.virtual_host
        self.port = port or self.port
        self.insist = kwargs.get("insist", False)
        self.connect_timeout = kwargs.get("connect_timeout", 5)
        self.ssl = kwargs.get("ssl", False)
        self.backend_cls = kwargs.get("backend_cls", None)
        self._closed = None
        self._connection = None
        self._backend = None

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_trace):
        self.close()

    def _establish_connection(self):
        return self.backend.establish_connection()

    @property
    def connection(self):
        if self._closed:
            return
        if not self._connection:
            self._connection = self._establish_connection()
            self._closed = False
        return self._connection

    @property
    def host(self):
        """The host as a hostname/port pair separated by colon."""
        return ":".join([self.hostname, str(self.port)])

    def get_backend_cls(self):
        """Get the currently used backend class."""
        backend_cls = self.backend_cls
        if not backend_cls or isinstance(backend_cls, basestring):
            backend_cls = get_backend_cls(backend_cls)
        return backend_cls

    def connect(self):
        """Establish a connection to the AMQP server."""
        self._closed = False
        return self.connection

    def channel(self):
        """Request a new AMQP channel."""
        return self.backend.create_channel(self.connection)

    def drain_events(self, **kwargs):
        return self.backend.drain_events(self.connection, **kwargs)

    def close(self):
        """Close the currently open connection."""
        try:
            if self._connection:
                self.backend.close_connection(self._connection)
        except socket.error:
            pass
        self._closed = True

    def _create_backend(self):
        return self.get_backend_cls()(connection=self)

    @property
    def backend(self):
        if self._backend is None:
            self._backend = self._create_backend()
        return self._backend
