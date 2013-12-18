import string

import marconiclient
from marconiclient.queues import client

from kombu.five import text_t, Empty
from kombu.transport import virtual
from kombu.utils.encoding import safe_str



# dots are replaced by dash, all other punctuation replaced by underscore.
CHARS_REPLACE_TABLE = dict(
    (ord(c), ord("_"))
    for c in string.punctuation if c not in "-_."
)
CHARS_REPLACE_TABLE[ord(".")] = ord("-")


class Channel(virtual.Channel):
    _client = None

    @property
    def client(self):
        if self._client is None:
            self._client = self.connect()
        return self._client

    def connect(self):
        opts = self.connection.client.transport_options
        return client.Client(
            opts.get("uri", "http://localhost:8888/v1"),
            version=opts.get("version", 1)
        )

    def close(self):
        super(Channel, self).close()
        self._client = None

    def _mangle_queue(self, q):
        return text_t(safe_str(q)).translate(CHARS_REPLACE_TABLE)

    def _put(self, queue, payload, **kwargs):
        self.client.queue(self._mangle_queue(queue)).post({
            "body": payload,
            # TODO: ...
            "ttl": 1209600,
        })

    def _get(self, queue):
        # TODO: Use a claim here once marconiclient supports them, otherwise
        # there is a race condition with concurrent ``_get()`` calls.
        msgs = self.client.queue(self._mangle_queue(queue)).messages(
            limit=1, echo=True
        )
        if not msgs:
            raise Empty()
        msg = next(msgs)
        msg.delete()
        return msg.body

    def _purge(self, queue):
        queue = self.client.queue(self._mangle_queue(queue))
        n_messages = queue.stats['messages']['total']
        queue.delete()
        return n_messages


class Transport(virtual.Transport):
    Channel = Channel

    driver_type = "marconi"
    driver_name = "marconi"

    def driver_version(self):
        return marconiclient.__version__
