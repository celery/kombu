from __future__ import annotations

import json
import uuid

from tembo_pgmq_python import PGMQueue
from tembo_pgmq_python import __version__ as pgmq_version

from kombu.exceptions import InconsistencyError
from kombu.transport import virtual
from kombu.utils.encoding import bytes_to_str, str_to_bytes
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property
from kombu.utils.url import _parse_url

DEFAULT_PORT = 5432
DEFAULT_HOST = 'localhost'
DEFAULT_USER = 'postgres'
DEFAULT_PASSWORD = 'postgres'
DEFAULT_DBNAME = 'postgres'
DEFAULT_TIMEOUT = 10.0

DEFAULT_RETRY_POLICY = {
    'interval_start': 0.1, 'interval_step': 0.1,
    'interval_max': 0.2, 'max_retries': 3,
}

def gen_unique_id():
    return uuid.uuid4().hex

class PGMQChannel(virtual.Channel):
    _client = None
    supports_fanout = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue_path = {}
        self._queue_cache = {}
        self._pgmq = None
        self._init_client()

    def _init_client(self):
        if self._pgmq is None:
            self._pgmq = PGMQueue(
                host=self.connection.client.transport_options.get('host', DEFAULT_HOST),
                user=self.connection.client.transport_options.get('user', DEFAULT_USER),
                password=self.connection.client.transport_options.get('password', DEFAULT_PASSWORD),
                database=self.connection.client.transport_options.get('dbname', DEFAULT_DBNAME),
                port=self.connection.client.transport_options.get('port', DEFAULT_PORT)
            )
            self._client = self._pgmq

    def _new_queue(self, queue, **kwargs):
        self._init_client()
        self._client.create_queue(queue)

    def _delete(self, queue, *args, **kwargs):
        self._init_client()
        # Implement delete queue if needed

    def _put(self, queue, message, **kwargs):
        self._init_client()
        self._client.create_queue(queue)
        message_id = gen_unique_id()
        message['id'] = message_id
        self._client.send(queue, {"id": message_id, "body": json.dumps(message)})

    def _get(self, queue):
        self._init_client()
        try:
            msg = self._client.pop(queue)
            if not msg:
                raise self.Empty()
            return json.loads(msg.message['body'])
        except Exception as exc:
            raise InconsistencyError(f"Error retrieving queue {queue}: {exc}")

    def _size(self, queue):
        self._init_client()
        try:
            messages = self._client.read(queue)
            return len(messages)
        except Exception:
            return 0

    def _purge(self, queue):
        self._init_client()
        try:
            messages = self._client.read(queue)
            for msg in messages:
                self._client.delete(queue, msg.msg_id)
            return len(messages)
        except Exception:
            return 0

class PGMQTransport(virtual.Transport):
    Channel = PGMQChannel

    driver_type = 'pgmq'
    driver_name = 'pgmq'

    default_port = DEFAULT_PORT

    def driver_version(self):
        return pgmq_version
