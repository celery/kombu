"""File-system Transport module for kombu.

Transport using the file-system as the message store. Messages written to the
queue are stored in `data_folder_in` directory and
messages read from the queue are read from `data_folder_out` directory. Both
directories must be created manually. Simple example:

* Producer:

.. code-block:: python

    import kombu

    conn = kombu.Connection(
        'filesystem://', transport_options={
            'data_folder_in': 'data_in', 'data_folder_out': 'data_out'
        }
    )
    conn.connect()

    test_queue = kombu.Queue('test', routing_key='test')

    with conn as conn:
        with conn.default_channel as channel:
            producer = kombu.Producer(channel)
            producer.publish(
                        {'hello': 'world'},
                        retry=True,
                        exchange=test_queue.exchange,
                        routing_key=test_queue.routing_key,
                        declare=[test_queue],
                        serializer='pickle'
            )

* Consumer:

.. code-block:: python

    import kombu

    conn = kombu.Connection(
        'filesystem://', transport_options={
            'data_folder_in': 'data_out', 'data_folder_out': 'data_in'
        }
    )
    conn.connect()

    def callback(body, message):
        print(body, message)
        message.ack()

    test_queue = kombu.Queue('test', routing_key='test')

    with conn as conn:
        with conn.default_channel as channel:
            consumer = kombu.Consumer(
                conn, [test_queue], accept=['pickle']
            )
            consumer.register_callback(callback)
            with consumer:
                conn.drain_events(timeout=1)

Features
========
* Type: Virtual
* Supports Direct: Yes
* Supports Topic: Yes
* Supports Fanout: Yes
* Supports Priority: No
* Supports TTL: No

Connection String
=================
Connection string is in the following format:

.. code-block::

    filesystem://

Transport Options
=================
* ``data_folder_in`` - directory where are messages stored when written
  to queue.
* ``data_folder_out`` - directory from which are messages read when read from
  queue.
* ``store_processed`` - if set to True, all processed messages are backed up to
  ``processed_folder``.
* ``processed_folder`` - directory where are backed up processed files.
"""

import os
import shutil
import tempfile
import uuid
from queue import Empty
from time import monotonic
from collections import namedtuple

from kombu.exceptions import ChannelError
from kombu.utils.encoding import bytes_to_str, str_to_bytes
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property

from . import virtual

VERSION = (1, 0, 0)
__version__ = '.'.join(map(str, VERSION))

# needs win32all to work on Windows
if os.name == 'nt':

    import pywintypes
    import win32con
    import win32file

    LOCK_EX = win32con.LOCKFILE_EXCLUSIVE_LOCK
    # 0 is the default
    LOCK_SH = 0
    LOCK_NB = win32con.LOCKFILE_FAIL_IMMEDIATELY
    __overlapped = pywintypes.OVERLAPPED()

    def lock(file, flags):
        """Create file lock."""
        hfile = win32file._get_osfhandle(file.fileno())
        win32file.LockFileEx(hfile, flags, 0, 0xffff0000, __overlapped)

    def unlock(file):
        """Remove file lock."""
        hfile = win32file._get_osfhandle(file.fileno())
        win32file.UnlockFileEx(hfile, 0, 0xffff0000, __overlapped)

elif os.name == 'posix':

    import fcntl
    from fcntl import LOCK_EX, LOCK_NB, LOCK_SH  # noqa

    def lock(file, flags):
        """Create file lock."""
        fcntl.flock(file.fileno(), flags)

    def unlock(file):
        """Remove file lock."""
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)
else:
    raise RuntimeError(
        'Filesystem plugin only defined for NT and POSIX platforms')


class Channel(virtual.Channel):
    """Filesystem Channel."""

    supports_fanout = True

    def get_table(self, exchange):
        filename = '{}.exchange'.format(exchange)
        filename =  os.path.join(self.control_folder,filename)
        try:
            f = open(filename,'r')
            exchange_table = loads(bytes_to_str(f.read()))
            result =  [tuple(q) for q in exchange_table]
            return result
        except FileNotFoundError:
            return []
        except OSError:
            raise ChannelError(
                f'Cannot open {filename}')
        finally:
            f.close()

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        filename = '{}.exchange'.format(exchange)
        filename = os.path.join(self.control_folder,filename)
        try:
            d_fileno = os.open(self.control_folder,os.O_RDONLY)
            d = namedtuple('Directory',[])
            d.fileno = lambda: d_fileno
            
            lock(d,LOCK_EX)
            if os.path.isfile(filename):
                f = open(filename,'r')
                exchange_table = loads(bytes_to_str(f.read()))
                f.close()
            else:
                exchange_table = []
            queues = [tuple(q) for q in exchange_table]
            queue_val = (routing_key or '', pattern or '', queue or '' )
            if (queue_val not in queues):
                exchange_table = [queue_val] +  queues
            f = open(filename,'wb')
            f.write(str_to_bytes(dumps(exchange_table)))
        except OSError:
            raise ChannelError(
                f'Cannot open {filename!r}')
        finally:
            unlock(d)
            if (f):
                f.close()

    def _put_fanout(self, exchange, payload, routing_key, **kwargs):
        for q in self.get_table(exchange):
            self._put(q[2], payload, **kwargs)

    def _put(self, queue, payload, **kwargs):
        """Put `message` onto `queue`."""
        queue_folder = os.path.join(self.data_folder_out, queue)
        filename = '{}_{}.msg'.format(int(round(monotonic() * 1000)),uuid.uuid4())
        filename = os.path.join(queue_folder, filename)
        f = None # define file descriptor
        try:
            os.makedirs(queue_folder, exist_ok = True)
            f = open(filename, 'wb')
            lock(f, LOCK_EX)
            f.write(str_to_bytes(dumps(payload)))
        except OSError:
            raise ChannelError(f'Cannot create {filename}')
        finally:
            if (f):
                unlock(f)
                f.close()

    def _get(self, queue):
        """Get next message from `queue`."""
        queue_folder = os.path.join(self.data_folder_in, queue)
        if (not os.path.exists(queue_folder)):
            raise Empty()

        messages = os.listdir(queue_folder)
        messages = sorted(messages)
        while len(messages) > 0:
            filename = messages.pop(0)
            if self.store_processed:
                processed_folder = os.path.join(self.processed_folder, queue)
                os.makedirs(processed_folder, exist_ok = True)
            else:
                processed_folder = tempfile.gettempdir()

            try:
                # move the file to the tmp/processed folder
                shutil.move(os.path.join(queue_folder, filename),
                            processed_folder)
            except OSError:
                pass  # file could be locked, or removed in meantime so ignore

            filename = os.path.join(processed_folder, filename)
            try:
                f = open(filename, 'rb')
                payload = f.read()
                f.close()
                if not self.store_processed:
                    os.remove(filename)
            except OSError:
                raise ChannelError(
                    f'Cannot read file {filename!r} from queue.')

            return loads(bytes_to_str(payload))

        raise Empty()

    def _purge(self, queue):
        """Remove all messages from `queue`."""
        queue_folder = os.path.join(self.data_folder_out, queue)

        try:
            count = len(os.listdir(queue_folder))
            shutil.rmtree(queue_folder)
            return count
        except OSError:
            # we simply ignore its existence, as it was probably
            # processed by another worker
            return 0
        
    def _size(self, queue):
        """Return the number of messages in `queue` as an :class:`int`."""
        try:
            return len(os.listdir(self.data_folder_in))
        except OSError:
            return 0


    @property
    def transport_options(self):
        return self.connection.client.transport_options

    @cached_property
    def data_folder_in(self):
        return self.transport_options.get('data_folder_in', 'data_in')

    @cached_property
    def data_folder_out(self):
        return self.transport_options.get('data_folder_out', 'data_out')

    @cached_property
    def store_processed(self):
        return self.transport_options.get('store_processed', False)

    @cached_property
    def processed_folder(self):
        return self.transport_options.get('processed_folder', 'processed')

    @cached_property
    def control_folder(self):
        return self.transport_options.get('control_folder', 'control')



class Transport(virtual.Transport):
    """Filesystem Transport."""

    implements = virtual.Transport.implements.extend(
        asynchronous=False,
        exchange_type=frozenset(['direct', 'topic', 'fanout'])
    )


    Channel = Channel
    # filesystem backend state is global.
    global_state = virtual.BrokerState()
    default_port = 0
    driver_type = 'filesystem'
    driver_name = 'filesystem'

    def __init__(self, client, **kwargs):
        super().__init__(client, **kwargs)
        self.state = self.global_state

    def driver_version(self):
        return 'N/A'
