"""AMQP Management API utilities."""

from __future__ import annotations

from typing import Optional, Any

from kombu import Connection

DEFAULT_RABBITMQ_PORT = 15672


def get_manager(client: Connection, hostname: Optional[str] = None,
                port: Optional[int] = None, userid: Optional[str] = None,
                password: Optional[str] = None):
    """Get pyrabbit manager."""
    import pyrabbit
    opt = client.transport_options.get

    def get(name: str, val: Any, default: Any):
        return (val if val is not None
                else opt('manager_%s' % name) or
                     getattr(client, name, None) or default)

    host = get('hostname', hostname, 'localhost')
    port = get('port', port, DEFAULT_RABBITMQ_PORT)
    userid = get('userid', userid, 'guest')
    password = get('password', password, 'guest')
    return pyrabbit.Client(f'{host}:{port}', userid, password)
