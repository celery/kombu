"""AMQP Management API utilities."""

from __future__ import annotations

from typing import Any, Optional

from kombu import Connection

DEFAULT_RABBITMQ_PORT = 15672


def get_manager(
    client: Connection,
    hostname: str | None = None,
    port: int | None = None,
    userid: str | None = None,
    password: str | None = None
):
    """Get pyrabbit manager."""
    import pyrabbit
    opt = client.transport_options.get

    def get(name: str, val: Any, default: Any) -> Any:
        return (
            val if val is not None
            else opt('manager_%s' % name) or getattr(client, name, None) or default
        )

    host = get('hostname', hostname, 'localhost')
    port = get('port', port, DEFAULT_RABBITMQ_PORT)
    userid = get('userid', userid, 'guest')
    password = get('password', password, 'guest')
    return pyrabbit.Client(f'{host}:{port}', userid, password)
