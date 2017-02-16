"""AMQP Management API utilities."""
from typing import Any, Union
from kombu.types import ClientT


def get_manager(client: ClientT,
                hostname: str = None,
                port: Union[int, str] = None,
                userid: str = None,
                password: str = None) -> Any:
    """Get pyrabbit manager."""
    import pyrabbit
    opt = client.transport_options.get

    def get(name, val, default):
        return (val if val is not None
                else opt('manager_%s' % name) or
                getattr(client, name, None) or default)

    host = get('hostname', hostname, 'localhost')
    port = port if port is not None else opt('manager_port', 15672)
    userid = get('userid', userid, 'guest')
    password = get('password', password, 'guest')
    return pyrabbit.Client('%s:%s' % (host, port), userid, password)
