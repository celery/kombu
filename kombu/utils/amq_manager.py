from __future__ import absolute_import


def get_manager(client, hostname=None, port=None, userid=None,
            password=None):
    import pyrabbit
    opt = client.transport_options.get
    host = (hostname if hostname is not None
                else opt('manager_hostname', client.hostname or 'localhost'))
    port = port if port is not None else opt('manager_port', 55672)
    return pyrabbit.Client('%s:%s' % (host, port),
        userid if userid is not None
                else opt('manager_userid', client.userid or 'guest'),
        password if password is not None
                else opt('manager_password', client.password or 'guest'))
