# This module applies two patches to qpid.messaging that are required for
# correct operation. Each patch fixes a bug. See links to the bugs below:
# https://issues.apache.org/jira/browse/QPID-5637
# https://issues.apache.org/jira/browse/QPID-5557

# ## Begin Monkey Patch 1 ###
# https://issues.apache.org/jira/browse/QPID-5637

#############################################################################
#  _   _  ___ _____ _____
# | \ | |/ _ \_   _| ____|
# |  \| | | | || | |  _|
# | |\  | |_| || | | |___
# |_| \_|\___/ |_| |_____|
#
# If you have code that also uses qpid.messaging and imports kombu,
# or causes this file to be imported, then you need to make sure that this
# import occurs first.
#
# Failure to do this will cause the following exception:
# AttributeError: 'Selector' object has no attribute '_current_pid'
#
# Fix this by importing this module prior to using qpid.messaging in other
# code that also uses this module.
#############################################################################


# this import is needed for Python 2.6. Without it, qpid.py will "mask" the
# system's qpid lib
from __future__ import absolute_import, unicode_literals

import os


# Imports for Monkey Patch 1
try:
    from qpid.selector import Selector
except ImportError:  # pragma: no cover
    Selector = None     # noqa
import atexit


# Prepare for Monkey Patch 1
def default_monkey():  # pragma: no cover
    Selector.lock.acquire()
    try:
        if Selector.DEFAULT is None:
            sel = Selector()
            atexit.register(sel.stop)
            sel.start()
            Selector.DEFAULT = sel
            Selector._current_pid = os.getpid()
        elif Selector._current_pid != os.getpid():
            sel = Selector()
            atexit.register(sel.stop)
            sel.start()
            Selector.DEFAULT = sel
            Selector._current_pid = os.getpid()
        return Selector.DEFAULT
    finally:
        Selector.lock.release()

# Apply Monkey Patch 1

try:
    import qpid.selector
    qpid.selector.Selector.default = staticmethod(default_monkey)
except ImportError:  # pragma: no cover
    pass

# ## End Monkey Patch 1 ###

# ## Begin Monkey Patch 2 ###
# https://issues.apache.org/jira/browse/QPID-5557

# Imports for Monkey Patch 2
try:
    from qpid.ops import ExchangeQuery, QueueQuery
except ImportError:  # pragma: no cover
    ExchangeQuery = None
    QueueQuery = None

try:
    from qpid.messaging.exceptions import (
        NotFound, AssertionFailed, ConnectionError,
    )
except ImportError:  # pragma: no cover
    NotFound = None
    AssertionFailed = None
    ConnectionError = None


# Prepare for Monkey Patch 2
def resolve_declare_monkey(self, sst, lnk, dir, action):  # pragma: no cover
    declare = lnk.options.get('create') in ('always', dir)
    assrt = lnk.options.get('assert') in ('always', dir)
    requested_type = lnk.options.get('node', {}).get('type')

    def do_resolved(type, subtype):
        err = None
        if type is None:
            if declare:
                err = self.declare(sst, lnk, action)
            else:
                err = NotFound(text='no such queue: %s' % lnk.name)
        else:
            if assrt:
                expected = lnk.options.get('node', {}).get('type')
                if expected and type != expected:
                    err = AssertionFailed(
                        text='expected %s, got %s' % (expected, type))
            if err is None:
                action(type, subtype)
        if err:
            tgt = lnk.target
            tgt.error = err
            del self._attachments[tgt]
            tgt.closed = True
            return

    self.resolve(sst, lnk.name, do_resolved, node_type=requested_type,
                 force=declare)


def resolve_monkey(self, sst, name, action, force=False,
                   node_type=None):  # pragma: no cover
    if not force and not node_type:
        try:
            type, subtype = self.address_cache[name]
            action(type, subtype)
            return
        except KeyError:
            pass
    args = []

    def do_result(r):
        args.append(r)

    def do_action(r):
        do_result(r)
        er, qr = args
        if node_type == 'topic' and not er.not_found:
            type, subtype = 'topic', er.type
        elif node_type == 'queue' and qr.queue:
            type, subtype = 'queue', None
        elif er.not_found and not qr.queue:
            type, subtype = None, None
        elif qr.queue:
            type, subtype = 'queue', None
        else:
            type, subtype = 'topic', er.type
        if type is not None:
            self.address_cache[name] = (type, subtype)
        action(type, subtype)

    sst.write_query(ExchangeQuery(name), do_result)
    sst.write_query(QueueQuery(name), do_action)


# Apply monkey patch 2
try:
    import qpid.messaging.driver
    qpid.messaging.driver.Engine.resolve_declare = resolve_declare_monkey
    qpid.messaging.driver.Engine.resolve = resolve_monkey
except ImportError:  # pragma: no cover
    pass
# ## End Monkey Patch 2 ###
