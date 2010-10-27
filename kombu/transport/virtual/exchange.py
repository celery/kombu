"""
kombu.transport.virtual.exchange
================================

Implementations of the standard exchanges defined
by the AMQ protocol  (excluding the `headers` exchange).

:copyright: (c) 2009 - 2010 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
import re


class NotEquivalentError(Exception):
    """Entity declaration is not equivalent to the previous declaration."""
    pass


class ExchangeType(object):
    """Implements the specifics for an exchange type.

    :param channel: AMQ Channel

    """

    def __init__(self, channel):
        self.channel = channel

    def lookup(self, exchange, routing_key, default):
        """Lookup all queues matching `routing_key` in `exchange`.

        :returns: `default` if no queues matched.

        """
        raise NotImplementedError("subclass responsibility")

    def prepare_bind(self, queue, exchange, routing_key, arguments):
        """:returns: `(routing_key, regex, queue)` tuple to store
        for bindings to this exchange."""
        return routing_key, None, queue

    def equivalent(self, prev, exchange, type, durable, auto_delete,
            arguments):
        """Assert equivalence to previous declaration.

        :raises NotEquivalentError: If the declarations are not equivalent.

        """
        return (type == prev["type"] and
                durable == prev["durable"] and
                auto_delete == prev["auto_delete"] and
                arguments or {} == prev["arguments"] or {})


class DirectExchange(ExchangeType):
    """The `direct` exchange routes based on exact routing keys."""

    def lookup(self, table, exchange, routing_key, default):
        return [queue for rkey, _, queue in table
                    if rkey == routing_key] or [default]


class TopicExchange(ExchangeType):
    """The `topic` exchanges routes based on words separated by dots, and
    wildcard characters `*` (any single word), and `#` (one or more words)."""

    #: map of wildcard to regex conversions
    wildcards = {"*": r".*?[^\.]",
                 "#": r".*?"}

    #: compiled regex cache
    _compiled = {}

    def lookup(self, table, exchange, routing_key, default):
        return  [queue for rkey, pattern, queue in table
                            if self._match(pattern, routing_key)] or [default]

    def prepare_bind(self, queue, exchange, routing_key, arguments):
        return routing_key, self.key_to_pattern(routing_key), queue

    def key_to_pattern(self, rkey):
        """Get the corresponding regex for any routing key."""
        return "^%s$" % ("\.".join(self.wildcards.get(word, word)
                                        for word in rkey.split(".")))

    def _match(self, pattern, string):
        """Same as :func:`re.match`, except the regex is compiled and cached,
        then reused on subsequent matches with the same pattern."""
        try:
            compiled = self._compiled[pattern]
        except KeyError:
            compiled = self._compiled[pattern] = re.compile(pattern, re.u)
        return compiled.match(routing_key)


class FanoutExchange(ExchangeType):
    """The `fanout` exchange implements broadcast messaging by delivering
    copies of all messages to all queues bound the the exchange.

    To support fanout the virtual channel needs to store the table
    as shared state.  This requires that the `Channel.supports_fanout`
    attribute is set to true, and the `Channel._queue_bind` and
    `Channel.get_table` methods are implemented.  See the redis backend
    for an example implementation of these methods.

    """

    def lookup(self, table, exchange, routing_key, default):
        return [queue for _, _, queue in table]

    def prepare_bind(self, queue, exchange, routing_key, arguments):
        return routing_key, None, queue


#: Map of standard exchange types and corresponding classes.
STANDARD_EXCHANGE_TYPES = {"direct": DirectExchange,
                           "topic": TopicExchange,
                           "fanout": FanoutExchange}
