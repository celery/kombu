from __future__ import annotations

from queue import Empty
from unittest.mock import patch

import django
import pytest
from django.conf import settings
from django.core.management import call_command

from kombu import Connection

settings.configure(
    INSTALLED_APPS=("kombu.transport.django",),
    DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
)
django.setup()
call_command("migrate", database="default")

# Need to import after setting up Django
from kombu.transport.django.models import Binding  # noqa: E402
from kombu.transport.django.models import Exchange  # noqa: E402
from kombu.transport.django.models import Message, Queue  # noqa: E402


@pytest.fixture
def channel():
    conn = Connection("django:///")
    conn.connect()
    channel = conn.channel()

    yield channel

    channel._purge("celery")
    conn.release()


def test_url_parser():
    with patch("kombu.transport.django.transport.Channel._open"):
        url = "django:///"
        Connection(url).connect()


def test_simple_queueing(channel):
    channel._put("celery", "DATA_SIMPLE_QUEUEING")
    assert channel._get("celery") == "DATA_SIMPLE_QUEUEING"


def test_queueing_multiple(channel):
    channel._put("celery", "DATA_SIMPLE_QUEUEING")
    channel._put("celery", "DATA_SIMPLE_QUEUEING2")

    assert channel._get("celery") == "DATA_SIMPLE_QUEUEING"
    assert channel._get("celery") == "DATA_SIMPLE_QUEUEING2"


def test__get_queue_that_does_not_exist(channel):
    with pytest.raises(Empty):
        channel._get("queue_that_does_not_exist")


def test__get_queue_when_empty(channel):
    channel._put("celery", "MESSAGE")
    channel._get("celery")
    with pytest.raises(Empty):
        channel._get("celery")


def test__purge_queue_that_does_not_exist(channel):
    channel._purge("queue_that_does_not_exist")  # does not raise an Exception


def test_queue_name(channel):
    queue = Queue.objects.create(name="celery_queue")
    assert "celery_queue" in str(queue)


def test_message_name(channel):
    queue = Queue.objects.create(name="another_queue")
    message = Message.objects.create(message="message", queue=queue)
    assert "message" in str(message)


def test_get_table_exchange_does_not_exist(channel):
    assert [] == channel.get_table("celery")


def test_get_table_empty(channel):
    Exchange.objects.create(name="celery")
    assert [] == channel.get_table("celery")


def test_get_table_one_binding(channel):
    exchange = Exchange.objects.create(name="another_exchange")
    queue = Queue.objects.create(name="another_queue2")
    Binding.objects.create(
        queue=queue, exchange=exchange, routing_key="routing_key"
    )
    assert [("routing_key", "", "another_queue2")] == channel.get_table(
        "another_exchange"
    )


def test__put_fanout_exchange_does_not_exist(channel):
    channel._put_fanout(
        "exchange_that_does_not_exist", "message", "routing_key"
    )  # does not raise an exception


def test__put_fanout_calls__put(channel):
    exchange = Exchange.objects.create(name="another_exchange2")
    queue = Queue.objects.create(name="another_queue3")
    Binding.objects.create(
        queue=queue, exchange=exchange, routing_key="routing_key"
    )
    with patch.object(channel, "_put") as m:

        channel._put_fanout("another_exchange2", "HELLO", "routing_key")

    m.assert_called_once_with("another_queue3", "HELLO", priority=0)


def test_priority(channel):
    channel._put('priority', 'message1', priority=1)
    channel._put('priority', 'message2', priority=0)
    assert 'message2' == channel._get('priority')


def test_ttl(channel):
    channel._put('ttl', 'message', ttl=0)
    with pytest.raises(Empty):
        channel._get('ttl')
