from __future__ import annotations

import os

import pytest

import kombu


def get_connection():
    return kombu.Connection(
        f"proton://{os.environ.get('RABBITMQ_HOST', 'localhost')}:"
        f"{os.environ.get('RABBITMQ_5672_TCP', '5672')}",
    )


@pytest.fixture()
def connection():
    return get_connection()


@pytest.mark.env("proton")
def test_connection(connection):
    connection.connect()
    try:
        assert connection.connected
    finally:
        connection.close()