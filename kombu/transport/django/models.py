from __future__ import annotations

from datetime import timedelta

from django.db import models
from django.utils import timezone


class Queue(models.Model):
    """The queue."""

    name = models.CharField(max_length=200, unique=True)

    def __str__(self):
        return self.name


class Message(models.Model):
    """The message."""

    visible = models.BooleanField(default=True, db_index=True)
    sent_at = models.DateTimeField(null=True, db_index=True, auto_now_add=True)
    message = models.TextField()
    version = models.PositiveIntegerField(default=1)
    priority = models.PositiveIntegerField(default=0)
    ttl = models.IntegerField(
        null=True, blank=True
    )  # TTL in seconds (null means no TTL)
    queue = models.ForeignKey(Queue, on_delete=models.CASCADE, related_name="messages")

    def __str__(self):
        return f"{self.sent_at} {self.message} {self.queue_id}"

    def is_expired(self):
        if self.ttl is None:
            return False  # No TTL set, so not expired
        expiration_time = self.sent_at + timedelta(seconds=self.ttl)
        return expiration_time < timezone.now()


class Exchange(models.Model):
    """The exchange."""

    name = models.CharField(max_length=200, unique=True)

    def __str__(self):
        return f"{self.name}"


class Binding(models.Model):
    """The binding."""

    queue = models.ForeignKey(Queue, on_delete=models.CASCADE, related_name="bindings")
    exchange = models.ForeignKey(
        Exchange, on_delete=models.CASCADE, related_name="bindings"
    )
    routing_key = models.CharField(max_length=255, null=True)

    def __str__(self):
        return f"Binding: {self.queue.name} -> {self.exchange.name} with routing_key {self.routing_key}"
