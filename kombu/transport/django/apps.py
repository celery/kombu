from __future__ import annotations

from django.apps import AppConfig


class KombuConfig(AppConfig):
    """Django app config."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "kombu.transport.django"
    label = "kombu"
