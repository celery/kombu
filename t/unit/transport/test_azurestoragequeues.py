from __future__ import annotations

from unittest.mock import patch

import pytest
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

from kombu import Connection

pytest.importorskip('azure.storage.queue')
from kombu.transport import azurestoragequeues  # noqa

URL_NOCREDS = 'azurestoragequeues://'
URL_CREDS = 'azurestoragequeues://sas/key%@https://STORAGE_ACCOUNT_NAME.queue.core.windows.net/' # noqa
AZURITE_CREDS = 'azurestoragequeues://Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==@http://localhost:10001/devstoreaccount1'  # noqa
AZURITE_CREDS_DOCKER_COMPOSE = 'azurestoragequeues://Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==@http://azurite:10001/devstoreaccount1'  # noqa
DEFAULT_AZURE_URL_CREDS = 'azurestoragequeues://DefaultAzureCredential@https://STORAGE_ACCOUNT_NAME.queue.core.windows.net/' # noqa
MANAGED_IDENTITY_URL_CREDS = 'azurestoragequeues://ManagedIdentityCredential@https://STORAGE_ACCOUNT_NAME.queue.core.windows.net/' # noqa


def test_queue_service_nocredentials():
    conn = Connection(URL_NOCREDS, transport=azurestoragequeues.Transport)
    with pytest.raises(
        ValueError,
        match='Need a URI like azurestoragequeues://{SAS or access key}@{URL}'
    ):
        conn.channel()


def test_queue_service():
    # Test getting queue service without credentials
    conn = Connection(URL_CREDS, transport=azurestoragequeues.Transport)
    with patch('kombu.transport.azurestoragequeues.QueueServiceClient'):
        channel = conn.channel()

        # Check the SAS token "sas/key%" has been parsed from the url correctly
        assert channel._credential == 'sas/key%'
        assert channel._url == 'https://STORAGE_ACCOUNT_NAME.queue.core.windows.net/' # noqa


@pytest.mark.parametrize(
    "creds, hostname",
    [
        (AZURITE_CREDS, 'localhost'),
        (AZURITE_CREDS_DOCKER_COMPOSE, 'azurite'),
    ]
)
def test_queue_service_works_for_azurite(creds, hostname):
    conn = Connection(creds, transport=azurestoragequeues.Transport)
    with patch('kombu.transport.azurestoragequeues.QueueServiceClient'):
        channel = conn.channel()

        assert channel._credential == {
            'account_name': 'devstoreaccount1',
            'account_key': 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='  # noqa
        }
        assert channel._url == f'http://{hostname}:10001/devstoreaccount1' # noqa


def test_queue_service_works_for_default_azure_credentials():
    conn = Connection(
        DEFAULT_AZURE_URL_CREDS, transport=azurestoragequeues.Transport
    )
    with patch("kombu.transport.azurestoragequeues.QueueServiceClient"):
        channel = conn.channel()

        assert isinstance(channel._credential, DefaultAzureCredential)
        assert (
            channel._url
            == "https://STORAGE_ACCOUNT_NAME.queue.core.windows.net/"
        )


def test_queue_service_works_for_managed_identity_credentials():
    conn = Connection(
        MANAGED_IDENTITY_URL_CREDS, transport=azurestoragequeues.Transport
    )
    with patch("kombu.transport.azurestoragequeues.QueueServiceClient"):
        channel = conn.channel()

        assert isinstance(channel._credential, ManagedIdentityCredential)
        assert (
            channel._url
            == "https://STORAGE_ACCOUNT_NAME.queue.core.windows.net/"
        )
