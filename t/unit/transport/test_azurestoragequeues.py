from __future__ import annotations

from unittest.mock import MagicMock, patch

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


def test_queue_name_cache_is_not_shared_across_connections():
    # Account A already has an `orders` queue; account B does not. A
    # Connection to B must still create `orders` in B instead of trusting
    # A's queue listing and sending to a queue that doesn't exist there.
    url_a = 'azurestoragequeues://key@https://account-a.queue.core.windows.net/'
    url_b = 'azurestoragequeues://key@https://account-b.queue.core.windows.net/'
    services = {}

    def service_for(account_url, credential):
        service = services[account_url] = MagicMock(name=account_url)
        if 'account-a' in account_url:
            service.list_queues.return_value = [{'name': 'orders'}]
        else:
            service.list_queues.return_value = []
        return service

    with patch(
        'kombu.transport.azurestoragequeues.QueueServiceClient',
        side_effect=service_for,
    ):
        Connection(url_a, transport=azurestoragequeues.Transport).channel()
        channel_b = Connection(url_b, transport=azurestoragequeues.Transport).channel()
        channel_b._put('orders', {'body': 'hello'})

    service_b = services['https://account-b.queue.core.windows.net/']
    service_b.create_queue.assert_called_once_with('orders')
