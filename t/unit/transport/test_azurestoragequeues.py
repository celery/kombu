from __future__ import annotations

from unittest.mock import patch

import pytest

from kombu import Connection

pytest.importorskip('azure.storage.queue')
from kombu.transport import azurestoragequeues  # noqa

URL_NOCREDS = 'azurestoragequeues://'
URL_CREDS = 'azurestoragequeues://sas/key%@https://STORAGE_ACCOUNT_NAME.queue.core.windows.net/' # noqa


def test_queue_service_nocredentials():
    conn = Connection(URL_NOCREDS, transport=azurestoragequeues.Transport)
    with pytest.raises(
        ValueError,
        match='Need a URI like azurestoragequeues://{SAS or access key}@{URL}'
    ):
        conn.channel()


def test_queue_service():
    # Test gettings queue service without credentials
    conn = Connection(URL_CREDS, transport=azurestoragequeues.Transport)
    with patch('kombu.transport.azurestoragequeues.QueueServiceClient'):
        channel = conn.channel()

        # Check the SAS token "sas/key%" has been parsed from the url correctly
        assert channel._credential == 'sas/key%'
        assert channel._url == 'https://STORAGE_ACCOUNT_NAME.queue.core.windows.net/' # noqa
