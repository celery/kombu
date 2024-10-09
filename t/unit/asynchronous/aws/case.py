from __future__ import annotations

import pytest

import t.skip

pytest.importorskip('boto3')
pytest.importorskip('urllib3')


@t.skip.if_pypy
@pytest.mark.usefixtures('hub')
class AWSCase:
    pass
