from __future__ import annotations

import pytest

import tests.skip

pytest.importorskip('boto3')
pytest.importorskip('pycurl')


@tests.skip.if_pypy
@pytest.mark.usefixtures('hub')
class AWSCase:
    pass
