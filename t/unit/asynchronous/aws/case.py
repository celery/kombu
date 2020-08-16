import pytest

import t.skip

pytest.importorskip('boto3')
pytest.importorskip('pycurl')


@t.skip.if_pypy
@pytest.mark.usefixtures('hub')
class AWSCase:
    pass
