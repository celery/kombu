import pytest

from case import skip
import t.skip


@t.skip.if_pypy
@skip.unless_module('boto3')
@skip.unless_module('pycurl')
@pytest.mark.usefixtures('hub')
class AWSCase:
    pass
