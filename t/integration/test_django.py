from __future__ import annotations

import pytest

from .common import BasicFunctionality


@pytest.mark.env('django')
class test_DjangoBasicFunctionality(BasicFunctionality):
    pass

