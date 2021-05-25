import pytest

from kombu.utils.time import maybe_s_to_ms


@pytest.mark.parametrize('input,expected', [
    (3, 3000),
    (3.0, 3000),
    (303, 303000),
    (303.33, 303330),
    (303.333, 303333),
    (303.3334, 303333),
    (None, None),
    (0, 0),
])
def test_maybe_s_to_ms(input, expected):
    ret = maybe_s_to_ms(input)
    if expected is None:
        assert ret is None
    else:
        assert ret == expected
