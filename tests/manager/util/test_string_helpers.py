# Test the helper objects in util.string.

import string
from collections.abc import Sequence

import pytest

from palace.manager.util.string_helpers import oxford_comma_join, random_key


def test_random_key():
    m = random_key
    assert "" == m(0)

    # The strings are random.
    res1 = m(8)
    res2 = m(8)
    assert res1 != res2

    # They match the length we asked for.
    assert len(m(40)) == 40

    # All characters are printable.
    for letter in m(40):
        assert letter in string.printable


@pytest.mark.parametrize(
    "items, conjunction, expected",
    [
        pytest.param([], "", "", id="none-empty"),
        pytest.param(["a"], "", "a", id="one-empty"),
        pytest.param(["a", "b"], "", "a, b", id="two-empty"),
        pytest.param(["a", "b", "c"], "", "a, b, c", id="three-empty"),
        pytest.param([], "or", "", id="none"),
        pytest.param(["a"], "or", "a", id="one"),
        pytest.param(["a", "b"], "or", "a or b", id="two"),
        pytest.param(["a", "b", "c"], "or", "a, b, or c", id="three"),
        pytest.param(["a", "b", "c", "d"], "or", "a, b, c, or d", id="four"),
        pytest.param(("a", "b", "c"), "and", "a, b, and c", id="tuple-and"),
    ],
)
def test_oxford_comma_join(
    items: Sequence[str], conjunction: str, expected: str
) -> None:
    assert oxford_comma_join(items, conjunction) == expected
