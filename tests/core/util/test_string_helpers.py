# Test the helper objects in util.string.

import string

from core.util.string_helpers import random_key


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
