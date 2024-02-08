# Test the helper objects in util.string.

import re
import string

from core.util.string_helpers import random_key, random_string


def test_random_string():
    m = random_string
    assert "" == m(0)

    # The strings are random.
    res1 = m(8)
    res2 = m(8)
    assert res1 != res2

    # We can't test exact values, because the randomness comes
    # from /dev/urandom, but we can test some of their properties:
    for size in range(1, 16):
        x = m(size)

        # The strings are Unicode strings, not bytestrings
        assert isinstance(x, str)

        # The strings are entirely composed of lowercase hex digits.
        assert None == re.compile("[^a-f0-9]").search(x)

        # Each byte is represented as two digits, so the length of the
        # string is twice the length passed in to the function.
        assert size * 2 == len(x)


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
