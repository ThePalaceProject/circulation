"""Helpers for validating and normalizing ISSNs.

An ISSN (International Standard Serial Number) identifies a serial
publication -- a magazine, a journal, a newspaper. It is eight characters
long: seven digits followed by a check character, which is either a digit or
the letter "X". It is conventionally written as two groups of four characters
separated by a hyphen, for example "0317-8471".

Because an ISSN is the identity of a periodical, a typo in one quietly splits
a magazine into two: one for the real ISSN and one for the mistyped one. So we
check the check character and refuse anything that doesn't add up, rather than
storing it and hoping for the best.
"""

from __future__ import annotations

import re

from palace.util.exceptions import PalaceValueError

# An ISSN, with or without its conventional hyphen, and with a check character
# that may be a digit or an "X" in either case. Only ASCII digits count: \d
# would also match digits from other scripts, which look like a normal ISSN
# but are a different string, so the same magazine would end up with two
# identifiers.
ISSN_RE = re.compile(r"^([0-9]{4})-?([0-9]{3})([0-9Xx])$")

# When calculating the check character, the first digit of the ISSN is
# multiplied by 8, the second by 7, and so on down to 2.
_CHECK_CHARACTER_WEIGHTS = (8, 7, 6, 5, 4, 3, 2)


class InvalidISSNError(PalaceValueError):
    """Raised when a string cannot be read as an ISSN."""


def _check_character(digits: str) -> str:
    """Work out the check character for the first seven digits of an ISSN.

    :param digits: The first seven digits of an ISSN.
    :return: The check character: a single digit, or "X" for a value of ten.
    """
    total = sum(
        int(digit) * weight
        for digit, weight in zip(digits, _CHECK_CHARACTER_WEIGHTS, strict=True)
    )
    remainder = total % 11
    if remainder == 0:
        return "0"
    check = 11 - remainder
    return "X" if check == 10 else str(check)


def normalize_issn(value: str) -> str:
    """Put an ISSN into the one form we store it in.

    Surrounding whitespace is ignored, the hyphen is optional on the way in
    and always present on the way out, and a lowercase "x" check character is
    uppercased. So "0317-8471", "03178471" and " 2434-561x " become
    "0317-8471", "0317-8471" and "2434-561X".

    :param value: A string that should contain an ISSN.
    :raises InvalidISSNError: If the string isn't shaped like an ISSN, or if
        its check character doesn't match the rest of the number.
    :return: The ISSN as "NNNN-NNNC", where C is an uppercase check character.
    """
    match = ISSN_RE.match(value.strip())
    if match is None:
        raise InvalidISSNError(f'"{value}" is not a valid ISSN.')

    first, second, check_character = match.groups()
    expected = _check_character(first + second)
    if check_character.upper() != expected:
        raise InvalidISSNError(
            f'"{value}" is not a valid ISSN. Its check character should be '
            f'"{expected}", not "{check_character.upper()}".'
        )

    return f"{first}-{second}{expected}"


def is_valid_issn(value: str) -> bool:
    """Is this string an ISSN that we can store?

    :param value: A string that should contain an ISSN.
    :return: True if `normalize_issn` would accept the string.
    """
    try:
        normalize_issn(value)
    except InvalidISSNError:
        return False
    return True
