# Helper functions and objects regarding strings -- especially stuff
# that lets us negotiate the distinction between Unicode and
# bytestrings.
import secrets
import string
from collections.abc import Sequence


def random_key(size: int) -> str:
    """Generate a random string suitable for use as a key.

    :param: Size of the key to generate.
    :return: A Unicode string.
    """
    return "".join(secrets.choice(string.printable) for i in range(size))


def oxford_comma_join(items: Sequence[str], conjunction: str) -> str:
    """Join items into a list with an Oxford comma.

    The conjunction goes only between the last two items. For example, with
    "or", ``["a", "b"]`` becomes "a or b" and ``["a", "b", "c"]`` becomes
    "a, b, or c". With an empty conjunction, the items are only separated by commas.

    :param items: The items to join.
    :param conjunction: The word placed before the last item, or an empty string
        for none.
    :return: The joined items, or an empty string if there are none.
    """
    if not conjunction:
        return ", ".join(items)
    if len(items) <= 2:
        return f" {conjunction} ".join(items)
    return f"{', '.join(items[:-1])}, {conjunction} {items[-1]}"
