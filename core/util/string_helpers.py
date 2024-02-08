# Helper functions and objects regarding strings -- especially stuff
# that lets us negotiate the distinction between Unicode and
# bytestrings.
import binascii
import os
import secrets
import string


def random_string(size: int) -> str:
    """Generate a random string of binary, encoded as hex digits.

    :param: Size of binary string in bytes.
    :return: A Unicode string.
    """
    return binascii.hexlify(os.urandom(size)).decode("utf8")


def random_key(size: int) -> str:
    """Generate a random string suitable for use as a key.

    :param: Size of the key to generate.
    :return: A Unicode string.
    """
    return "".join(secrets.choice(string.printable) for i in range(size))
