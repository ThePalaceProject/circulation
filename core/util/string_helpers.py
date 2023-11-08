# Helper functions and objects regarding strings -- especially stuff
# that lets us negotiate the distinction between Unicode and
# bytestrings.
import binascii
import os


def random_string(size: int) -> str:
    """Generate a random string of binary, encoded as hex digits.

    :param: Size of binary string in bytes.
    :return: A Unicode string.
    """
    return binascii.hexlify(os.urandom(size)).decode("utf8")
