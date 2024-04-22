# Helper functions and objects regarding strings -- especially stuff
# that lets us negotiate the distinction between Unicode and
# bytestrings.
import secrets
import string


def random_key(size: int) -> str:
    """Generate a random string suitable for use as a key.

    :param: Size of the key to generate.
    :return: A Unicode string.
    """
    return "".join(secrets.choice(string.printable) for i in range(size))
