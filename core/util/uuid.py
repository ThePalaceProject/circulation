from base64 import urlsafe_b64decode
from uuid import UUID

from core.util.base64 import urlsafe_b64encode


def uuid_encode(uuid: UUID) -> str:
    """
    Encode a UUID to a URL-safe base64 string with = padding removed,
    provides a compact representation of the UUID to use in URLs.
    """
    encoded = urlsafe_b64encode(uuid.bytes)
    unpadded = encoded.rstrip("=")
    return unpadded


def uuid_decode(encoded: str) -> UUID:
    """
    Decode a URL-safe base64 string to a UUID. Reverse of uuid_encode.
    """
    if len(encoded) == 22:
        # This looks like an encoded UUID, so add padding and try to decode it
        padding = "=="
        decoded_bytes = urlsafe_b64decode(encoded + padding)
        return UUID(bytes=decoded_bytes)

    # See if this is a normal UUID hex string
    encoded = encoded.replace("urn:", "").replace("uuid:", "")
    encoded = encoded.strip("{}").replace("-", "")
    if len(encoded) == 32:
        return UUID(hex=encoded)

    raise ValueError("Invalid string for UUID")
