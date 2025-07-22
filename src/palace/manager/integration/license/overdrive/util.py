from urllib.parse import quote, urlsplit, urlunsplit


def _make_link_safe(url: str) -> str:
    """Turn a server-provided link into a link the server will accept!

    The {} part is completely obnoxious and I have complained about it to
    Overdrive.

    The availability part is to make sure we always use v2 of the
    availability API, even if Overdrive sent us a link to v1.
    """
    parts = list(urlsplit(url))
    parts[2] = quote(parts[2])
    endings = ("/availability", "/availability/")
    if parts[2].startswith("/v1/collections/") and any(
        parts[2].endswith(x) for x in endings
    ):
        parts[2] = parts[2].replace("/v1/collections/", "/v2/collections/", 1)
    query_string = parts[3]
    query_string = query_string.replace("+", "%2B")
    query_string = query_string.replace(":", "%3A")
    query_string = query_string.replace("{", "%7B")
    query_string = query_string.replace("}", "%7D")
    parts[3] = query_string
    return urlunsplit(tuple(parts))
