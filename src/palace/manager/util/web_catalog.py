"""Helpers for building links into the web catalog."""

from collections.abc import Iterable
from urllib.parse import parse_qs, unquote_plus, urlencode, urlsplit

# Query parameter the web catalog reads for the libraries to promote, to make them
# easier to find. Libraries are named by short name, comma-separated, so a short
# name containing a comma cannot be promoted.
PROMOTE_LIBRARIES_PARAM = "promote"

# Query parameter the web catalog reads for the label of the promoted libraries.
PROMOTE_LABEL_PARAM = "promoteLabel"

_PROMOTE_PARAMS = frozenset({PROMOTE_LIBRARIES_PARAM, PROMOTE_LABEL_PARAM})


def promote_label(url: str) -> str | None:
    """Returns the promote label in a web catalog URL.

    :param url: Web catalog URL

    :return: The first non-blank promote label, stripped, or None if there is none
    """
    for label in parse_qs(urlsplit(url).query).get(PROMOTE_LABEL_PARAM, []):
        if label := label.strip():
            return label
    return None


def with_promoted_libraries(
    url: str, libraries: Iterable[str], label: str | None = None
) -> str:
    """Returns the web catalog URL with the given libraries promoted.

    Any promoted libraries and promote label already in the URL are replaced, or
    removed if no libraries are given. Other query parameters and the fragment are
    kept as written, though empty query pairs are dropped.

    :param url: Web catalog URL
    :param libraries: Short names of the libraries to promote, in display order
    :param label: Label for the promoted libraries, if any

    :return: The URL with the promoted libraries
    """
    parts = urlsplit(url)
    query = [
        pair
        for pair in parts.query.split("&")
        if pair and unquote_plus(pair.partition("=")[0]) not in _PROMOTE_PARAMS
    ]

    if library_list := ",".join(libraries):
        params = {PROMOTE_LIBRARIES_PARAM: library_list}
        if label:
            params[PROMOTE_LABEL_PARAM] = label
        query.append(urlencode(params, safe=","))

    return parts._replace(query="&".join(query)).geturl()
