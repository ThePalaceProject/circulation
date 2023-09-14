import urllib
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable
from unittest.mock import patch

import pytest
from flask import has_request_context

from core.feed.annotator.circulation import CirculationManagerAnnotator


def _patched_url_for(*args: Any, _original=None, **kwargs: Any) -> str:
    """Test mode url_for for the annotators
    :param _original: Is the original Annotator.url_for method
    """
    if has_request_context() and _original:
        # Ignore the patch if we have a request context
        return _original(object(), *args, **kwargs)
    # Generate a plausible-looking URL that doesn't depend on Flask
    # being set up.
    host = "host"
    url = ("http://%s/" % host) + "/".join(args)
    connector = "?"
    for k, v in sorted(kwargs.items()):
        if v is None:
            v = ""
        v = urllib.parse.quote(str(v))
        k = urllib.parse.quote(str(k))
        url += connector + f"{k}={v}"
        connector = "&"
    return url


@dataclass
class PatchedUrlFor:
    patched_url_for: Callable


@pytest.fixture(scope="function")
def patch_url_for():
    """Patch the url_for method for annotators"""
    with patch(
        "core.feed.annotator.circulation.CirculationManagerAnnotator.url_for",
        new=partial(_patched_url_for, _original=CirculationManagerAnnotator.url_for),
    ) as patched:
        yield PatchedUrlFor(patched)
