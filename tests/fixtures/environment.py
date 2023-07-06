import os

import pytest

from core.config import Configuration


@pytest.fixture(scope="session", autouse=True)
def clear_unneeded_environment_variables():
    """The testing environment may have variables that leak from the application space"""
    for key in (
        Configuration.CDN_BASE_URL_ENVIRONMENT_VARIABLE,
        Configuration.CDN_OPDS1_ENABLED_ENVIRONMENT_VARIABLE,
        Configuration.CDN_OPDS2_ENABLED_ENVIRONMENT_VARIABLE,
    ):
        if os.environ.get(key):
            del os.environ[key]
