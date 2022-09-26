# Pull in the session_fixture defined in core/testing.py
# which does the database setup and initialization
pytest_plugins = ["core.testing"]

import os

import pytest


def pytest_generate_tests(metafunc: pytest.Metafunc):
    _setup_dual_search_tests(metafunc)


# Test the env variables for dual testing availability
DUAL_SEARCH_PRESENT = None not in [
    os.environ.get("SIMPLIFIED_TEST_ELASTICSEARCH"),
    os.environ.get("SIMPLIFIED_TEST_OPENSEARCH"),
]


def _setup_dual_search_tests(metafunc: pytest.Metafunc):
    """If dual search indexes are available, we parameterize the marked test classes
    with 2 parameters so we have 2 runs of each method"""
    if (
        DUAL_SEARCH_PRESENT
        and metafunc.cls
        and getattr(metafunc.cls, "DUAL_SEARCH_TEST", None)
    ):
        metafunc.fixturenames.append("dual_search_test")
        metafunc.parametrize("dual_search_test", ["ElasticSearch", "OpenSearch"])
