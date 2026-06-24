import pytest
from freezegun.config import configure as fg_configure
from pytest import register_assert_rewrite

register_assert_rewrite("tests.fixtures")

pytest_plugins = [
    "tests.fixtures.announcements",
    "tests.fixtures.api_admin",
    "tests.fixtures.api_controller",
    "tests.fixtures.api_routes",
    "tests.fixtures.celery",
    "tests.fixtures.database",
    "tests.fixtures.equivalents",
    "tests.fixtures.files",
    "tests.fixtures.flask",
    "tests.fixtures.http",
    "tests.fixtures.library",
    "tests.fixtures.marc",
    "tests.fixtures.odl",
    "tests.fixtures.oidc",
    "tests.fixtures.overdrive",
    "tests.fixtures.pyinstrument",
    "tests.fixtures.redis",
    "tests.fixtures.s3",
    "tests.fixtures.search",
    "tests.fixtures.services",
    "tests.fixtures.test_utils",
    "tests.fixtures.time",
    "tests.fixtures.tls_server",
    "tests.fixtures.vendor_id",
    "tests.fixtures.webserver",
]

# Ensure that Freezegun does not interfere when using PyInstrument to profile tests or
# running Celery tests.
# See: https://github.com/spulec/freezegun#ignore-packages
fg_configure(extend_ignore_list=["pyinstrument", "celery"])


# Tests acquire a real OpenSearch instance through one of these fixtures (directly or
# transitively). The fake search fixture is intentionally excluded: it needs no running
# OpenSearch and therefore must not be marked ``opensearch``.
OPENSEARCH_FIXTURES = frozenset(
    {"external_search_fixture", "end_to_end_search_fixture"}
)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Automatically apply the ``db`` and ``opensearch`` markers based on fixture usage.

    These test sets are too numerous to mark by hand, so we derive the markers from fixture
    usage at collection time. This lets a run include or exclude them with ``-m db`` /
    ``-m 'not db'`` (e.g. the backwards-compatibility CI check runs ``-m db`` against an
    externally applied schema) and ``-m opensearch`` / ``-m 'not opensearch'`` (e.g. to skip
    tests that need a running OpenSearch instance). Runs ``tryfirst`` so the markers are
    present before pytest evaluates any ``-m`` expression.
    """
    for item in items:
        fixturenames = getattr(item, "fixturenames", ())
        if "db" in fixturenames:
            item.add_marker("db")
        if not OPENSEARCH_FIXTURES.isdisjoint(fixturenames):
            item.add_marker("opensearch")
