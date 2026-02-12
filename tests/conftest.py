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
