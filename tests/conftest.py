from freezegun.config import configure as fg_configure
from pytest import register_assert_rewrite

register_assert_rewrite("tests.fixtures")

pytest_plugins = [
    "tests.fixtures.announcements",
    "tests.fixtures.api_admin",
    "tests.fixtures.api_controller",
    "tests.fixtures.api_routes",
    "tests.fixtures.authenticator",
    "tests.fixtures.celery",
    "tests.fixtures.database",
    "tests.fixtures.files",
    "tests.fixtures.flask",
    "tests.fixtures.library",
    "tests.fixtures.odl",
    "tests.fixtures.redis",
    "tests.fixtures.s3",
    "tests.fixtures.search",
    "tests.fixtures.services",
    "tests.fixtures.time",
    "tests.fixtures.tls_server",
    "tests.fixtures.vendor_id",
    "tests.fixtures.webserver",
]

# Make sure if we are using pyinstrument to profile tests, that
# freezegun doesn't interfere with it.
# See: https://github.com/spulec/freezegun#ignore-packages
fg_configure(extend_ignore_list=["pyinstrument"])
