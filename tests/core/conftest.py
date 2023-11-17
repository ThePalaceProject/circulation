from freezegun.config import configure as fg_configure

pytest_plugins = [
    "tests.fixtures.announcements",
    "tests.fixtures.csv_files",
    "tests.fixtures.database",
    "tests.fixtures.library",
    "tests.fixtures.opds2_files",
    "tests.fixtures.opds_files",
    "tests.fixtures.s3",
    "tests.fixtures.sample_covers",
    "tests.fixtures.search",
    "tests.fixtures.services",
    "tests.fixtures.time",
    "tests.fixtures.tls_server",
    "tests.fixtures.webserver",
]

# Make sure if we are using pyinstrument to profile tests, that
# freezegun doesn't interfere with it.
# See: https://github.com/spulec/freezegun#ignore-packages
fg_configure(extend_ignore_list=["pyinstrument"])
