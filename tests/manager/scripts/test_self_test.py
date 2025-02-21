from __future__ import annotations

import datetime
from io import StringIO

from palace.manager.core.selftest import SelfTestResult
from palace.manager.scripts.self_test import RunSelfTestsScript
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from tests.fixtures.database import DatabaseTransactionFixture


class TestRunSelfTestsScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        library1 = db.default_library()
        library2 = db.library(name="library2")
        out = StringIO()

        class MockParsed:
            pass

        class MockScript(RunSelfTestsScript):
            tested = []

            def parse_command_line(self, *args, **kwargs):
                parsed = MockParsed()
                parsed.libraries = [library1, library2]
                return parsed

            def test_collection(self, collection, api_map):
                self.tested.append((collection, api_map))

        script = MockScript(db.session, out)
        script.do_run()
        # Both libraries were tested.
        assert out.getvalue() == "Testing {}\nTesting {}\n".format(
            library1.name,
            library2.name,
        )

        # `library2` doesn't have any associated collections, but `library1`
        # has two, the default and default_inactive collections.
        [(c_default, api_map_1), (c_default_inactive, api_map_2)] = script.tested
        assert {c_default, c_default_inactive} == {
            db.default_collection(),
            db.default_inactive_collection(),
        }
        assert set(library1.associated_collections) == {c_default, c_default_inactive}

        # The API lookup maps passed into test_collection() are `LicenseProvidersRegistry`s.
        assert isinstance(api_map_1, LicenseProvidersRegistry)
        assert isinstance(api_map_2, LicenseProvidersRegistry)

        # If test_collection raises an exception, the exception is recorded,
        # and we move on.
        class MockScript2(MockScript):
            def test_collection(self, collection, api_map):
                raise Exception(f"blah - {collection.name}")

        out = StringIO()
        script = MockScript2(db.session, out)
        script.do_run()
        assert out.getvalue() == (
            f"Testing {library1.name}\n"
            f"  Exception while running self-test: 'blah - {c_default.name}'\n"
            f"  Exception while running self-test: 'blah - {c_default_inactive.name}'\n"
            f"Testing {library2.name}\n"
        )

    def test_test_collection(self, db: DatabaseTransactionFixture):
        class MockScript(RunSelfTestsScript):
            processed = []

            def process_result(self, result):
                self.processed.append(result)

        collection = db.default_collection()

        # If the api_map does not map the collection's protocol to a
        # HasSelfTests class, nothing happens.
        out = StringIO()
        script = MockScript(db.session, out)
        script.test_collection(collection, api_map={})
        assert (
            out.getvalue()
            == " Cannot find a self-test for %s, ignoring.\n" % collection.name
        )

        # If the api_map does map the colelction's protocol to a
        # HasSelfTests class, the class's run_self_tests class method
        # is invoked. Any extra arguments found in the extra_args dictionary
        # are passed in to run_self_tests.
        class MockHasSelfTests:
            @classmethod
            def run_self_tests(cls, _db, constructor_method, *constructor_args):
                cls.run_self_tests_called_with = (_db, constructor_method)
                cls.run_self_tests_constructor_args = constructor_args
                return {}, ["result 1", "result 2"]

        out = StringIO()
        script = MockScript(db.session, out)
        protocol = db.default_collection().protocol
        script.test_collection(
            collection,
            api_map={protocol: MockHasSelfTests},
            extra_args={MockHasSelfTests: ["an extra arg"]},
        )

        # run_self_tests() was called with the correct arguments,
        # including the extra one.
        assert (db.session, None) == MockHasSelfTests.run_self_tests_called_with  # type: ignore
        assert (
            db.session,
            collection,
            "an extra arg",
        ) == MockHasSelfTests.run_self_tests_constructor_args  # type: ignore

        # Each result was run through process_result().
        assert ["result 1", "result 2"] == script.processed

    def test_process_result(self, db: DatabaseTransactionFixture):
        # Test a successful test that returned a result.
        success = SelfTestResult("i succeeded")
        success.success = True
        success.end = success.start + datetime.timedelta(seconds=1.5)
        success.result = "a result"
        out = StringIO()
        script = RunSelfTestsScript(db.session, out)
        script.process_result(success)
        assert out.getvalue() == "  SUCCESS i succeeded (1.5sec)\n   Result: a result\n"

        # Test a failed test that raised an exception.
        failure = SelfTestResult("i failed")
        failure.end = failure.start
        failure.exception = Exception("bah")
        out = StringIO()
        script = RunSelfTestsScript(db.session, out)
        script.process_result(failure)
        assert out.getvalue() == "  FAILURE i failed (0.0sec)\n   Exception: 'bah'\n"
