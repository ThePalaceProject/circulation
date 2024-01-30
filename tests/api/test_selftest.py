"""Test circulation-specific extensions to the self-test infrastructure."""
from __future__ import annotations

import datetime
from io import StringIO
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock

import pytest

from api.authentication.basic import BasicAuthenticationProvider
from api.circulation import CirculationAPI
from api.selftest import HasCollectionSelfTests, HasPatronSelfTests, SelfTestResult
from core.exceptions import IntegrationException
from core.model import Patron
from core.scripts import RunSelfTestsScript
from core.util.problem_detail import ProblemDetail
from tests.fixtures.authenticator import SimpleAuthIntegrationFixture

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture


class TestHasPatronSelfTests:
    def test__determine_self_test_patron(
        self,
        db: DatabaseTransactionFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
    ):
        """Test per-library default patron lookup for self-tests.

        Ensure that the tested method either:
        - returns a 2-tuple of (patron, password) or
        - raises the expected _NoValidLibrarySelfTestPatron exception.
        """

        test_patron_lookup_method = HasPatronSelfTests._determine_self_test_patron
        test_patron_lookup_exception = HasPatronSelfTests._NoValidLibrarySelfTestPatron

        # This library has no patron authentication integration configured.
        library_without_default_patron = db.library()
        with pytest.raises(test_patron_lookup_exception) as excinfo:
            test_patron_lookup_method(library_without_default_patron)
        assert "Library has no test patron configured." == excinfo.value.message
        assert (
            "You can specify a test patron when you configure the library's patron authentication service."
            == excinfo.value.detail
        )

        # Add a patron authentication integration
        create_simple_auth_integration(db.default_library())

        # This library's patron authentication integration has a default
        # patron (for this library).
        patron, password = test_patron_lookup_method(db.default_library())
        assert isinstance(patron, Patron)
        assert "username1" == patron.authorization_identifier
        assert "password1" == password

        # Patron authentication integration returns a problem detail.
        expected_message = "fake-pd-1 detail"
        expected_detail = "fake-pd-1 debug message"
        result_patron = ProblemDetail(
            "https://example.com/fake-problemdetail-1",
            title="fake-pd-1",
            detail=expected_message,
            debug_message=expected_detail,
        )
        result_password = None
        with mock.patch.object(
            BasicAuthenticationProvider, "testing_patron"
        ) as testing_patron:
            testing_patron.return_value = (result_patron, result_password)
            with pytest.raises(test_patron_lookup_exception) as excinfo:
                test_patron_lookup_method(db.default_library())
        assert expected_message == excinfo.value.message
        assert expected_detail == excinfo.value.detail

        # Patron authentication integration returns something that is neither
        # a Patron nor a ProblemDetail.
        result_patron = ()  # type: ignore
        result_patron_type = type(result_patron)
        expected_message = f"Authentication provider returned unexpected type ({result_patron_type}) instead of patron."
        with mock.patch.object(
            BasicAuthenticationProvider, "testing_patron"
        ) as testing_patron:
            testing_patron.return_value = (result_patron, None)
            with pytest.raises(test_patron_lookup_exception) as excinfo:
                test_patron_lookup_method(db.default_library())
        assert not isinstance(result_patron, (Patron, ProblemDetail))
        assert expected_message == excinfo.value.message  # type: ignore
        assert excinfo.value.detail is None

    def test_default_patrons(
        self,
        db: DatabaseTransactionFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
    ):
        """Some self-tests must run with a patron's credentials.  The
        default_patrons() method finds the default Patron for every
        Library associated with a given Collection.
        """
        h = HasPatronSelfTests

        # This collection is not in any libraries, so there's no way
        # to test it.
        not_in_library = db.collection()
        [result] = h.default_patrons(not_in_library)
        assert isinstance(result, SelfTestResult)
        assert "Acquiring test patron credentials." == result.name
        assert False == result.success
        assert isinstance(result.exception, IntegrationException)
        assert "Collection is not associated with any libraries." == str(
            result.exception
        )
        assert (
            "Add the collection to a library that has a patron authentication service."
            == result.exception.debug_message
        )

        # This collection is in two libraries.
        collection = db.default_collection()

        # This library has no default patron set up.
        no_default_patron = db.library()
        collection.libraries.append(no_default_patron)

        # This library has a default patron set up.
        create_simple_auth_integration(db.default_library())

        # Calling default_patrons on the Collection returns one result for
        # each Library associated with that Collection.

        results = list(h.default_patrons(collection))
        assert 2 == len(results)
        [failure] = [x for x in results if isinstance(x, SelfTestResult)]
        [success] = [x for x in results if x != failure]

        # A SelfTestResult indicating failure was returned for the library
        # without a test patron, since the test cannot proceed without one.
        assert failure.success is False
        assert (
            "Acquiring test patron credentials for library %s" % no_default_patron.name
            == failure.name
        )
        assert isinstance(failure.exception, IntegrationException)
        assert "Library has no test patron configured." == str(failure.exception)
        assert (
            "You can specify a test patron when you configure the library's patron authentication service."
            == failure.exception.debug_message
        )

        # The test patron for the library that has one was looked up,
        # and the test can proceed using this patron.
        assert isinstance(success, tuple)
        library, patron, password = success
        assert db.default_library() == library
        assert "username1" == patron.authorization_identifier
        assert "password1" == password


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

        # The default library is the only one with a collection;
        # test_collection() was called on that collection.
        [(collection, api_map)] = script.tested
        assert [collection] == library1.collections

        # The API lookup map passed into test_collection() is based on
        # CirculationAPI's default API map.
        registry = CirculationAPI(db.session, db.default_library()).registry
        for k, v in registry:
            assert api_map[k] == v

        # If test_collection raises an exception, the exception is recorded,
        # and we move on.
        class MockScript2(MockScript):
            def test_collection(self, collection, api_map):
                raise Exception("blah")

        out = StringIO()
        script = MockScript2(db.session, out)
        script.do_run()
        assert (
            out.getvalue()
            == "Testing %s\n  Exception while running self-test: 'blah'\nTesting %s\n"
            % (library1.name, library2.name)
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


class TestHasCollectionSelfTests:
    def test__run_self_tests(self, db: DatabaseTransactionFixture):
        # Verify that _run_self_tests calls all the test methods
        # we want it to.
        class Mock(HasCollectionSelfTests):
            # Mock the methods that run the actual tests.
            def _no_delivery_mechanisms_test(self):
                self._no_delivery_mechanisms_called = True
                return "1"

            @property
            def collection(self) -> None:
                return None

        mock = Mock()
        results = [x for x in mock._run_self_tests(MagicMock())]
        assert ["1"] == [x.result for x in results]
        assert True == mock._no_delivery_mechanisms_called

    def test__no_delivery_mechanisms_test(self, db: DatabaseTransactionFixture):
        # Verify that _no_delivery_mechanisms_test works whether all
        # titles in the collection have delivery mechanisms or not.

        # There's one LicensePool, and it has a delivery mechanism,
        # so a string is returned.
        pool = db.licensepool(None)

        class Mock(HasCollectionSelfTests):
            collection = db.default_collection()

        hastests = Mock()
        result = hastests._no_delivery_mechanisms_test()
        success = "All titles in this collection have delivery mechanisms."
        assert success == result

        # Destroy the delivery mechanism.
        for x in pool.delivery_mechanisms:
            db.session.delete(x)

        # Now a list of strings is returned, one for each problematic
        # book.
        [result] = hastests._no_delivery_mechanisms_test()
        assert "[title unknown] (ID: %s)" % pool.identifier.identifier == result

        # Change the LicensePool so it has no owned licenses.
        # Now the book is no longer considered problematic,
        # since it's not actually in the collection.
        pool.licenses_owned = 0
        result = hastests._no_delivery_mechanisms_test()
        assert success == result
