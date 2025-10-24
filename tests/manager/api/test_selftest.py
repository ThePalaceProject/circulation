"""Test circulation-specific extensions to the self-test infrastructure."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock

import pytest

from palace.manager.api.authentication.basic import BasicAuthenticationProvider
from palace.manager.api.selftest import (
    HasCollectionSelfTests,
    HasPatronSelfTests,
)
from palace.manager.core.exceptions import IntegrationException
from palace.manager.core.selftest import SelfTestResult
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture


class TestHasPatronSelfTests:
    def test__determine_self_test_patron(
        self,
        db: DatabaseTransactionFixture,
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
        db.simple_auth_integration(db.default_library())

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
        assert expected_message == excinfo.value.message
        assert excinfo.value.detail is None

    def test_default_patrons(
        self,
        db: DatabaseTransactionFixture,
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
        collection.associated_libraries.append(no_default_patron)

        # This library has a default patron set up.
        db.simple_auth_integration(db.default_library())

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
        results = hastests._no_delivery_mechanisms_test()
        assert isinstance(results, list)
        [result] = results
        assert "[title unknown] (ID: %s)" % pool.identifier.identifier == result

        # Change the LicensePool so it has no owned licenses.
        # Now the book is no longer considered problematic,
        # since it's not actually in the collection.
        pool.licenses_owned = 0
        result = hastests._no_delivery_mechanisms_test()
        assert success == result
