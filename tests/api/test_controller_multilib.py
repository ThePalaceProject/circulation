import pytest

from core.model import Collection, ExternalIntegration, get_one_or_create
from tests.fixtures.api_controller import CirculationControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.vendor_id import VendorIDFixture


@pytest.fixture
def multiple_circulation_fixture(
    db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture
):
    class MultipleLibraryFixture(CirculationControllerFixture):
        def make_default_libraries(self, _db):
            return [self.db.library() for x in range(2)]

        def make_default_collection(self, _db, library):
            collection, ignore = get_one_or_create(
                self.db.session,
                Collection,
                name=f"{self.db.fresh_str()} (for multi-library test)",
            )
            collection.create_external_integration(ExternalIntegration.OPDS_IMPORT)
            library.collections.append(collection)
            return collection

    return MultipleLibraryFixture(db, vendor_id_fixture)


class TestMultipleLibraries:
    def test_authentication(
        self, multiple_circulation_fixture: CirculationControllerFixture
    ):
        """It's possible to authenticate with multiple libraries and make a
        request that runs in the context of each different library.
        """
        l1, l2 = multiple_circulation_fixture.libraries
        assert l1 != l2
        for library in multiple_circulation_fixture.libraries:
            headers = dict(Authorization=multiple_circulation_fixture.valid_auth)
            with multiple_circulation_fixture.request_context_with_library(
                "/", headers=headers, library=library
            ):
                patron = (
                    multiple_circulation_fixture.manager.loans.authenticated_patron_from_request()
                )
                assert library == patron.library
                response = multiple_circulation_fixture.manager.index_controller()
                assert (
                    "http://cdn/%s/groups/" % library.short_name
                    == response.headers["location"]
                )
