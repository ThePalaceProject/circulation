from core.model import Collection, ExternalIntegration, get_one_or_create
from tests.fixtures.api_controller import (
    CirculationControllerFixture,
    ControllerFixtureSetupOverrides,
)


class TestMultipleLibraries:
    def test_authentication(self, controller_fixture: CirculationControllerFixture):
        """It's possible to authenticate with multiple libraries and make a
        request that runs in the context of each different library.
        """

        def make_default_libraries(_db):
            return [controller_fixture.db.library() for x in range(2)]

        def make_default_collection(_db, library):
            collection, ignore = get_one_or_create(
                controller_fixture.db.session,
                Collection,
                name=f"{controller_fixture.db.fresh_str()} (for multi-library test)",
            )
            collection.create_external_integration(ExternalIntegration.OPDS_IMPORT)
            library.collections.append(collection)
            return collection

        controller_fixture.circulation_manager_setup(
            overrides=ControllerFixtureSetupOverrides(
                make_default_collection=make_default_collection,
                make_default_libraries=make_default_libraries,
            )
        )

        l1, l2 = controller_fixture.libraries
        assert l1 != l2
        for library in controller_fixture.libraries:
            headers = dict(Authorization=controller_fixture.valid_auth)
            with controller_fixture.request_context_with_library(
                "/", headers=headers, library=library
            ):
                patron = (
                    controller_fixture.manager.loans.authenticated_patron_from_request()
                )
                assert library == patron.library
                response = controller_fixture.manager.index_controller()
                assert (
                    "http://localhost/%s/groups/" % library.short_name
                    == response.headers["location"]
                )
