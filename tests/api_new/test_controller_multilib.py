from tests.fixtures.api_controller import CirculationControllerFixture


class TestMultipleLibraries:
    def test_authentication(self, circulation_fixture: CirculationControllerFixture):
        """It's possible to authenticate with multiple libraries and make a
        request that runs in the context of each different library.
        """
        circulation_fixture.libraries.append(circulation_fixture.db.library())

        l1, l2 = circulation_fixture.libraries
        assert l1 != l2
        for library in circulation_fixture.libraries:
            headers = dict(Authorization=circulation_fixture.valid_auth)
            with circulation_fixture.request_context_with_library(
                "/", headers=headers, library=library
            ):
                patron = (
                    circulation_fixture.manager.loans.authenticated_patron_from_request()
                )
                assert library == patron.library
                response = circulation_fixture.manager.index_controller()
                assert (
                    "http://cdn/%s/groups/" % library.short_name
                    == response.headers["location"]
                )
