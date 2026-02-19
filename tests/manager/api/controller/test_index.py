import json

from palace.manager.opds.authentication import AuthenticationDocument
from palace.manager.sqlalchemy.model.lane import Lane
from tests.fixtures.api_controller import CirculationControllerFixture


class TestIndexController:
    def test_simple_redirect(self, circulation_fixture: CirculationControllerFixture):
        with circulation_fixture.app.test_request_context("/") as ctx:
            setattr(ctx.request, "library", circulation_fixture.library)
            response = circulation_fixture.manager.index_controller()
            assert 302 == response.status_code
            assert "http://localhost/default/groups/" == response.headers["location"]

    def test_authenticated_patron_root_lane(
        self, circulation_fixture: CirculationControllerFixture
    ):
        root_1, root_2 = circulation_fixture.db.session.query(Lane).all()[:2]

        # Patrons of external type '1' and '2' have a certain root lane.
        root_1.root_for_patron_type = ["1", "2"]

        # Patrons of external type '3' have a different root.
        root_2.root_for_patron_type = ["3"]

        circulation_fixture.default_patron.external_type = "1"
        with circulation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=circulation_fixture.invalid_auth)
        ):
            response = circulation_fixture.manager.index_controller()
            assert 401 == response.status_code

        with circulation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=circulation_fixture.valid_auth)
        ):
            response = circulation_fixture.manager.index_controller()
            assert 302 == response.status_code
            assert (
                "http://localhost/default/groups/%s" % root_1.id
                == response.headers["location"]
            )

        circulation_fixture.default_patron.external_type = "2"
        with circulation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=circulation_fixture.valid_auth)
        ):
            response = circulation_fixture.manager.index_controller()
            assert 302 == response.status_code
            assert (
                "http://localhost/default/groups/%s" % root_1.id
                == response.headers["location"]
            )

        circulation_fixture.default_patron.external_type = "3"
        with circulation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=circulation_fixture.valid_auth)
        ):
            response = circulation_fixture.manager.index_controller()
            assert 302 == response.status_code
            assert (
                "http://localhost/default/groups/%s" % root_2.id
                == response.headers["location"]
            )

        # Patrons with a different type get sent to the top-level lane.
        circulation_fixture.default_patron.external_type = "4"
        with circulation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=circulation_fixture.valid_auth)
        ):
            response = circulation_fixture.manager.index_controller()
            assert 302 == response.status_code
            assert "http://localhost/default/groups/" == response.headers["location"]

        # Patrons with no type get sent to the top-level lane.
        circulation_fixture.default_patron.external_type = None
        with circulation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=circulation_fixture.valid_auth)
        ):
            response = circulation_fixture.manager.index_controller()
            assert 302 == response.status_code
            assert "http://localhost/default/groups/" == response.headers["location"]

    def test_authentication_document(
        self, circulation_fixture: CirculationControllerFixture
    ):
        # Test the ability to retrieve an Authentication For OPDS document.
        library_name = circulation_fixture.library.short_name
        with circulation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=circulation_fixture.invalid_auth)
        ):
            response = (
                circulation_fixture.manager.index_controller.authentication_document()
            )
            assert 200 == response.status_code
            assert AuthenticationDocument.MEDIA_TYPE == response.headers["Content-Type"]
            data = response.get_data(as_text=True)
            assert (
                circulation_fixture.manager.auth.create_authentication_document()
                == data
            )

            # Make sure we got the A4OPDS document for the right library.
            doc = json.loads(data)
            assert library_name == doc["title"]
