import json

import flask

from core.lane import Lane
from core.util.authentication_for_opds import AuthenticationForOPDSDocument
from tests.fixtures.api_controller import CirculationControllerFixture


class TestIndexController:
    def test_simple_redirect(self, circulation_fixture: CirculationControllerFixture):
        with circulation_fixture.app.test_request_context("/"):
            flask.request.library = circulation_fixture.library  # type: ignore
            response = circulation_fixture.manager.index_controller()
            assert 302 == response.status_code
            assert "http://localhost/default/groups/" == response.headers["location"]

    def test_custom_index_view(self, circulation_fixture: CirculationControllerFixture):
        """If a custom index view is registered for a library,
        it is called instead of the normal IndexController code.
        """

        class MockCustomIndexView:
            def __call__(self, library, annotator):
                self.called_with = (library, annotator)
                return "fake response"

        # Set up our MockCustomIndexView as the custom index for
        # the default library.
        mock = MockCustomIndexView()
        circulation_fixture.manager.custom_index_views[
            circulation_fixture.db.default_library().id
        ] = mock

        # Mock CirculationManager.annotator so it's easy to check
        # that it was called.
        mock_annotator = object()

        def make_mock_annotator(lane):
            assert lane == None
            return mock_annotator

        circulation_fixture.manager.annotator = make_mock_annotator  # type: ignore

        # Make a request, and the custom index is invoked.
        with circulation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=circulation_fixture.invalid_auth)
        ):
            response = circulation_fixture.manager.index_controller()
        assert "fake response" == response

        # The custom index was invoked with the library associated
        # with the request + the output of self.manager.annotator()
        library, annotator = mock.called_with
        assert circulation_fixture.db.default_library() == library
        assert mock_annotator == annotator

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
            assert (
                AuthenticationForOPDSDocument.MEDIA_TYPE
                == response.headers["Content-Type"]
            )
            data = response.get_data(as_text=True)
            assert (
                circulation_fixture.manager.auth.create_authentication_document()
                == data
            )

            # Make sure we got the A4OPDS document for the right library.
            doc = json.loads(data)
            assert library_name == doc["title"]

        # Verify that the authentication document cache is working.
        circulation_fixture.manager.authentication_for_opds_documents[
            library_name
        ] = "Cached value"
        with circulation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=circulation_fixture.invalid_auth)
        ):
            response = (
                circulation_fixture.manager.index_controller.authentication_document()
            )
            assert response.get_data(as_text=True) == "Cached value"

        # Verify what happens when the cache is disabled.
        circulation_fixture.manager.authentication_for_opds_documents.max_age = 0
        with circulation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=circulation_fixture.invalid_auth)
        ):
            response = (
                circulation_fixture.manager.index_controller.authentication_document()
            )
            assert response.get_data(as_text=True) != "Cached value"
