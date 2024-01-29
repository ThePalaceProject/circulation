from unittest.mock import MagicMock, create_autospec

from _pytest.monkeypatch import MonkeyPatch

from api.authenticator import LibraryAuthenticator
from api.circulation_manager import CirculationManager
from api.problem_details import *
from core.feed.annotator.circulation import (
    CirculationManagerAnnotator,
    LibraryAnnotator,
)
from core.lane import Facets, WorkList
from core.model import create
from core.model.discovery_service_registration import DiscoveryServiceRegistration
from core.problem_details import *
from core.util.problem_detail import ProblemDetail

# TODO: we can drop this when we drop support for Python 3.6 and 3.7
from tests.fixtures.api_controller import CirculationControllerFixture
from tests.fixtures.database import IntegrationConfigurationFixture
from tests.fixtures.services import ServicesFixture


class TestCirculationManager:
    """Test the CirculationManager object itself."""

    def test__init__(self, monkeypatch: MonkeyPatch, services_fixture: ServicesFixture):
        # Test that the CirculationManager is created with the right
        # configuration settings.
        mock_db = MagicMock()

        services_fixture.set_sitewide_config_option(
            "authentication_document_cache_time", 12
        )
        mock_load_settings = create_autospec(CirculationManager.load_settings)
        mock_setup_controllers = create_autospec(
            CirculationManager.setup_one_time_controllers
        )

        monkeypatch.setattr(CirculationManager, "load_settings", mock_load_settings)
        monkeypatch.setattr(
            CirculationManager, "setup_one_time_controllers", mock_setup_controllers
        )

        manager = CirculationManager(mock_db, services_fixture.services)

        assert mock_load_settings.called
        assert mock_setup_controllers.called

        assert manager.authentication_for_opds_documents.max_age == 12
        assert manager.services is services_fixture.services
        assert manager.analytics is services_fixture.analytics_fixture.analytics_mock
        assert manager.external_search is services_fixture.search_fixture.index_mock

    def test_load_settings(
        self,
        circulation_fixture: CirculationControllerFixture,
        create_integration_configuration: IntegrationConfigurationFixture,
        services_fixture: ServicesFixture,
    ):
        # Here's a CirculationManager which we've been using for a while.
        manager = circulation_fixture.manager

        # Certain fields of the CirculationManager have certain values
        # which are about to be reloaded.
        manager.auth = object()
        manager.patron_web_domains = set()

        # But some fields are _not_ about to be reloaded
        index_controller = manager.index_controller

        # The CirculationManager has a top-level lane and a CirculationAPI,
        # for the default library, but no others.
        assert 1 == len(manager.top_level_lanes)
        assert 1 == len(manager.circulation_apis)

        # The authentication document cache has a default value for
        # max_age.
        assert 3600 == manager.authentication_for_opds_documents.max_age

        # Now let's create a brand new library, never before seen.
        library = circulation_fixture.db.library()
        circulation_fixture.library_setup(library)

        # We also set up some configuration settings that will
        # be loaded.
        services_fixture.set_sitewide_config_option(
            "patron_web_hostnames", "http://sitewide"
        )

        # And a discovery service registration, that sets a web client url.
        registry = create_integration_configuration.discovery_service()
        create(
            circulation_fixture.db.session,
            DiscoveryServiceRegistration,
            library=library,
            integration=registry,
            web_client="http://registration",
        )

        # And a library-specific configuration setting.
        manager.authentication_for_opds_documents["test"] = "document"
        assert len(manager.authentication_for_opds_documents) == 1

        # Then reload the CirculationManager...
        circulation_fixture.manager.load_settings()

        # Now the new library has a top-level lane.
        assert library.id in manager.top_level_lanes

        # And a circulation API.
        assert library.id in manager.circulation_apis

        # The Authenticator has been reloaded with information about
        # how to authenticate patrons of the new library.
        assert isinstance(
            manager.auth.library_authenticators[library.short_name],  # type: ignore
            LibraryAuthenticator,
        )

        # So have the patron web domains
        assert {"http://sitewide", "http://registration"} == manager.patron_web_domains

        # The authentication document cache has been cleared
        assert len(manager.authentication_for_opds_documents) == 0

        # Controllers that don't depend on site configuration
        # have not been reloaded.
        assert index_controller == manager.index_controller

        # The sitewide patron web domain can also be set to *.
        services_fixture.set_sitewide_config_option("patron_web_hostnames", "*")
        circulation_fixture.manager.load_settings()
        assert {"*", "http://registration"} == manager.patron_web_domains

        # The sitewide patron web domain can also be a list of domains
        services_fixture.set_sitewide_config_option(
            "patron_web_hostnames",
            [
                "https://1.com",
                "http://2.com",
                "http://subdomain.3.com",
            ],
        )
        circulation_fixture.manager.load_settings()
        assert {
            "https://1.com",
            "http://2.com",
            "http://subdomain.3.com",
            "http://registration",
        } == manager.patron_web_domains

    def test_annotator(self, circulation_fixture: CirculationControllerFixture):
        # Test our ability to find an appropriate OPDSAnnotator for
        # any request context.

        # The simplest case -- a Lane is provided and we build a
        # LibraryAnnotator for its library
        lane = circulation_fixture.db.lane()
        facets = Facets.default(circulation_fixture.db.default_library())
        annotator = circulation_fixture.manager.annotator(lane, facets)
        assert isinstance(annotator, LibraryAnnotator)
        assert (
            circulation_fixture.manager.circulation_apis[
                circulation_fixture.db.default_library().id
            ]
            == annotator.circulation
        )
        assert "All Books" == annotator.top_level_title()
        assert True == annotator.identifies_patrons

        # Try again using a library that has no patron authentication.
        library2 = circulation_fixture.db.library()
        lane2 = circulation_fixture.db.lane(library=library2)
        mock_circulation = object()
        circulation_fixture.manager.circulation_apis[library2.id] = mock_circulation

        annotator = circulation_fixture.manager.annotator(lane2, facets)
        assert isinstance(annotator, LibraryAnnotator)
        assert library2 == annotator.library
        assert lane2 == annotator.lane
        assert facets == annotator.facets
        assert mock_circulation == annotator.circulation

        # This LibraryAnnotator knows not to generate any OPDS that
        # implies it has any way of authenticating or differentiating
        # between patrons.
        assert False == annotator.identifies_patrons

        # Any extra positional or keyword arguments passed into annotator()
        # are propagated to the Annotator constructor.
        class MockAnnotator:
            def __init__(self, *args, **kwargs):
                self.positional = args
                self.keyword = kwargs

        annotator = circulation_fixture.manager.annotator(
            lane,
            facets,
            "extra positional",
            kw="extra keyword",
            annotator_class=MockAnnotator,
        )
        assert isinstance(annotator, MockAnnotator)
        assert "extra positional" == annotator.positional[-1]
        assert "extra keyword" == annotator.keyword.pop("kw")

        # Now let's try more and more obscure ways of figuring out which
        # library should be used to build the LibraryAnnotator.

        # If a WorkList initialized with a library is provided, a
        # LibraryAnnotator for that library is created.
        worklist = WorkList()
        worklist.initialize(library2)
        annotator = circulation_fixture.manager.annotator(worklist, facets)
        assert isinstance(annotator, LibraryAnnotator)
        assert library2 == annotator.library
        assert worklist == annotator.lane
        assert facets == annotator.facets

        # If no library can be found through the WorkList,
        # LibraryAnnotator uses the library associated with the
        # current request.
        worklist = WorkList()
        worklist.initialize(None)
        with circulation_fixture.request_context_with_library("/"):
            annotator = circulation_fixture.manager.annotator(worklist, facets)
            assert isinstance(annotator, LibraryAnnotator)
            assert circulation_fixture.db.default_library() == annotator.library
            assert worklist == annotator.lane

        # If there is absolutely no library associated with this
        # request, we get a generic CirculationManagerAnnotator for
        # the provided WorkList.
        with circulation_fixture.app.test_request_context("/"):
            annotator = circulation_fixture.manager.annotator(worklist, facets)
            assert isinstance(annotator, CirculationManagerAnnotator)
            assert worklist == annotator.lane

    def test_load_facets_from_request_denies_access_to_inaccessible_worklist(
        self, circulation_fixture: CirculationControllerFixture
    ):
        """You can't access a WorkList that's inaccessible to your patron
        type, and load_facets_from_request (which is called when
        presenting the WorkList) is where we enforce this.
        """
        wl = WorkList()
        wl.accessible_to = MagicMock(return_value=True)  # type: ignore

        # The authenticated patron, if any, is passed into
        # WorkList.accessible_to.
        with circulation_fixture.request_context_with_library("/"):
            facets = circulation_fixture.manager.load_facets_from_request(worklist=wl)
            assert isinstance(facets, Facets)
            wl.accessible_to.assert_called_once_with(None)

        with circulation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=circulation_fixture.valid_auth)
        ):
            facets = circulation_fixture.manager.load_facets_from_request(worklist=wl)
            assert isinstance(facets, Facets)
            wl.accessible_to.assert_called_with(circulation_fixture.default_patron)

        # The request is short-circuited if accessible_to returns
        # False.
        wl.accessible_to = MagicMock(return_value=False)  # type: ignore
        with circulation_fixture.request_context_with_library("/"):
            facets = circulation_fixture.manager.load_facets_from_request(worklist=wl)
            assert isinstance(facets, ProblemDetail)

            # Because the patron didn't ask for a specific title, we
            # respond that the lane doesn't exist rather than saying
            # they've been denied access to age-inappropriate content.
            assert NO_SUCH_LANE.uri == facets.uri
