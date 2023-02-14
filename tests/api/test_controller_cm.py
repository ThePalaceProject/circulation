from unittest.mock import MagicMock

from api.adobe_vendor_id import AuthdataUtility, DeviceManagementProtocolController
from api.authenticator import LibraryAuthenticator, OAuthController
from api.config import Configuration
from api.controller import CirculationManager
from api.custom_index import CustomIndexView
from api.opds import CirculationManagerAnnotator, LibraryAnnotator
from api.problem_details import *
from api.registration.registry import Registration, RegistrationConstants
from api.shared_collection import SharedCollectionAPI
from core.config import CannotLoadConfiguration
from core.external_search import MockExternalSearchIndex
from core.lane import Facets, WorkList
from core.model import Admin, CachedFeed, ConfigurationSetting, ExternalIntegration
from core.problem_details import *
from core.util.problem_detail import ProblemDetail

# TODO: we can drop this when we drop support for Python 3.6 and 3.7
from tests.fixtures.api_controller import CirculationControllerFixture


class TestCirculationManager:
    """Test the CirculationManager object itself."""

    def test_initialization(self, circulation_fixture: CirculationControllerFixture):
        # As soon as the CirculationManager object is created,
        # it sets a public/private key pair for the site.
        public, private = ConfigurationSetting.sitewide(
            circulation_fixture.db.session, Configuration.KEY_PAIR
        ).json_value
        assert "BEGIN PUBLIC KEY" in public
        assert "BEGIN RSA PRIVATE KEY" in private

    def test_load_settings(self, circulation_fixture: CirculationControllerFixture):
        # Here's a CirculationManager which we've been using for a while.
        manager = circulation_fixture.manager

        # Certain fields of the CirculationManager have certain values
        # which are about to be reloaded.
        manager._external_search = object()
        manager.adobe_device_management = object()
        manager.oauth_controller = object()
        manager.auth = object()
        manager.shared_collection_api = object()
        manager.new_custom_index_views = object()
        manager.patron_web_domains = object()

        # But some fields are _not_ about to be reloaded
        index_controller = manager.index_controller

        # The CirculationManager has a top-level lane and a CirculationAPI,
        # for the default library, but no others.
        assert 1 == len(manager.top_level_lanes)
        assert 1 == len(manager.circulation_apis)

        # The authentication document cache has a default value for
        # max_age.
        assert 0 == manager.authentication_for_opds_documents.max_age

        # WSGI debug is off by default.
        assert False == manager.wsgi_debug

        # Now let's create a brand new library, never before seen.
        library = circulation_fixture.db.library()
        circulation_fixture.library_setup(library)

        # In addition to the setup performed by library_setup(), give it
        # a registry integration with short client tokens so we can verify
        # that the DeviceManagementProtocolController is recreated.
        circulation_fixture.vendor_ids.initialize_adobe(library, [library])

        # We also register a CustomIndexView for this new library.
        mock_custom_view = object()

        def mock_for_library(incoming_library):
            if incoming_library == library:
                return mock_custom_view
            return None

        old_for_library = CustomIndexView.for_library
        CustomIndexView.for_library = mock_for_library

        # We also set up some configuration settings that will
        # be loaded.
        ConfigurationSetting.sitewide(
            circulation_fixture.db.session, Configuration.PATRON_WEB_HOSTNAMES
        ).value = "http://sitewide/1234"
        registry = circulation_fixture.db.external_integration(
            protocol="some protocol", goal=ExternalIntegration.DISCOVERY_GOAL
        )
        ConfigurationSetting.for_library_and_externalintegration(
            circulation_fixture.db.session,
            Registration.LIBRARY_REGISTRATION_WEB_CLIENT,
            library,
            registry,
        ).value = "http://registration"

        ConfigurationSetting.sitewide(
            circulation_fixture.db.session,
            Configuration.AUTHENTICATION_DOCUMENT_CACHE_TIME,
        ).value = "60"

        ConfigurationSetting.sitewide(
            circulation_fixture.db.session, Configuration.WSGI_DEBUG_KEY
        ).value = "true"

        # Then reload the CirculationManager...
        circulation_fixture.manager.load_settings()

        # Now the new library has a top-level lane.
        assert library.id in manager.top_level_lanes

        # And a circulation API.
        assert library.id in manager.circulation_apis

        # And a CustomIndexView.
        assert mock_custom_view == manager.custom_index_views[library.id]
        assert (
            None
            == manager.custom_index_views[circulation_fixture.db.default_library().id]
        )

        # The Authenticator has been reloaded with information about
        # how to authenticate patrons of the new library.
        assert isinstance(
            manager.auth.library_authenticators[library.short_name],  # type: ignore
            LibraryAuthenticator,
        )

        # The ExternalSearch object has been reset.
        assert isinstance(manager.external_search, MockExternalSearchIndex)

        # The OAuth controller has been recreated.
        assert isinstance(manager.oauth_controller, OAuthController)

        # So has the controller for the Device Management Protocol.
        assert isinstance(
            manager.adobe_device_management, DeviceManagementProtocolController
        )

        # So has the SharecCollectionAPI.
        assert isinstance(manager.shared_collection_api, SharedCollectionAPI)

        # So have the patron web domains, and their paths have been
        # removed.
        assert {"http://sitewide", "http://registration"} == manager.patron_web_domains

        # The authentication document cache has been rebuilt with a
        # new max_age.
        assert 60 == manager.authentication_for_opds_documents.max_age

        # The WSGI debug setting has been changed.
        assert True == manager.wsgi_debug

        # Controllers that don't depend on site configuration
        # have not been reloaded.
        assert index_controller == manager.index_controller

        # The sitewide patron web domain can also be set to *.
        ConfigurationSetting.sitewide(
            circulation_fixture.db.session, Configuration.PATRON_WEB_HOSTNAMES
        ).value = "*"
        circulation_fixture.manager.load_settings()
        assert {"*", "http://registration"} == manager.patron_web_domains

        # The sitewide patron web domain can have pipe separated domains, and will get spaces stripped
        ConfigurationSetting.sitewide(
            circulation_fixture.db.session, Configuration.PATRON_WEB_HOSTNAMES
        ).value = "https://1.com|http://2.com |  http://subdomain.3.com|4.com"
        circulation_fixture.manager.load_settings()
        assert {
            "https://1.com",
            "http://2.com",
            "http://subdomain.3.com",
            "http://registration",
        } == manager.patron_web_domains

        # Restore the CustomIndexView.for_library implementation
        CustomIndexView.for_library = old_for_library

    def test_exception_during_external_search_initialization_is_stored(
        self, circulation_fixture: CirculationControllerFixture
    ):
        class BadSearch(CirculationManager):
            @property
            def setup_search(self):
                raise Exception("doomed!")

        circulation = BadSearch(circulation_fixture.db.session, testing=True)

        # We didn't get a search object.
        assert None == circulation.external_search

        # The reason why is stored here.
        ex = circulation.external_search_initialization_exception
        assert isinstance(ex, Exception)
        assert "doomed!" == str(ex)

    def test_exception_during_short_client_token_initialization_is_stored(
        self, circulation_fixture: CirculationControllerFixture
    ):

        # Create an incomplete Short Client Token setup for our
        # library.
        registry_integration = circulation_fixture.db.external_integration(
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
            libraries=[circulation_fixture.library],
        )
        registry_integration.username = "something"
        registry_integration.set_setting(AuthdataUtility.VENDOR_ID_KEY, "vendorid")
        # Indicate that the library has successfully registered.
        ConfigurationSetting.for_library_and_externalintegration(
            circulation_fixture.db.session,
            RegistrationConstants.LIBRARY_REGISTRATION_STATUS,
            circulation_fixture.library,
            registry_integration,
        ).value = RegistrationConstants.SUCCESS_STATUS

        # Then try to set up the Adobe Vendor ID configuration for
        # that library.
        circulation_fixture.manager.setup_adobe_vendor_id(
            circulation_fixture.db.session, circulation_fixture.library
        )

        # The exception caused when we tried to load the incomplete
        # configuration was stored here.
        ex = circulation_fixture.manager.short_client_token_initialization_exceptions[
            circulation_fixture.library.id
        ]
        assert isinstance(ex, CannotLoadConfiguration)
        assert str(ex).startswith("Short Client Token configuration is incomplete")

    def test_setup_adobe_vendor_id_does_not_override_existing_configuration(
        self, circulation_fixture: CirculationControllerFixture
    ):
        # Our circulation manager is perfectly happy with its Adobe Vendor ID
        # configuration, which it got from one of its libraries.
        obj = object()
        circulation_fixture.manager.adobe_vendor_id = obj

        # This library wants to set up an Adobe Vendor ID but it doesn't
        # actually have one configured.
        circulation_fixture.manager.setup_adobe_vendor_id(
            circulation_fixture.db.session, circulation_fixture.db.default_library()
        )

        # The sitewide Adobe Vendor ID configuration is not changed by
        # the presence of another library that doesn't have a Vendor
        # ID configuration.
        assert obj == circulation_fixture.manager.adobe_vendor_id

    def test_sitewide_key_pair(self, circulation_fixture: CirculationControllerFixture):
        # A public/private key pair was created when the
        # CirculationManager was initialized. Clear it out.
        pair = ConfigurationSetting.sitewide(
            circulation_fixture.db.session, Configuration.KEY_PAIR
        )
        pair.value = None

        # Calling sitewide_key_pair will create a new pair of keys.
        new_public, new_private = circulation_fixture.manager.sitewide_key_pair
        assert "BEGIN PUBLIC KEY" in new_public
        assert "BEGIN RSA PRIVATE KEY" in new_private

        # The new values are stored in the appropriate
        # ConfigurationSetting.
        assert [new_public, new_private] == pair.json_value

        # Calling it again will do nothing.
        assert (
            new_public,
            new_private,
        ) == circulation_fixture.manager.sitewide_key_pair

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

    def test_load_facets_from_request_disable_caching(
        self, circulation_fixture: CirculationControllerFixture
    ):
        # Only an authenticated admin can ask to disable caching,
        # and load_facets_from_request is where we enforce this.
        class MockAdminSignInController:
            # Pretend to be able to find (or not) an Admin authenticated
            # to make the current request.
            admin = None

            def authenticated_admin_from_request(self):
                return self.admin

        admin = Admin()
        controller = MockAdminSignInController()

        circulation_fixture.manager.admin_sign_in_controller = controller

        with circulation_fixture.request_context_with_library("/"):
            # If you don't specify a max cache age, nothing happens,
            # whether or not you're an admin.
            for value in INVALID_CREDENTIALS, admin:
                controller.admin = value  # type: ignore
                facets = circulation_fixture.manager.load_facets_from_request()
                assert None == facets.max_cache_age

        with circulation_fixture.request_context_with_library("/?max_age=0"):
            # Not an admin, max cache age requested.
            controller.admin = INVALID_CREDENTIALS  # type: ignore
            facets = circulation_fixture.manager.load_facets_from_request()
            assert None == facets.max_cache_age

            # Admin, max age requested. This is the only case where
            # nonstandard caching rules make it through
            # load_facets_from_request().
            controller.admin = admin  # type: ignore
            facets = circulation_fixture.manager.load_facets_from_request()
            assert CachedFeed.IGNORE_CACHE == facets.max_cache_age

        # Since the admin sign-in controller is part of the admin
        # package and not the API proper, test a situation where, for
        # whatever reason, that controller was never initialized.
        del circulation_fixture.manager.admin_sign_in_controller

        # Now what controller.admin says doesn't matter, because the
        # controller's not associated with the CirculationManager.
        # But everything still basically works; you just can't
        # disable the cache.
        with circulation_fixture.request_context_with_library("/?max_age=0"):
            for value in (INVALID_CREDENTIALS, admin):
                controller.admin = value  # type: ignore
                facets = circulation_fixture.manager.load_facets_from_request()
                assert None == facets.max_cache_age

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
            assert NO_SUCH_LANE.uri == facets.uri  # type: ignore
