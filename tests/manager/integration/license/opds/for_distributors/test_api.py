import json
from collections.abc import Callable
from unittest.mock import MagicMock

import pytest

from palace.manager.api.circulation.exceptions import (
    CannotFulfill,
    DeliveryMechanismError,
    LibraryAuthorizationFailedException,
)
from palace.manager.api.circulation.fulfillment import (
    DirectFulfillment,
    RedirectFulfillment,
)
from palace.manager.integration.license.opds.for_distributors.api import (
    OPDSForDistributorsAPI,
)
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, RightsStatus
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.datetime_helpers import utc_now
from tests.manager.integration.license.opds.for_distributors.conftest import (
    OPDSForDistributorsAPIFixture,
)
from tests.mocks.opds_for_distributors import MockOPDSForDistributorsAPI


@pytest.fixture()
def authentication_document() -> Callable[[str], str]:
    """Returns a method that computes an authentication document."""

    def _auth_doc(without_links=False) -> str:
        """Returns an authentication document.

        :param without_links: Whether or not to include an authenticate link.
        """
        links = (
            {
                "links": [
                    {
                        "rel": "authenticate",
                        "href": "http://authenticate",
                    }
                ],
            }
            if not without_links
            else {}
        )
        doc: dict[str, list[dict[str, str | list]]] = {
            "authentication": [
                {
                    **{"type": "http://opds-spec.org/auth/oauth/client_credentials"},
                    **links,
                },
            ]
        }
        return json.dumps(doc)

    return _auth_doc


class TestOPDSForDistributorsAPI:
    def test__run_self_tests(
        self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture
    ):
        """The self-test for OPDSForDistributorsAPI just tries to negotiate
        a fulfillment token.
        """

        class Mock(OPDSForDistributorsAPI):
            def __init__(self):
                pass

            def _get_token(self, _db):
                self.called_with = _db
                return "a token"

        api = Mock()
        [result] = api._run_self_tests(opds_dist_api_fixture.db.session)
        assert opds_dist_api_fixture.db.session == api.called_with
        assert "Negotiate a fulfillment token" == result.name
        assert True == result.success
        assert "a token" == result.result

    def test_supported_media_types(
        self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture
    ):
        # If the default client supports media type X with the
        # BEARER_TOKEN access control scheme, then X is a supported
        # media type for an OPDS For Distributors collection.
        supported = opds_dist_api_fixture.api.SUPPORTED_MEDIA_TYPES
        for format, drm in DeliveryMechanism.default_client_can_fulfill_lookup:
            if drm == (DeliveryMechanism.BEARER_TOKEN) and format is not None:
                assert format in supported

        # Here's a media type that sometimes shows up in OPDS For
        # Distributors collections but is _not_ supported. Incoming
        # items with this media type will _not_ be imported.
        assert MediaTypes.JPEG_MEDIA_TYPE not in supported

    def test_can_fulfill_without_loan(
        self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture
    ):
        """A book made available through OPDS For Distributors can be
        fulfilled with no underlying loan, if its delivery mechanism
        uses bearer token fulfillment.
        """
        patron = MagicMock()
        pool = opds_dist_api_fixture.db.licensepool(
            edition=None, collection=opds_dist_api_fixture.collection
        )
        [lpdm] = pool.delivery_mechanisms

        m = opds_dist_api_fixture.api.can_fulfill_without_loan

        # No LicensePoolDeliveryMechanism -> False
        assert False == m(patron, pool, MagicMock())

        # No LicensePool -> False (there can be multiple LicensePools for
        # a single LicensePoolDeliveryMechanism).
        assert False == m(patron, MagicMock(), lpdm)

        # No DeliveryMechanism -> False
        old_dm = lpdm.delivery_mechanism
        lpdm.delivery_mechanism = None
        assert False == m(patron, pool, lpdm)

        # DRM mechanism requires identifying a specific patron -> False
        lpdm.delivery_mechanism = old_dm
        lpdm.delivery_mechanism.drm_scheme = DeliveryMechanism.ADOBE_DRM
        assert False == m(patron, pool, lpdm)

        # Otherwise -> True
        lpdm.delivery_mechanism.drm_scheme = DeliveryMechanism.NO_DRM
        assert True == m(patron, pool, lpdm)

        lpdm.delivery_mechanism.drm_scheme = DeliveryMechanism.BEARER_TOKEN
        assert True == m(patron, pool, lpdm)

    def test_get_token_success(
        self,
        authentication_document,
        opds_dist_api_fixture: OPDSForDistributorsAPIFixture,
    ):
        # The API hasn't been used yet, so it will need to find the auth
        # document and authenticate url.
        feed = '<feed><link rel="http://opds-spec.org/auth/document" href="http://authdoc"/></feed>'

        opds_dist_api_fixture.api.queue_response(200, content=feed)
        opds_dist_api_fixture.api.queue_response(200, content=authentication_document())
        token = opds_dist_api_fixture.db.fresh_str()
        token_response = json.dumps({"access_token": token, "expires_in": 60})
        opds_dist_api_fixture.api.queue_response(200, content=token_response)

        assert (
            token
            == opds_dist_api_fixture.api._get_token(
                opds_dist_api_fixture.db.session
            ).credential
        )

        # Now that the API has the authenticate url, it only needs
        # to get the token.
        opds_dist_api_fixture.api.queue_response(200, content=token_response)
        assert (
            token
            == opds_dist_api_fixture.api._get_token(
                opds_dist_api_fixture.db.session
            ).credential
        )

        # A credential was created.
        [credential] = opds_dist_api_fixture.db.session.query(Credential).all()
        assert token == credential.credential

        # If we call _get_token again, it uses the existing credential.
        assert (
            token
            == opds_dist_api_fixture.api._get_token(
                opds_dist_api_fixture.db.session
            ).credential
        )

        opds_dist_api_fixture.db.session.delete(credential)

        # Create a new API that doesn't have an auth url yet.
        opds_dist_api_fixture.api = MockOPDSForDistributorsAPI(
            opds_dist_api_fixture.db.session, opds_dist_api_fixture.collection
        )

        # This feed requires authentication and returns the auth document.
        opds_dist_api_fixture.api.queue_response(401, content=authentication_document())
        token = opds_dist_api_fixture.db.fresh_str()
        token_response = json.dumps({"access_token": token, "expires_in": 60})
        opds_dist_api_fixture.api.queue_response(200, content=token_response)

        assert (
            token
            == opds_dist_api_fixture.api._get_token(
                opds_dist_api_fixture.db.session
            ).credential
        )

    def test_credentials_for_multiple_collections(
        self,
        authentication_document,
        opds_dist_api_fixture: OPDSForDistributorsAPIFixture,
    ):
        # We should end up with distinct credentials for each collection.
        # We have an existing credential from the collection
        # [credential1] = opds_dist_api_fixture.db.session.query(Credential).all()
        # assert credential1.collection_id is not None

        feed = '<feed><link rel="http://opds-spec.org/auth/document" href="http://authdoc"/></feed>'

        # Getting a token for a collection should result in a cached credential.
        collection1 = opds_dist_api_fixture.mock_collection(
            name="Collection 1",
        )
        api1 = MockOPDSForDistributorsAPI(opds_dist_api_fixture.db.session, collection1)
        token1 = opds_dist_api_fixture.db.fresh_str()
        token1_response = json.dumps({"access_token": token1, "expires_in": 60})
        api1.queue_response(200, content=feed)
        api1.queue_response(200, content=authentication_document())
        api1.queue_response(200, content=token1_response)
        credential1 = api1._get_token(opds_dist_api_fixture.db.session)
        all_credentials = opds_dist_api_fixture.db.session.query(Credential).all()

        assert token1 == credential1.credential
        assert credential1.collection_id == collection1.id
        assert 1 == len(all_credentials)

        # Getting a token for a second collection should result in an
        # additional cached credential.
        collection2 = opds_dist_api_fixture.mock_collection(
            name="Collection 2",
        )
        api2 = MockOPDSForDistributorsAPI(opds_dist_api_fixture.db.session, collection2)
        token2 = opds_dist_api_fixture.db.fresh_str()
        token2_response = json.dumps({"access_token": token2, "expires_in": 60})
        api2.queue_response(200, content=feed)
        api2.queue_response(200, content=authentication_document())
        api2.queue_response(200, content=token2_response)

        credential2 = api2._get_token(opds_dist_api_fixture.db.session)
        all_credentials = opds_dist_api_fixture.db.session.query(Credential).all()

        assert token2 == credential2.credential
        assert credential2.collection_id == collection2.id

        # Both credentials should now be present.
        assert 2 == len(all_credentials)
        assert credential1 != credential2
        assert credential1 in all_credentials
        assert credential2 in all_credentials
        assert token1 != token2

    def test_get_token_errors(
        self,
        authentication_document,
        opds_dist_api_fixture: OPDSForDistributorsAPIFixture,
    ):
        no_auth_document = "<feed></feed>"
        opds_dist_api_fixture.api.queue_response(200, content=no_auth_document)
        with pytest.raises(LibraryAuthorizationFailedException) as excinfo:
            opds_dist_api_fixture.api._get_token(opds_dist_api_fixture.db.session)
        assert "No authentication document link found in http://opds" in str(
            excinfo.value
        )

        feed = '<feed><link rel="http://opds-spec.org/auth/document" href="http://authdoc"/></feed>'
        opds_dist_api_fixture.api.queue_response(200, content=feed)
        auth_doc_without_client_credentials = json.dumps({"authentication": []})
        opds_dist_api_fixture.api.queue_response(
            200, content=auth_doc_without_client_credentials
        )
        with pytest.raises(LibraryAuthorizationFailedException) as excinfo:
            opds_dist_api_fixture.api._get_token(opds_dist_api_fixture.db.session)
        assert (
            "Could not find any credential-based authentication mechanisms in http://authdoc"
            in str(excinfo.value)
        )

        # If our authentication document doesn't have a `rel="authenticate"` link
        # then we will not be able to fetch a token, so should raise and exception.
        opds_dist_api_fixture.api.queue_response(200, content=feed)
        opds_dist_api_fixture.api.queue_response(
            200, content=authentication_document(without_links=True)
        )
        with pytest.raises(LibraryAuthorizationFailedException) as excinfo:
            opds_dist_api_fixture.api._get_token(opds_dist_api_fixture.db.session)
        assert "Could not find any authentication links in http://authdoc" in str(
            excinfo.value
        )

        opds_dist_api_fixture.api.queue_response(200, content=feed)
        opds_dist_api_fixture.api.queue_response(200, content=authentication_document())
        token_response = json.dumps({"error": "unexpected error"})
        opds_dist_api_fixture.api.queue_response(200, content=token_response)
        with pytest.raises(LibraryAuthorizationFailedException) as excinfo:
            opds_dist_api_fixture.api._get_token(opds_dist_api_fixture.db.session)
        assert (
            'Document retrieved from http://authenticate is not a bearer token: {"error": "unexpected error"}'
            in str(excinfo.value)
        )

    def test_checkin(self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture):
        # The patron has two loans, one from this API's collection and
        # one from a different collection.
        patron = opds_dist_api_fixture.db.patron()

        data_source = DataSource.lookup(
            opds_dist_api_fixture.db.session, "Biblioboard", autocreate=True
        )
        edition, pool = opds_dist_api_fixture.db.edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=opds_dist_api_fixture.collection,
        )
        pool.loan_to(patron)

        other_collection = opds_dist_api_fixture.db.collection(protocol=OverdriveAPI)
        other_edition, other_pool = opds_dist_api_fixture.db.edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=other_collection,
        )
        other_pool.loan_to(patron)

        assert 2 == opds_dist_api_fixture.db.session.query(Loan).count()

        opds_dist_api_fixture.api.checkin(patron, "1234", pool)

        # The loan from this API's collection has been deleted.
        # The loan from the other collection wasn't touched.
        assert 1 == opds_dist_api_fixture.db.session.query(Loan).count()
        [loan] = opds_dist_api_fixture.db.session.query(Loan).all()
        assert other_pool == loan.license_pool

    def test_checkout(self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture):
        patron = opds_dist_api_fixture.db.patron()

        data_source = DataSource.lookup(
            opds_dist_api_fixture.db.session, "Biblioboard", autocreate=True
        )
        edition, pool = opds_dist_api_fixture.db.edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=opds_dist_api_fixture.collection,
        )

        loan_info = opds_dist_api_fixture.api.checkout(
            patron, "1234", pool, MagicMock()
        )
        assert opds_dist_api_fixture.collection.id == loan_info.collection_id
        assert Identifier.URI == loan_info.identifier_type
        assert pool.identifier.identifier == loan_info.identifier

        # The loan's start date has been set to the current time.
        now = utc_now()
        assert loan_info.start_date is not None
        assert (now - loan_info.start_date).seconds < 2

        # The loan is of indefinite duration.
        assert None == loan_info.end_date

    @pytest.mark.parametrize(
        "drm_scheme,acquisition_rel_type",
        [
            pytest.param(
                DeliveryMechanism.BEARER_TOKEN,
                Hyperlink.GENERIC_OPDS_ACQUISITION,
                id="bearer token with generic acquisition",
            ),
            pytest.param(
                DeliveryMechanism.NO_DRM,
                Hyperlink.OPEN_ACCESS_DOWNLOAD,
                id="no drm and open access download",
            ),
        ],
    )
    def test_fulfill(
        self,
        opds_dist_api_fixture: OPDSForDistributorsAPIFixture,
        drm_scheme,
        acquisition_rel_type,
    ):
        patron = opds_dist_api_fixture.db.patron()

        data_source = DataSource.lookup(
            opds_dist_api_fixture.db.session, "Biblioboard", autocreate=True
        )
        edition, pool = opds_dist_api_fixture.db.edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=opds_dist_api_fixture.collection,
        )
        pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE,
            drm_scheme,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        # Find the correct delivery mechanism
        delivery_mechanism = None
        for mechanism in pool.delivery_mechanisms:
            if mechanism.delivery_mechanism.drm_scheme == drm_scheme:
                delivery_mechanism = mechanism
        assert delivery_mechanism is not None

        # This pool doesn't have an acquisition link, so
        # we can't fulfill it yet.
        pytest.raises(
            CannotFulfill,
            opds_dist_api_fixture.api.fulfill,
            patron,
            "1234",
            pool,
            delivery_mechanism,
        )

        # Set up an epub acquisition link for the pool.
        url = opds_dist_api_fixture.db.fresh_url()
        link, ignore = pool.identifier.add_link(
            acquisition_rel_type,
            url,
            data_source,
            Representation.EPUB_MEDIA_TYPE,
        )
        delivery_mechanism.resource = link.resource

        # Set the API's auth url so it doesn't have to get it -
        # that's tested in test_get_token.
        opds_dist_api_fixture.api.auth_url = "http://auth"

        token_response = json.dumps({"access_token": "token", "expires_in": 60})
        opds_dist_api_fixture.api.queue_response(200, content=token_response)

        fulfillment = opds_dist_api_fixture.api.fulfill(
            patron, "1234", pool, delivery_mechanism
        )

        if drm_scheme == DeliveryMechanism.BEARER_TOKEN:
            assert delivery_mechanism
            assert acquisition_rel_type == Hyperlink.GENERIC_OPDS_ACQUISITION
            assert isinstance(fulfillment, DirectFulfillment)
            assert DeliveryMechanism.BEARER_TOKEN == fulfillment.content_type
            assert fulfillment.content is not None
            bearer_token_document = json.loads(fulfillment.content)
            expires_in = bearer_token_document["expires_in"]
            assert expires_in < 60
            assert "Bearer" == bearer_token_document["token_type"]
            assert "token" == bearer_token_document["access_token"]
            assert url == bearer_token_document["location"]
        else:
            assert drm_scheme == DeliveryMechanism.NO_DRM
            assert acquisition_rel_type == Hyperlink.OPEN_ACCESS_DOWNLOAD
            assert isinstance(fulfillment, RedirectFulfillment)
            assert fulfillment.content_link == url
            assert fulfillment.content_type == Representation.EPUB_MEDIA_TYPE

    def test_fulfill_delivery_mechanism_error(
        self,
        opds_dist_api_fixture: OPDSForDistributorsAPIFixture,
    ):
        patron = opds_dist_api_fixture.db.patron()

        data_source = DataSource.lookup(
            opds_dist_api_fixture.db.session, "My Datasource", autocreate=True
        )

        # a drm_scheme that is neither NO_DRM nor BEARER_TOKEN
        # should fail.
        drm_scheme = DeliveryMechanism.LCP_DRM

        edition, pool = opds_dist_api_fixture.db.edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=opds_dist_api_fixture.collection,
        )
        pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE,
            drm_scheme,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        # Find the correct delivery mechanism
        delivery_mechanism = None
        for mechanism in pool.delivery_mechanisms:
            if mechanism.delivery_mechanism.drm_scheme == drm_scheme:
                delivery_mechanism = mechanism
        assert delivery_mechanism is not None

        # this call should fail because it is not a valid DeliveryMechanism
        # for an OPDS for Distributors feed.
        pytest.raises(
            DeliveryMechanismError,
            opds_dist_api_fixture.api.fulfill,
            patron,
            "1234",
            pool,
            delivery_mechanism,
        )
