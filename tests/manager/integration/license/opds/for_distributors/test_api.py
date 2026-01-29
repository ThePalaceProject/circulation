import json
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from palace.manager.api.circulation.exceptions import (
    CannotFulfill,
    DeliveryMechanismError,
)
from palace.manager.api.circulation.fulfillment import (
    DirectFulfillment,
    RedirectFulfillment,
    StreamingFulfillment,
)
from palace.manager.celery.tasks import opds_for_distributors
from palace.manager.integration.license.opds.for_distributors.api import (
    OPDSForDistributorsAPI,
)
from palace.manager.integration.license.opds.for_distributors.settings import (
    OPDSForDistributorsSettings,
)
from palace.manager.integration.license.opds.requests import OAuthOpdsRequest
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, RightsStatus
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDSForDistributorsFilesFixture
from tests.fixtures.http import MockHttpClientFixture


class OPDSForDistributorsAPIFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        files: OPDSForDistributorsFilesFixture,
        http_client: MockHttpClientFixture,
    ):
        self.db = db
        self.collection = self.mock_collection(db.default_library())
        self.api = OPDSForDistributorsAPI(db.session, self.collection)
        self.files = files
        self.http_client = http_client

    def mock_collection(
        self,
        library: Library | None = None,
        name: str = "Test OPDS For Distributors Collection",
    ) -> Collection:
        """Create a mock OPDS For Distributors collection to use in tests."""
        library = library or self.db.default_library()
        return self.db.collection(
            name,
            protocol=OPDSForDistributorsAPI,
            settings=OPDSForDistributorsSettings(
                username="a",
                password="b",
                data_source="data_source",
                external_account_id="http://opds",
            ),
            library=library,
        )


@pytest.fixture(scope="function")
def opds_dist_api_fixture(
    db: DatabaseTransactionFixture,
    opds_dist_files_fixture: OPDSForDistributorsFilesFixture,
    http_client: MockHttpClientFixture,
) -> OPDSForDistributorsAPIFixture:
    return OPDSForDistributorsAPIFixture(db, opds_dist_files_fixture, http_client)


class TestOPDSForDistributorsAPI:
    def test__run_self_tests(
        self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture
    ):
        """The self-test for OPDSForDistributorsAPI just tries to negotiate
        a fulfillment token.
        """
        api = opds_dist_api_fixture.api
        mock_make_request = create_autospec(OAuthOpdsRequest)
        api._make_request = mock_make_request
        [result] = api._run_self_tests(opds_dist_api_fixture.db.session)
        mock_make_request.refresh_token.assert_called_once_with()
        assert result.name == "Negotiate a fulfillment token"
        assert result.success is True
        assert result.result == mock_make_request.refresh_token.return_value

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

        # STREAMING_DRM is also allowed
        lpdm.delivery_mechanism.drm_scheme = DeliveryMechanism.STREAMING_DRM
        assert True == m(patron, pool, lpdm)

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

        # Set the API's token URL so it doesn't have to get it -
        # that's tested in TestOAuthOpdsRequest.
        opds_dist_api_fixture.api._make_request._token_url = "http://auth"

        token_response = json.dumps(
            {"access_token": "token", "expires_in": 60, "token_type": "Bearer"}
        )
        opds_dist_api_fixture.http_client.queue_response(200, content=token_response)

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

    def test_import_task(self) -> None:
        collection_id = MagicMock()
        force = MagicMock()
        with patch.object(opds_for_distributors, "import_collection") as mock_import:
            result = OPDSForDistributorsAPI.import_task(collection_id, force)

        mock_import.s.assert_called_once_with(collection_id, force=force)
        assert result == mock_import.s.return_value

    @pytest.mark.parametrize(
        "url,token,expected",
        [
            pytest.param(
                "http://example.com/viewer",
                "abc123",
                "http://example.com/viewer?token=abc123",
                id="simple url",
            ),
            pytest.param(
                "http://example.com/viewer?foo=bar",
                "abc123",
                "http://example.com/viewer?foo=bar&token=abc123",
                id="url with existing query param",
            ),
            pytest.param(
                "https://library.biblioboard.com/viewer/book/12345",
                "mytoken",
                "https://library.biblioboard.com/viewer/book/12345?token=mytoken",
                id="biblioboard style url",
            ),
        ],
    )
    def test__append_token_to_url(self, url: str, token: str, expected: str) -> None:
        """Test that _append_token_to_url correctly appends the token."""
        result = OPDSForDistributorsAPI._append_token_to_url(url, token)
        assert result == expected

    def test_fulfill_streaming(
        self,
        opds_dist_api_fixture: OPDSForDistributorsAPIFixture,
    ):
        """Test that fulfilling streaming content returns a StreamingFulfillment."""
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

        # Set up a streaming delivery mechanism
        pool.set_delivery_mechanism(
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        # Find the streaming delivery mechanism
        delivery_mechanism = None
        for mechanism in pool.delivery_mechanisms:
            if (
                mechanism.delivery_mechanism.drm_scheme
                == DeliveryMechanism.STREAMING_DRM
            ):
                delivery_mechanism = mechanism
        assert delivery_mechanism is not None

        # This pool doesn't have an acquisition link yet, so fulfillment should fail
        pytest.raises(
            CannotFulfill,
            opds_dist_api_fixture.api.fulfill,
            patron,
            "1234",
            pool,
            delivery_mechanism,
        )

        # Set up a streaming acquisition link
        viewer_url = "https://library.biblioboard.com/viewer/book/12345"
        link, ignore = pool.identifier.add_link(
            Hyperlink.GENERIC_OPDS_ACQUISITION,
            viewer_url,
            data_source,
            DeliveryMechanism.STREAMING_MEDIA_LINK_TYPE,
        )
        delivery_mechanism.resource = link.resource

        # Set the API's token URL so it doesn't have to get it
        opds_dist_api_fixture.api._make_request._token_url = "http://auth"

        token_response = json.dumps(
            {
                "access_token": "streaming_token",
                "expires_in": 60,
                "token_type": "Bearer",
            }
        )
        opds_dist_api_fixture.http_client.queue_response(200, content=token_response)

        fulfillment = opds_dist_api_fixture.api.fulfill(
            patron, "1234", pool, delivery_mechanism
        )

        # Verify we got a StreamingFulfillment
        assert isinstance(fulfillment, StreamingFulfillment)
        assert "token=streaming_token" in fulfillment.content_link
        assert fulfillment.content_link.startswith(viewer_url)
        # StreamingFulfillment appends the streaming profile to the content type
        assert fulfillment.content_type == (
            MediaTypes.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE
        )
