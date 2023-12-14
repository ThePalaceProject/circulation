import datetime
import json
from collections.abc import Callable
from unittest.mock import MagicMock, patch

import pytest

from api.circulation_exceptions import *
from api.opds_for_distributors import (
    OPDSForDistributorsAPI,
    OPDSForDistributorsImporter,
    OPDSForDistributorsImportMonitor,
    OPDSForDistributorsReaperMonitor,
)
from core.metadata_layer import CirculationData, LinkData
from core.model import (
    Collection,
    Credential,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    Loan,
    MediaTypes,
    Representation,
    RightsStatus,
    Timestamp,
    create,
    get_one,
)
from core.util.datetime_helpers import utc_now
from core.util.opds_writer import OPDSFeed
from tests.api.mockapi.opds_for_distributors import MockOPDSForDistributorsAPI
from tests.fixtures.api_opds_dist_files import OPDSForDistributorsFilesFixture
from tests.fixtures.database import DatabaseTransactionFixture


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


class OPDSForDistributorsAPIFixture:
    def __init__(
        self, db: DatabaseTransactionFixture, files: OPDSForDistributorsFilesFixture
    ):
        self.db = db
        self.collection = MockOPDSForDistributorsAPI.mock_collection(
            db.session, db.default_library()
        )
        self.api = MockOPDSForDistributorsAPI(db.session, self.collection)
        self.files = files


@pytest.fixture(scope="function")
def opds_dist_api_fixture(
    db: DatabaseTransactionFixture,
    api_opds_dist_files_fixture: OPDSForDistributorsFilesFixture,
) -> OPDSForDistributorsAPIFixture:
    return OPDSForDistributorsAPIFixture(db, api_opds_dist_files_fixture)


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
        collection1 = MockOPDSForDistributorsAPI.mock_collection(
            opds_dist_api_fixture.db.session,
            opds_dist_api_fixture.db.default_library(),
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
        collection2 = MockOPDSForDistributorsAPI.mock_collection(
            opds_dist_api_fixture.db.session,
            opds_dist_api_fixture.db.default_library(),
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

        other_collection = opds_dist_api_fixture.db.collection(
            protocol=ExternalIntegration.OVERDRIVE
        )
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
        assert data_source.name == loan_info.data_source_name
        assert Identifier.URI == loan_info.identifier_type
        assert pool.identifier.identifier == loan_info.identifier

        # The loan's start date has been set to the current time.
        now = utc_now()
        assert loan_info.start_date is not None
        assert (now - loan_info.start_date).seconds < 2

        # The loan is of indefinite duration.
        assert None == loan_info.end_date

    def test_fulfill(self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture):
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
            DeliveryMechanism.BEARER_TOKEN,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        # Find the correct delivery mechanism
        delivery_mechanism = None
        for mechanism in pool.delivery_mechanisms:
            if (
                mechanism.delivery_mechanism.drm_scheme
                == DeliveryMechanism.BEARER_TOKEN
            ):
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
            Hyperlink.GENERIC_OPDS_ACQUISITION,
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

        fulfillment_time = utc_now()
        fulfillment_info = opds_dist_api_fixture.api.fulfill(
            patron, "1234", pool, delivery_mechanism
        )
        assert opds_dist_api_fixture.collection.id == fulfillment_info.collection_id
        assert data_source.name == fulfillment_info.data_source_name
        assert Identifier.URI == fulfillment_info.identifier_type
        assert pool.identifier.identifier == fulfillment_info.identifier
        assert None == fulfillment_info.content_link

        assert DeliveryMechanism.BEARER_TOKEN == fulfillment_info.content_type
        assert fulfillment_info.content is not None
        bearer_token_document = json.loads(fulfillment_info.content)
        expires_in = bearer_token_document["expires_in"]
        assert expires_in < 60
        assert "Bearer" == bearer_token_document["token_type"]
        assert "token" == bearer_token_document["access_token"]
        assert url == bearer_token_document["location"]

        # The FulfillmentInfo's content_expires is approximately the
        # time you get if you add the number of seconds until the
        # bearer token expires to the time at which the title was
        # originally fulfilled.
        expect_expiration = fulfillment_time + datetime.timedelta(seconds=expires_in)
        assert fulfillment_info.content_expires is not None
        assert (
            abs((fulfillment_info.content_expires - expect_expiration).total_seconds())
            < 5
        )

    def test_patron_activity(
        self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture
    ):
        # The patron has two loans from this API's collection and
        # one from a different collection.
        patron = opds_dist_api_fixture.db.patron()

        data_source = DataSource.lookup(
            opds_dist_api_fixture.db.session, "Biblioboard", autocreate=True
        )
        e1, p1 = opds_dist_api_fixture.db.edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=opds_dist_api_fixture.collection,
        )
        p1.loan_to(patron)

        e2, p2 = opds_dist_api_fixture.db.edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=opds_dist_api_fixture.collection,
        )
        p2.loan_to(patron)

        other_collection = opds_dist_api_fixture.db.collection(
            protocol=ExternalIntegration.OVERDRIVE
        )
        e3, p3 = opds_dist_api_fixture.db.edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=other_collection,
        )
        p3.loan_to(patron)

        activity = opds_dist_api_fixture.api.patron_activity(patron, "1234")
        assert 2 == len(activity)
        [l1, l2] = activity
        assert l1.collection_id == opds_dist_api_fixture.collection.id
        assert l2.collection_id == opds_dist_api_fixture.collection.id
        assert {l1.identifier, l2.identifier} == {
            p1.identifier.identifier,
            p2.identifier.identifier,
        }


class TestOPDSForDistributorsImporter:
    def test_import(self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture):
        feed = opds_dist_api_fixture.files.sample_data("biblioboard_mini_feed.opds")

        data_source = DataSource.lookup(
            opds_dist_api_fixture.db.session, "Biblioboard", autocreate=True
        )
        collection = MockOPDSForDistributorsAPI.mock_collection(
            opds_dist_api_fixture.db.session, opds_dist_api_fixture.db.default_library()
        )
        DatabaseTransactionFixture.set_settings(
            collection.integration_configuration,
            **{Collection.DATA_SOURCE_NAME_SETTING: data_source.name}
        )

        importer = OPDSForDistributorsImporter(
            opds_dist_api_fixture.db.session,
            collection=collection,
        )

        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = importer.import_from_feed(feed)

        # This importer works the same as the base OPDSImporter, except that
        # it adds delivery mechanisms for books with epub acquisition links
        # and sets pools' licenses_owned and licenses_available.

        # Both works were created, since we can use their acquisition links
        # to give copies to patrons.
        [camelot, camelot_audio, southern] = sorted(
            imported_works, key=lambda x: x.title
        )

        # Each work has a license pool.
        [camelot_pool] = camelot.license_pools
        [southern_pool] = southern.license_pools
        now = utc_now()

        for pool in [camelot_pool, southern_pool]:
            assert False == pool.open_access
            assert (
                RightsStatus.IN_COPYRIGHT
                == pool.delivery_mechanisms[0].rights_status.uri
            )
            assert (
                Representation.EPUB_MEDIA_TYPE
                == pool.delivery_mechanisms[0].delivery_mechanism.content_type
            )
            assert (
                DeliveryMechanism.BEARER_TOKEN
                == pool.delivery_mechanisms[0].delivery_mechanism.drm_scheme
            )
            assert LicensePool.UNLIMITED_ACCESS == pool.licenses_owned
            assert LicensePool.UNLIMITED_ACCESS == pool.licenses_available
            assert (pool.work.last_update_time - now).total_seconds() <= 2
            assert pool.should_track_playtime == False

        # Audiobooks always track playtime
        camelot_audio_pool = camelot_audio.license_pools[0]
        assert camelot_audio_pool.should_track_playtime == True
        [camelot_audio_acquisition_link] = [
            l
            for l in camelot_audio_pool.identifier.links
            if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
            and l.resource.representation.media_type
            == Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
        ]
        assert (
            "https://library.biblioboard.com/ext/api/media/04377e87-ab69-41c8-a2a4-812d55dc0953/assets/content.json"
            == camelot_audio_acquisition_link.resource.representation.url
        )

        [camelot_acquisition_link] = [
            l
            for l in camelot_pool.identifier.links
            if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
            and l.resource.representation.media_type == Representation.EPUB_MEDIA_TYPE
        ]
        camelot_acquisition_url = camelot_acquisition_link.resource.representation.url
        assert (
            "https://library.biblioboard.com/ext/api/media/04377e87-ab69-41c8-a2a4-812d55dc0952/assets/content.epub"
            == camelot_acquisition_url
        )

        [southern_acquisition_link] = [
            l
            for l in southern_pool.identifier.links
            if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
            and l.resource.representation.media_type == Representation.EPUB_MEDIA_TYPE
        ]
        southern_acquisition_url = southern_acquisition_link.resource.representation.url
        assert (
            "https://library.biblioboard.com/ext/api/media/04da95cd-6cfc-4e82-810f-121d418b6963/assets/content.epub"
            == southern_acquisition_url
        )

    def test__add_format_data(
        self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture
    ):
        # Mock SUPPORTED_MEDIA_TYPES for purposes of test.
        api = OPDSForDistributorsAPI
        old_value = api.SUPPORTED_MEDIA_TYPES
        good_media_type = "media/type"
        api.SUPPORTED_MEDIA_TYPES = [good_media_type]

        # Create a CirculationData object with a number of links.
        # Only the third of these links will become a FormatData
        # object.
        circulation = CirculationData("data source", "identifier")
        good_rel = Hyperlink.GENERIC_OPDS_ACQUISITION
        for rel, media, href in (
            ("http://wrong/rel/", good_media_type, "http://url1/"),
            (good_rel, "wrong/media type", "http://url2/"),
            (good_rel, good_media_type, "http://url3/"),
        ):
            link = LinkData(rel=rel, href=href, media_type=media)
            circulation.links.append(link)

        assert [] == circulation.formats
        OPDSForDistributorsImporter._add_format_data(circulation)

        # Only one FormatData was created.
        [format] = circulation.formats

        # It's the third link we created -- the one where both rel and
        # media_type were good.
        assert "http://url3/" == format.link.href
        assert good_rel == format.link.rel

        # The FormatData has the content type provided by the LinkData,
        # and the implicit Bearer Token access control scheme defined
        # by OPDS For Distrubutors.
        assert good_media_type == format.content_type
        assert DeliveryMechanism.BEARER_TOKEN == format.drm_scheme

        # Undo the mock of SUPPORTED_MEDIA_TYPES.
        api.SUPPORTED_MEDIA_TYPES = old_value

    def test_update_work_for_edition_returns_correct_license_pool(
        self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture
    ):
        # If there are two or more collections, `update_work_for_edition`
        # should return the license pool for the right one.
        data_source_name = "BiblioBoard"
        data_source = DataSource.lookup(
            opds_dist_api_fixture.db.session, data_source_name, autocreate=True
        )

        def setup_collection(*, name: str, datasource: DataSource) -> Collection:
            collection = MockOPDSForDistributorsAPI.mock_collection(
                opds_dist_api_fixture.db.session,
                opds_dist_api_fixture.db.default_library(),
                name=name,
            )
            DatabaseTransactionFixture.set_settings(
                collection.integration_configuration,
                **{Collection.DATA_SOURCE_NAME_SETTING: data_source.name}
            )
            return collection

        collection1 = setup_collection(name="Test Collection 1", datasource=data_source)
        collection2 = setup_collection(name="Test Collection 2", datasource=data_source)

        work = opds_dist_api_fixture.db.work(
            with_license_pool=False,
            collection=collection1,
            data_source_name=data_source_name,
        )
        edition = work.presentation_edition

        collection1_lp = opds_dist_api_fixture.db.licensepool(
            edition=edition, collection=collection1, set_edition_as_presentation=True
        )
        collection2_lp = opds_dist_api_fixture.db.licensepool(
            edition=edition, collection=collection2, set_edition_as_presentation=True
        )
        importer1 = OPDSForDistributorsImporter(
            opds_dist_api_fixture.db.session,
            collection=collection1,
        )
        importer2 = OPDSForDistributorsImporter(
            opds_dist_api_fixture.db.session,
            collection=collection2,
        )

        with patch("core.opds_import.get_one", wraps=get_one) as get_one_mock:
            importer1_lp, _ = importer1.update_work_for_edition(edition)
            importer2_lp, _ = importer2.update_work_for_edition(edition)

        # Ensure distinct collections.
        assert collection1_lp != collection2_lp
        assert collection1_lp.collection.name == "Test Collection 1"
        assert collection2_lp.collection.name == "Test Collection 2"

        # The license pool returned to the importer should be the
        # same one originally created for a given collection.
        assert collection1_lp == importer1_lp
        assert collection2_lp == importer2_lp

        # With OPDS for Distributors imports, `update_work_for_edition`
        # should include `collection` in the license pool lookup criteria.
        assert 2 == len(get_one_mock.call_args_list)
        for call_args in get_one_mock.call_args_list:
            assert "collection" in call_args.kwargs


class TestOPDSForDistributorsReaperMonitor:
    def test_reaper(self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture):
        feed = opds_dist_api_fixture.files.sample_data("biblioboard_mini_feed.opds")

        class MockOPDSForDistributorsReaperMonitor(OPDSForDistributorsReaperMonitor):
            """An OPDSForDistributorsReaperMonitor that overrides _get."""

            def _get(self, url, headers):
                return (200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, feed)

        data_source = DataSource.lookup(
            opds_dist_api_fixture.db.session, "Biblioboard", autocreate=True
        )
        collection = MockOPDSForDistributorsAPI.mock_collection(
            opds_dist_api_fixture.db.session,
            opds_dist_api_fixture.db.default_library(),
        )
        DatabaseTransactionFixture.set_settings(
            collection.integration_configuration,
            **{Collection.DATA_SOURCE_NAME_SETTING: data_source.name}
        )
        monitor = MockOPDSForDistributorsReaperMonitor(
            opds_dist_api_fixture.db.session,
            collection,
            OPDSForDistributorsImporter,
        )

        # There's a license pool in the database that isn't in the feed anymore.
        edition, now_gone = opds_dist_api_fixture.db.edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=collection,
        )
        now_gone.licenses_owned = LicensePool.UNLIMITED_ACCESS
        now_gone.licenses_available = LicensePool.UNLIMITED_ACCESS

        edition, still_there = opds_dist_api_fixture.db.edition(
            identifier_type=Identifier.URI,
            identifier_id="urn:uuid:04377e87-ab69-41c8-a2a4-812d55dc0952",
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=collection,
        )
        still_there.licenses_owned = LicensePool.UNLIMITED_ACCESS
        still_there.licenses_available = LicensePool.UNLIMITED_ACCESS

        progress = monitor.run_once(monitor.timestamp().to_data())

        # One LicensePool has been cleared out.
        assert 0 == now_gone.licenses_owned
        assert 0 == now_gone.licenses_available

        # The other is still around.
        assert LicensePool.UNLIMITED_ACCESS == still_there.licenses_owned
        assert LicensePool.UNLIMITED_ACCESS == still_there.licenses_available

        # The TimestampData returned by run_once() describes its
        # achievements.
        assert "License pools removed: 1." == progress.achievements

        # The TimestampData does not include any timing information --
        # that will be applied by run().
        assert None == progress.start
        assert None == progress.finish


class TestOPDSForDistributorsImportMonitor:
    def test_opds_import_has_db_failure(
        self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture
    ):
        feed = opds_dist_api_fixture.files.sample_data("biblioboard_mini_feed.opds")

        class MockOPDSForDistributorsImportMonitor(OPDSForDistributorsImportMonitor):
            """An OPDSForDistributorsImportMonitor that overrides _get."""

            def _get(self, url, headers):
                # This should cause a database failure on commit
                ts = create(self._db, Timestamp)
                return (200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, feed)

        data_source = DataSource.lookup(
            opds_dist_api_fixture.db.session, "Biblioboard", autocreate=True
        )
        collection = MockOPDSForDistributorsAPI.mock_collection(
            opds_dist_api_fixture.db.session,
            opds_dist_api_fixture.db.default_library(),
        )
        DatabaseTransactionFixture.set_settings(
            collection.integration_configuration,
            **{Collection.DATA_SOURCE_NAME_SETTING: data_source.name}
        )
        monitor = MockOPDSForDistributorsImportMonitor(
            opds_dist_api_fixture.db.session,
            collection,
            OPDSForDistributorsImporter,
        )

        monitor.run()

        assert monitor.timestamp().exception is not None
