import datetime
import io
from typing import List, Union
from unittest.mock import MagicMock, patch

import pytest
from requests import Response
from webpub_manifest_parser.opds2 import OPDS2FeedParserFactory

from api.circulation import CirculationAPI
from api.circulation_exceptions import CannotFulfill
from core.model import (
    ConfigurationSetting,
    Contribution,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    EditionConstants,
    ExternalIntegration,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Loan,
    MediaTypes,
    Work,
)
from core.model.collection import Collection
from core.model.constants import IdentifierType
from core.opds2_import import OPDS2API, OPDS2Importer, RWPMManifestParser
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.opds2_files import OPDS2FilesFixture


class OPDS2Test:
    @staticmethod
    def _get_edition_by_identifier(editions, identifier):
        """Find an edition in the list by its identifier.

        :param editions: List of editions
        :type editions: List[Edition]

        :return: Edition with the specified id (if any)
        :rtype: Optional[Edition]
        """
        for edition in editions:
            if edition.primary_identifier.urn == identifier:
                return edition

        return None

    @staticmethod
    def _get_license_pool_by_identifier(pools, identifier):
        """Find a license pool in the list by its identifier.

        :param pools: List of license pools
        :type pools: List[LicensePool]

        :return: Edition with the specified id (if any)
        :rtype: Optional[LicensePool]
        """
        for pool in pools:
            if pool.identifier.urn == identifier:
                return pool

        return None

    @staticmethod
    def _get_work_by_identifier(works, identifier):
        """Find a license pool in the list by its identifier.

        :param works: List of license pools
        :type works: List[Work]

        :return: Edition with the specified id (if any)
        :rtype: Optional[Work]
        """
        for work in works:
            if work.presentation_edition.primary_identifier.urn == identifier:
                return work

        return None


class TestOPDS2ImporterFixture:
    transaction: DatabaseTransactionFixture
    collection: Collection
    data_source: DataSource
    importer: OPDS2Importer


@pytest.fixture
def opds2_importer_fixture(
    db: DatabaseTransactionFixture,
) -> TestOPDS2ImporterFixture:
    data = TestOPDS2ImporterFixture()
    data.transaction = db
    data.collection = db.collection(protocol=OPDS2API.label())
    data.library = db.default_library()
    data.library.collections.append(data.collection)
    data.data_source = DataSource.lookup(
        db.session, "OPDS 2.0 Data Source", autocreate=True
    )
    data.collection.data_source = data.data_source
    data.importer = OPDS2Importer(
        db.session, data.collection, RWPMManifestParser(OPDS2FeedParserFactory())
    )
    return data


class TestOPDS2Importer(OPDS2Test):
    MOBY_DICK_ISBN_IDENTIFIER = "urn:isbn:978-3-16-148410-0"
    HUCKLEBERRY_FINN_URI_IDENTIFIER = "http://example.org/huckleberry-finn"
    POSTMODERNISM_PROQUEST_IDENTIFIER = (
        "urn:librarysimplified.org/terms/id/ProQuest%20Doc%20ID/181639"
    )

    @pytest.mark.parametrize(
        "name,manifest_type",
        [
            ("manifest encoded as a string", "string"),
            ("manifest encoded as a byte-string", "bytes"),
        ],
    )
    def test_opds2_importer_correctly_imports_valid_opds2_feed(
        self,
        opds2_importer_fixture: TestOPDS2ImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
        name: str,
        manifest_type: str,
    ):
        """Ensure that OPDS2Importer correctly imports valid OPDS 2.x feeds.
        :param manifest_type: Manifest's type: string or binary
        """
        # Arrange
        data, transaction, session = (
            opds2_importer_fixture,
            opds2_importer_fixture.transaction,
            opds2_importer_fixture.transaction.session,
        )
        content_server_feed_text = opds2_files_fixture.sample_text("feed.json")
        content_server_feed: Union[str, bytes]

        if manifest_type == "bytes":
            content_server_feed = content_server_feed_text.encode()
        else:
            content_server_feed = content_server_feed_text

        # Act
        imported_editions, pools, works, failures = data.importer.import_from_feed(
            content_server_feed
        )

        # Assert

        # 1. Make sure that editions contain all required metadata
        assert isinstance(imported_editions, list)
        assert 3 == len(imported_editions)

        # 1.1. Edition with open-access links (Moby-Dick)
        moby_dick_edition = self._get_edition_by_identifier(
            imported_editions, self.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_edition, Edition)

        assert "Moby-Dick" == moby_dick_edition.title
        assert "eng" == moby_dick_edition.language
        assert "eng" == moby_dick_edition.language
        assert EditionConstants.BOOK_MEDIUM == moby_dick_edition.medium
        assert "Herman Melville" == moby_dick_edition.author

        assert 1 == len(moby_dick_edition.author_contributors)
        [moby_dick_author] = moby_dick_edition.author_contributors
        assert isinstance(moby_dick_author, Contributor)
        assert "Herman Melville" == moby_dick_author.display_name
        assert "Melville, Herman" == moby_dick_author.sort_name

        assert 1 == len(moby_dick_author.contributions)
        [moby_dick_author_contribution] = moby_dick_author.contributions
        assert isinstance(moby_dick_author_contribution, Contribution)
        assert moby_dick_author == moby_dick_author_contribution.contributor
        assert moby_dick_edition == moby_dick_author_contribution.edition
        assert Contributor.AUTHOR_ROLE == moby_dick_author_contribution.role

        assert data.data_source == moby_dick_edition.data_source

        assert "Test Publisher" == moby_dick_edition.publisher
        assert datetime.date(2015, 9, 29) == moby_dick_edition.published

        assert "http://example.org/cover.jpg" == moby_dick_edition.cover_full_url
        assert (
            "http://example.org/cover-small.jpg"
            == moby_dick_edition.cover_thumbnail_url
        )

        # 1.2. Edition with non open-access acquisition links (Adventures of Huckleberry Finn)
        huckleberry_finn_edition = self._get_edition_by_identifier(
            imported_editions, self.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_edition, Edition)

        assert "Adventures of Huckleberry Finn" == huckleberry_finn_edition.title
        assert "eng" == huckleberry_finn_edition.language
        assert EditionConstants.BOOK_MEDIUM == huckleberry_finn_edition.medium
        assert "Samuel Langhorne Clemens, Mark Twain" == huckleberry_finn_edition.author

        assert 2 == len(huckleberry_finn_edition.author_contributors)
        huckleberry_finn_authors = huckleberry_finn_edition.author_contributors

        assert isinstance(huckleberry_finn_authors[0], Contributor)
        assert "Mark Twain" == huckleberry_finn_authors[0].display_name
        assert "Twain, Mark" == huckleberry_finn_authors[0].sort_name

        assert 1 == len(huckleberry_finn_authors[0].contributions)
        [huckleberry_finn_author_contribution] = huckleberry_finn_authors[
            0
        ].contributions
        assert isinstance(huckleberry_finn_author_contribution, Contribution)
        assert (
            huckleberry_finn_authors[0]
            == huckleberry_finn_author_contribution.contributor
        )
        assert huckleberry_finn_edition == huckleberry_finn_author_contribution.edition
        assert Contributor.AUTHOR_ROLE == huckleberry_finn_author_contribution.role

        assert isinstance(huckleberry_finn_authors[1], Contributor)
        assert "Samuel Langhorne Clemens" == huckleberry_finn_authors[1].display_name
        assert "Clemens, Samuel Langhorne" == huckleberry_finn_authors[1].sort_name

        assert 1 == len(huckleberry_finn_authors[1].contributions)
        [huckleberry_finn_author_contribution] = huckleberry_finn_authors[
            1
        ].contributions
        assert isinstance(huckleberry_finn_author_contribution, Contribution)
        assert (
            huckleberry_finn_authors[1]
            == huckleberry_finn_author_contribution.contributor
        )
        assert huckleberry_finn_edition == huckleberry_finn_author_contribution.edition
        assert Contributor.AUTHOR_ROLE == huckleberry_finn_author_contribution.role

        assert data.data_source == huckleberry_finn_edition.data_source

        assert "Test Publisher" == huckleberry_finn_edition.publisher
        assert datetime.date(2014, 9, 28) == huckleberry_finn_edition.published

        assert "http://example.org/cover.jpg" == moby_dick_edition.cover_full_url

        # 2. Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 3 == len(pools)

        # 2.1. Edition with open-access links (Moby-Dick)
        moby_dick_license_pool = self._get_license_pool_by_identifier(
            pools, self.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert moby_dick_license_pool.open_access
        assert LicensePool.UNLIMITED_ACCESS == moby_dick_license_pool.licenses_owned
        assert LicensePool.UNLIMITED_ACCESS == moby_dick_license_pool.licenses_available

        assert 1 == len(moby_dick_license_pool.delivery_mechanisms)
        [moby_dick_delivery_mechanism] = moby_dick_license_pool.delivery_mechanisms
        assert (
            DeliveryMechanism.NO_DRM
            == moby_dick_delivery_mechanism.delivery_mechanism.drm_scheme
        )
        assert (
            MediaTypes.EPUB_MEDIA_TYPE
            == moby_dick_delivery_mechanism.delivery_mechanism.content_type
        )

        # 2.2. Edition with non open-access acquisition links (Adventures of Huckleberry Finn)
        huckleberry_finn_license_pool = self._get_license_pool_by_identifier(
            pools, self.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert True == isinstance(huckleberry_finn_license_pool, LicensePool)
        assert False == huckleberry_finn_license_pool.open_access
        assert (
            LicensePool.UNLIMITED_ACCESS == huckleberry_finn_license_pool.licenses_owned
        )
        assert (
            LicensePool.UNLIMITED_ACCESS
            == huckleberry_finn_license_pool.licenses_available
        )

        assert 2 == len(huckleberry_finn_license_pool.delivery_mechanisms)
        huckleberry_finn_delivery_mechanisms = (
            huckleberry_finn_license_pool.delivery_mechanisms
        )

        assert (
            DeliveryMechanism.ADOBE_DRM
            == huckleberry_finn_delivery_mechanisms[0].delivery_mechanism.drm_scheme
        )
        assert (
            MediaTypes.EPUB_MEDIA_TYPE
            == huckleberry_finn_delivery_mechanisms[0].delivery_mechanism.content_type
        )

        assert (
            DeliveryMechanism.LCP_DRM
            == huckleberry_finn_delivery_mechanisms[1].delivery_mechanism.drm_scheme
        )
        assert (
            MediaTypes.EPUB_MEDIA_TYPE
            == huckleberry_finn_delivery_mechanisms[1].delivery_mechanism.content_type
        )

        # 2.3 Edition with non open-access acquisition links (The Politics of Postmodernism)
        postmodernism_license_pool = self._get_license_pool_by_identifier(
            pools, self.POSTMODERNISM_PROQUEST_IDENTIFIER
        )
        assert True == isinstance(postmodernism_license_pool, LicensePool)
        assert False == postmodernism_license_pool.open_access
        assert LicensePool.UNLIMITED_ACCESS == postmodernism_license_pool.licenses_owned
        assert (
            LicensePool.UNLIMITED_ACCESS
            == postmodernism_license_pool.licenses_available
        )

        assert 2 == len(postmodernism_license_pool.delivery_mechanisms)
        postmodernism_delivery_mechanisms = (
            postmodernism_license_pool.delivery_mechanisms
        )

        assert (
            DeliveryMechanism.ADOBE_DRM
            == postmodernism_delivery_mechanisms[0].delivery_mechanism.drm_scheme
        )
        assert (
            MediaTypes.EPUB_MEDIA_TYPE
            == postmodernism_delivery_mechanisms[0].delivery_mechanism.content_type
        )

        assert (
            DeliveryMechanism.ADOBE_DRM
            == postmodernism_delivery_mechanisms[1].delivery_mechanism.drm_scheme
        )
        assert (
            MediaTypes.PDF_MEDIA_TYPE
            == postmodernism_delivery_mechanisms[1].delivery_mechanism.content_type
        )

        # 3. Make sure that work objects contain all the required metadata
        assert isinstance(works, list)
        assert 3 == len(works)

        # 3.1. Work (Moby-Dick)
        moby_dick_work = self._get_work_by_identifier(
            works, self.MOBY_DICK_ISBN_IDENTIFIER
        )
        assert isinstance(moby_dick_work, Work)
        assert moby_dick_edition == moby_dick_work.presentation_edition
        assert 1 == len(moby_dick_work.license_pools)
        assert moby_dick_license_pool == moby_dick_work.license_pools[0]

        # 3.2. Work (Adventures of Huckleberry Finn)
        huckleberry_finn_work = self._get_work_by_identifier(
            works, self.HUCKLEBERRY_FINN_URI_IDENTIFIER
        )
        assert isinstance(huckleberry_finn_work, Work)
        assert huckleberry_finn_edition == huckleberry_finn_work.presentation_edition
        assert 1 == len(huckleberry_finn_work.license_pools)
        assert huckleberry_finn_license_pool == huckleberry_finn_work.license_pools[0]
        assert (
            "Adventures of Huckleberry Finn is a novel by Mark Twain, first published in the United Kingdom in "
            "December 1884 and in the United States in February 1885."
            == huckleberry_finn_work.summary_text
        )

    @pytest.mark.parametrize(
        "this_identifier_type,ignore_identifier_type,identifier",
        [
            (
                IdentifierType.ISBN,
                [IdentifierType.URI, IdentifierType.PROQUEST_ID],
                MOBY_DICK_ISBN_IDENTIFIER,
            ),
            (
                IdentifierType.URI,
                [IdentifierType.ISBN, IdentifierType.PROQUEST_ID],
                HUCKLEBERRY_FINN_URI_IDENTIFIER,
            ),
            (
                IdentifierType.PROQUEST_ID,
                [IdentifierType.ISBN, IdentifierType.URI],
                POSTMODERNISM_PROQUEST_IDENTIFIER,
            ),
        ],
    )
    def test_opds2_importer_skips_publications_with_unsupported_identifier_types(
        self,
        opds2_importer_fixture: TestOPDS2ImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
        this_identifier_type,
        ignore_identifier_type: List[IdentifierType],
        identifier: str,
    ) -> None:
        """Ensure that OPDS2Importer imports only publications having supported identifier types.
        This test imports the feed consisting of two publications,
        each having a different identifier type: ISBN and URI.
        First, it tries to import the feed marking ISBN as the only supported identifier type. Secondly, it uses URI.
        Each time it checks that CM imported only the publication having the selected identifier type.
        """
        data, transaction, session = (
            opds2_importer_fixture,
            opds2_importer_fixture.transaction,
            opds2_importer_fixture.transaction.session,
        )

        # Arrange
        # Update the list of supported identifier types in the collection's configuration settings
        # and set the identifier type passed as a parameter as the only supported identifier type.
        data.importer.ignored_identifier_types = [
            t.value for t in ignore_identifier_type
        ]

        content_server_feed = opds2_files_fixture.sample_text("feed.json")

        # Act
        imported_editions, pools, works, failures = data.importer.import_from_feed(
            content_server_feed
        )

        # Assert

        # Ensure that that CM imported only the edition having the selected identifier type.
        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)
        assert (
            imported_editions[0].primary_identifier.type == this_identifier_type.value
        )

        # Ensure that it was parsed correctly and available by its identifier.
        edition = self._get_edition_by_identifier(imported_editions, identifier)
        assert edition is not None

    def test_auth_token_feed(
        self,
        opds2_importer_fixture: TestOPDS2ImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        data, transaction, session = (
            opds2_importer_fixture,
            opds2_importer_fixture.transaction,
            opds2_importer_fixture.transaction.session,
        )

        content = opds2_files_fixture.sample_text("auth_token_feed.json")
        imported_editions, pools, works, failures = data.importer.import_from_feed(
            content
        )
        setting = ConfigurationSetting.for_externalintegration(
            ExternalIntegration.TOKEN_AUTH, data.collection.external_integration
        )

        # Did the token endpoint get stored correctly?
        assert setting.value == "http://example.org/auth?userName={patron_id}"


class TestOpds2Api:
    def test_opds2_with_authentication_tokens(
        self,
        db: DatabaseTransactionFixture,
        opds2_importer_fixture: TestOPDS2ImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        """Test the end to end workflow from importing the feed to a fulfill"""
        content = opds2_files_fixture.sample_text("auth_token_feed.json")
        (
            imported_editions,
            pools,
            works,
            failures,
        ) = opds2_importer_fixture.importer.import_from_feed(content)

        work = works[0]

        api = CirculationAPI(db.session, db.default_library())
        patron = db.patron()

        # Borrow the book from the library
        api.borrow(patron, "pin", work.license_pools[0], MagicMock(), None)

        loans = db.session.query(Loan).filter(Loan.patron == patron)
        assert loans.count() == 1

        loan = loans.first()
        assert isinstance(loan, Loan)

        epub_mechanism = None
        for mechanism in loan.license_pool.delivery_mechanisms:
            if mechanism.delivery_mechanism.content_type == "application/epub+zip":
                epub_mechanism = mechanism
                break

        assert epub_mechanism is not None

        # Fulfill (Download) the book, should redirect to an authenticated URL
        with patch.object(OPDS2API, "get_authentication_token") as mock_auth:
            mock_auth.return_value = "plaintext-token"
            fulfillment = api.fulfill(
                patron, "pin", work.license_pools[0], epub_mechanism
            )

        assert (
            fulfillment.content_link
            == "http://example.org//getDrmFreeFile.action?documentId=1543720&mediaType=epub&authToken=plaintext-token"
        )
        assert fulfillment.content_type == "application/epub+zip"
        assert fulfillment.content is None
        assert fulfillment.content_expires is None
        assert fulfillment.content_link_redirect is True

    @patch("core.opds2_import.HTTP")
    def test_fulfill(self, mock_http: MagicMock, db: DatabaseTransactionFixture):
        patron = db.patron()
        collection: Collection = db.collection(
            protocol=ExternalIntegration.OPDS2_IMPORT
        )
        integration: ExternalIntegration = collection.create_external_integration(
            ExternalIntegration.OPDS2_IMPORT
        )
        setting: ConfigurationSetting = ConfigurationSetting.for_externalintegration(
            ExternalIntegration.TOKEN_AUTH, integration
        )
        setting.value = "http://example.org/token?userName={patron_id}"

        resp = Response()
        resp.status_code = 200
        resp.raw = io.BytesIO(b"plaintext-auth-token")
        mock_http.get_with_timeout.return_value = resp

        data_source = DataSource.lookup(db.session, "test", autocreate=True)

        pool = MagicMock(spec=LicensePool)
        mechanism = MagicMock(spec=LicensePoolDeliveryMechanism)
        pool.delivery_mechanisms = [mechanism]
        pool.data_source = data_source
        mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill?authToken={authentication_token}"
        )

        api = OPDS2API(db.session, collection)
        ff_info = api.fulfill(patron, "", pool, mechanism)

        patron_id = patron.identifier_to_remote_service(data_source)

        assert mock_http.get_with_timeout.call_count == 1
        assert (
            mock_http.get_with_timeout.call_args[0][0]
            == f"http://example.org/token?userName={patron_id}"
        )

        assert (
            ff_info.content_link
            == "http://example.org/11234/fulfill?authToken=plaintext-auth-token"
        )
        assert ff_info.content_link_redirect == True

        # Alternative templating
        mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill{?authentication_token}"
        )
        ff_info = api.fulfill(patron, "", pool, mechanism)

        assert (
            ff_info.content_link
            == "http://example.org/11234/fulfill?authentication_token=plaintext-auth-token"
        )

        ## Test error case
        # non-200 response
        resp = Response()
        resp.status_code = 400
        mock_http.reset_mock()
        mock_http.get_with_timeout.return_value = resp
        with pytest.raises(CannotFulfill):
            api.fulfill(patron, "", pool, mechanism)

        ## Pass through cases
        # No templating in the url
        mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill?authToken=authentication_token"
        )
        ff_info = api.fulfill(patron, "", pool, mechanism)
        assert ff_info.content_link_redirect is False
        assert ff_info.content_link == mechanism.resource.representation.public_url

        # No token endpoint config
        mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill?authToken={authentication_token}"
        )
        api.token_auth_configuration = None
        ff_info = api.fulfill(patron, "", pool, mechanism)
        assert ff_info.content_link_redirect is False
        assert ff_info.content_link == mechanism.resource.representation.public_url

    @patch("core.opds2_import.HTTP")
    def test_get_authentication_token(self, mock_http, db: DatabaseTransactionFixture):
        resp = Response()
        resp.status_code = 200
        resp.raw = io.BytesIO(b"plaintext-auth-token")
        mock_http.get_with_timeout.return_value = resp
        patron = db.patron()
        datasource = DataSource.lookup(db.session, "test", autocreate=True)
        token = OPDS2API.get_authentication_token(
            patron, datasource, "http://example.org/token"
        )

        assert token == "plaintext-auth-token"
        assert mock_http.get_with_timeout.call_count == 1

    @patch("core.opds2_import.HTTP")
    def test_get_authentication_token_errors(
        self, mock_http: MagicMock, db: DatabaseTransactionFixture
    ):
        resp = Response()
        resp.status_code = 400
        mock_http.get_with_timeout.return_value = resp
        datasource = DataSource.lookup(db.session, "test", autocreate=True)
        with pytest.raises(CannotFulfill):
            OPDS2API.get_authentication_token(
                db.patron(), datasource, "http://example.org/token"
            )
