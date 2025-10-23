import datetime
from copy import deepcopy

import pytest

from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.license import LicenseData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.data_layer.subject import SubjectData
from palace.manager.opds.odl.info import LicenseStatus
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestCirculationData:
    def test_circulationdata_may_require_collection(
        self, db: DatabaseTransactionFixture
    ):
        """Depending on the information provided in a CirculationData
        object, it might or might not be possible to call apply()
        without providing a Collection.
        """
        identifier = IdentifierData(type=Identifier.OVERDRIVE_ID, identifier="1")
        format = FormatData(
            content_type=Representation.EPUB_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.NO_DRM,
            rights_uri=RightsStatus.IN_COPYRIGHT,
        )
        circdata = CirculationData(
            data_source_name=DataSource.OVERDRIVE,
            primary_identifier_data=identifier,
            formats=[format],
        )
        circdata.apply(db.session, collection=None)

        # apply() has created a LicensePoolDeliveryMechanism for this
        # title, even though there are no LicensePools for it.
        identifier_obj, ignore = identifier.load(db.session)
        assert [] == identifier_obj.licensed_through
        [lpdm] = identifier_obj.delivery_mechanisms
        assert DataSource.OVERDRIVE == lpdm.data_source.name
        assert RightsStatus.IN_COPYRIGHT == lpdm.rights_status.uri

        mechanism = lpdm.delivery_mechanism
        assert Representation.EPUB_MEDIA_TYPE == mechanism.content_type
        assert DeliveryMechanism.NO_DRM == mechanism.drm_scheme

        # But if we put some information in the CirculationData
        # that can only be stored in a LicensePool, there's trouble.
        circdata.licenses_owned = 0
        with pytest.raises(ValueError) as excinfo:
            circdata.apply(db.session, collection=None)
        assert (
            "Cannot store circulation information because no Collection was provided."
            in str(excinfo.value)
        )

    def test_circulationdata_can_be_deepcopied(self):
        # Check that we didn't put something in the CirculationData that
        # will prevent it from being copied. (e.g., self.log)

        subject = SubjectData(type=Subject.TAG, identifier="subject")
        contributor = ContributorData()
        identifier = IdentifierData(type=Identifier.GUTENBERG_ID, identifier="1")
        link = LinkData(rel=Hyperlink.OPEN_ACCESS_DOWNLOAD, href="example.epub")
        format = FormatData(
            content_type=Representation.EPUB_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.NO_DRM,
        )
        rights_uri = RightsStatus.GENERIC_OPEN_ACCESS

        circulation_data = CirculationData(
            data_source_name=DataSource.GUTENBERG,
            primary_identifier_data=identifier,
            links=[link],
            licenses_owned=5,
            licenses_available=5,
            licenses_reserved=None,
            patrons_in_hold_queue=None,
            formats=[format],
            default_rights_uri=rights_uri,
        )

        circulation_data_copy = deepcopy(circulation_data)

        # If deepcopy didn't throw an exception we're ok.
        assert circulation_data_copy is not None

    def test_links_filtered(self):
        # Tests that passed-in links filter down to only the relevant ones.
        link1 = LinkData(rel=Hyperlink.OPEN_ACCESS_DOWNLOAD, href="example.epub")
        link2 = LinkData(rel=Hyperlink.IMAGE, href="http://example.com/")
        link3 = LinkData(rel=Hyperlink.DESCRIPTION, content="foo")
        link4 = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE,
            href="http://thumbnail.com/",
            media_type=Representation.JPEG_MEDIA_TYPE,
        )
        link5 = LinkData(
            rel=Hyperlink.IMAGE,
            href="http://example.com/",
            thumbnail=link4,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )
        links = [link1, link2, link3, link4, link5]

        identifier = IdentifierData(type=Identifier.GUTENBERG_ID, identifier="1")
        circulation_data = CirculationData(
            data_source_name=DataSource.GUTENBERG,
            primary_identifier_data=identifier,
            links=links,
        )

        filtered_links = sorted(circulation_data.links, key=lambda x: x.rel)

        assert [link1] == filtered_links

    def test_explicit_formatdata(self, db: DatabaseTransactionFixture):
        # Creating an edition with an open-access download will
        # automatically create a delivery mechanism.
        edition, pool = db.edition(with_open_access_download=True)

        # Let's also add a DRM format.
        drm_format = FormatData(
            content_type=Representation.PDF_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.ADOBE_DRM,
        )

        circulation_data = CirculationData(
            formats=[drm_format],
            data_source_name=edition.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(
                edition.primary_identifier
            ),
        )
        circulation_data.apply(db.session, pool.collection)

        [epub, pdf] = sorted(
            pool.delivery_mechanisms, key=lambda x: x.delivery_mechanism.content_type
        )
        assert epub.resource is not None

        assert Representation.PDF_MEDIA_TYPE == pdf.delivery_mechanism.content_type
        assert DeliveryMechanism.ADOBE_DRM == pdf.delivery_mechanism.drm_scheme

        # If we tell Metadata to replace the list of formats, we only
        # have the one format we manually created.
        replace = ReplacementPolicy(
            formats=True,
        )
        circulation_data.apply(db.session, pool.collection, replace=replace)
        [pdf] = pool.delivery_mechanisms
        assert Representation.PDF_MEDIA_TYPE == pdf.delivery_mechanism.content_type

    def test_apply_removes_old_formats_based_on_replacement_policy(
        self, db: DatabaseTransactionFixture
    ):
        edition, pool = db.edition(with_license_pool=True)

        # Start with one delivery mechanism for this pool.
        for lpdm in pool.delivery_mechanisms:
            db.session.delete(lpdm)

        old_lpdm = pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        # And it has been loaned.
        patron = db.patron()
        loan, ignore = pool.loan_to(patron, fulfillment=old_lpdm)
        assert old_lpdm == loan.fulfillment

        # We have new circulation data that has a different format.
        format = FormatData(
            content_type=Representation.EPUB_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.ADOBE_DRM,
        )
        circulation_data = CirculationData(
            formats=[format],
            data_source_name=edition.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(
                edition.primary_identifier
            ),
        )

        # If we apply the new CirculationData with formats false in the policy,
        # we'll add the new format, but keep the old one as well.
        replacement_policy = ReplacementPolicy(formats=False)
        circulation_data.apply(db.session, pool.collection, replacement_policy)

        assert 2 == len(pool.delivery_mechanisms)
        assert {Representation.PDF_MEDIA_TYPE, Representation.EPUB_MEDIA_TYPE} == {
            lpdm.delivery_mechanism.content_type for lpdm in pool.delivery_mechanisms
        }
        assert old_lpdm == loan.fulfillment

        # But if we make formats true in the policy, we'll delete the old format
        # and remove it from its loan.
        replacement_policy = ReplacementPolicy(formats=True)
        circulation_data.apply(db.session, pool.collection, replacement_policy)

        assert 1 == len(pool.delivery_mechanisms)
        assert (
            Representation.EPUB_MEDIA_TYPE
            == pool.delivery_mechanisms[0].delivery_mechanism.content_type
        )
        assert None == loan.fulfillment

    def test_apply_adds_new_licenses(self, db: DatabaseTransactionFixture):
        edition, pool = db.edition(with_license_pool=True)

        # Start with one license for this pool.
        old_license = db.license(
            pool,
            expires=None,
            checkouts_left=2,
            checkouts_available=3,
        )

        # And it has been loaned.
        patron = db.patron()
        loan, ignore = old_license.loan_to(patron)
        assert old_license == loan.license

        # We have new circulation data that has a different license.
        license_data = LicenseData(
            identifier="8c5fdbfe-c26e-11e8-8706-5254009434c4",
            checkout_url="https://borrow2",
            status_url="https://status2",
            expires=(utc_now() + datetime.timedelta(days=7)),
            checkouts_left=None,
            checkouts_available=1,
            terms_concurrency=1,
            status=LicenseStatus.available,
        )

        circulation_data = CirculationData(
            licenses=[license_data],
            data_source_name=edition.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(
                edition.primary_identifier
            ),
        )

        # If we apply the new CirculationData, we'll add the new license,
        # but keep the old one as well.
        circulation_data.apply(db.session, pool.collection)
        db.session.commit()

        assert 2 == len(pool.licenses)
        assert {old_license.identifier, license_data.identifier} == {
            license.identifier for license in pool.licenses
        }
        assert old_license == loan.license

    def test_apply_updates_existing_licenses(self, db: DatabaseTransactionFixture):
        edition, pool = db.edition(with_license_pool=True)

        # Start with one license for this pool.
        old_license = db.license(
            pool,
            expires=None,
            checkouts_left=2,
            checkouts_available=3,
        )

        assert isinstance(old_license.identifier, str)
        assert isinstance(old_license.checkout_url, str)
        assert isinstance(old_license.status_url, str)
        license_data = LicenseData(
            identifier=old_license.identifier,
            expires=old_license.expires,
            checkouts_left=0,
            checkouts_available=3,
            status=LicenseStatus.unavailable,
            checkout_url=old_license.checkout_url,
            status_url=old_license.status_url,
        )

        circulation_data = CirculationData(
            licenses=[license_data],
            data_source_name=edition.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(
                edition.primary_identifier
            ),
        )

        circulation_data.apply(db.session, pool.collection)
        db.session.commit()

        assert 1 == len(pool.licenses)
        new_license = pool.licenses[0]
        assert new_license.id == old_license.id
        assert old_license.status == LicenseStatus.unavailable

    def test_apply_with_licenses_overrides_availability(
        self, db: DatabaseTransactionFixture
    ):
        edition, pool = db.edition(with_license_pool=True)

        license_data = LicenseData(
            identifier="8c5fdbfe-c26e-11e8-8706-5254009434c4",
            checkout_url="https://borrow2",
            status_url="https://status2",
            checkouts_available=0,
            terms_concurrency=1,
            status=LicenseStatus.available,
        )

        # If we give CirculationData both availability information
        # and licenses, it ignores the availability information and
        # instead uses the licenses to calculate availability.
        circulation_data = CirculationData(
            licenses=[license_data],
            data_source_name=edition.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(
                edition.primary_identifier
            ),
            licenses_owned=999,
            licenses_available=999,
            licenses_reserved=999,
            patrons_in_hold_queue=999,
        )

        circulation_data.apply(db.session, pool.collection)

        assert len(pool.licenses) == 1
        assert pool.licenses_available == 0
        assert pool.licenses_owned == 1
        assert pool.licenses_reserved == 0
        assert pool.patrons_in_hold_queue == 0

    def test_apply_without_licenses_sets_availability(
        self, db: DatabaseTransactionFixture
    ):
        edition, pool = db.edition(with_license_pool=True)

        # If we give CirculationData availability information without
        # also giving it licenses it uses the availability information
        # to set values on the LicensePool.
        circulation_data = CirculationData(
            data_source_name=edition.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(
                edition.primary_identifier
            ),
            licenses_owned=999,
            licenses_available=999,
            licenses_reserved=999,
            patrons_in_hold_queue=999,
        )

        circulation_data.apply(db.session, pool.collection)

        assert len(pool.licenses) == 0
        assert pool.licenses_available == 999
        assert pool.licenses_owned == 999
        assert pool.licenses_reserved == 999
        assert pool.patrons_in_hold_queue == 999

    def test_apply_creates_work_and_presentation_edition_if_needed(
        self, db: DatabaseTransactionFixture
    ):
        edition = db.edition()
        # This pool doesn't have a presentation edition or a work yet.
        pool = db.licensepool(edition)

        # We have new circulation data for this pool.
        circulation_data = CirculationData(
            formats=[],
            data_source_name=edition.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(
                edition.primary_identifier
            ),
        )

        # If we apply the new CirculationData the work gets both a
        # presentation and a work.
        replacement_policy = ReplacementPolicy()
        circulation_data.apply(db.session, pool.collection, replacement_policy)

        assert edition == pool.presentation_edition
        assert pool.work != None

        # If we have another new pool for the same book in another
        # collection, it will share the work.
        collection = db.collection()
        pool2 = db.licensepool(edition, collection=collection)
        circulation_data.apply(db.session, pool2.collection, replacement_policy)
        assert edition == pool2.presentation_edition
        assert pool.work == pool2.work

    def test_apply_respects_last_checked_timestamp(
        self, db: DatabaseTransactionFixture
    ) -> None:
        identifier = IdentifierData(type="test identifier", identifier="1")
        circulation = CirculationData(
            data_source_name="Test data source",
            primary_identifier_data=identifier,
            last_checked=utc_now() - datetime.timedelta(days=5),
            licenses_owned=100,
        )

        collection = db.collection()
        pool, new = circulation.license_pool(
            db.session, db.default_collection(), autocreate=False
        )
        assert pool is None
        assert new is False

        # Even though the last_checked value is 5 days ago, we still apply this data because
        # the license pool does not exist yet.
        pool, changes = circulation.apply(db.session, collection)
        assert changes is True
        assert pool is not None
        assert pool.last_checked == circulation.last_checked
        assert pool.availability_time == pool.last_checked
        assert pool.licenses_owned == 100

        # If we try to apply the same data again, nothing will change because
        # the last_checked value is older than the current last_checked value on the pool.
        circulation.licenses_owned = 10
        pool, changes = circulation.apply(db.session, collection)
        assert changes is False
        assert pool is not None
        assert pool.last_checked == circulation.last_checked
        assert pool.licenses_owned == 100

        # Unless the replacement policy forces it.
        pool, changes = circulation.apply(
            db.session,
            collection,
            replace=ReplacementPolicy(even_if_not_apparently_updated=True),
        )
        assert changes is True
        assert pool is not None
        assert pool.last_checked == circulation.last_checked
        assert pool.licenses_owned == 10

    def test_license_pool_sets_default_license_values(
        self, db: DatabaseTransactionFixture
    ):
        """We have no information about how many copies of the book we've
        actually licensed, but a LicensePool can be created anyway,
        so we can store format information.
        """
        identifier = IdentifierData(type=Identifier.OVERDRIVE_ID, identifier="1")
        drm_format = FormatData(
            content_type=Representation.PDF_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.ADOBE_DRM,
        )
        circulation = CirculationData(
            data_source_name=DataSource.OVERDRIVE,
            primary_identifier_data=identifier,
            formats=[drm_format],
        )
        collection = db.default_collection()
        pool, is_new = circulation.license_pool(db.session, collection)
        assert True == is_new
        assert collection == pool.collection

        # We start with the conservative assumption that we own no
        # licenses for the book.
        assert 0 == pool.licenses_owned
        assert 0 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

    def test_implicit_format_for_open_access_link(self, db: DatabaseTransactionFixture):
        # A format is a delivery mechanism.  We handle delivery on open access
        # pools from our mirrored content in S3.
        # Tests that when a link is open access, a pool can be delivered.

        edition, pool = db.edition(with_license_pool=True)

        # This is the delivery mechanism created by default when you
        # create a book with _edition().
        [epub] = pool.delivery_mechanisms
        assert Representation.EPUB_MEDIA_TYPE == epub.delivery_mechanism.content_type
        assert DeliveryMechanism.ADOBE_DRM == epub.delivery_mechanism.drm_scheme

        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.PDF_MEDIA_TYPE,
            href=db.fresh_url(),
        )
        circulation_data = CirculationData(
            data_source_name=DataSource.GUTENBERG,
            primary_identifier_data=IdentifierData.from_identifier(
                edition.primary_identifier
            ),
            links=[link],
        )

        replace = ReplacementPolicy(
            formats=True,
        )
        circulation_data.apply(db.session, pool.collection, replace)

        # We destroyed the default delivery format and added a new,
        # open access delivery format.
        [pdf] = pool.delivery_mechanisms
        assert Representation.PDF_MEDIA_TYPE == pdf.delivery_mechanism.content_type
        assert DeliveryMechanism.NO_DRM == pdf.delivery_mechanism.drm_scheme

        circulation_data = CirculationData(
            data_source_name=DataSource.GUTENBERG,
            primary_identifier_data=IdentifierData.from_identifier(
                edition.primary_identifier
            ),
            links=[],
        )
        replace = ReplacementPolicy(
            formats=True,
            links=True,
        )
        circulation_data.apply(db.session, pool.collection, replace)

        # Now we have no formats at all.
        assert 0 == len(pool.delivery_mechanisms)

    def test_rights_status_default_rights_passed_in(
        self, db: DatabaseTransactionFixture
    ):
        identifier = IdentifierData(
            type=Identifier.GUTENBERG_ID,
            identifier="abcd",
        )
        link = LinkData(
            rel=Hyperlink.DRM_ENCRYPTED_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=db.fresh_url(),
        )

        circulation_data = CirculationData(
            data_source_name=DataSource.OA_CONTENT_SERVER,
            primary_identifier_data=identifier,
            default_rights_uri=RightsStatus.CC_BY,
            links=[link],
        )

        replace = ReplacementPolicy(
            formats=True,
        )

        pool, ignore = circulation_data.license_pool(
            db.session, db.default_collection()
        )
        circulation_data.apply(db.session, pool.collection, replace)
        assert True == pool.open_access
        assert 1 == len(pool.delivery_mechanisms)
        # The rights status is the one that was passed in to CirculationData.
        assert RightsStatus.CC_BY == pool.delivery_mechanisms[0].rights_status.uri

    def test_rights_status_default_rights_from_data_source(
        self, db: DatabaseTransactionFixture
    ):
        identifier = IdentifierData(
            type=Identifier.GUTENBERG_ID,
            identifier="abcd",
        )
        link = LinkData(
            rel=Hyperlink.DRM_ENCRYPTED_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=db.fresh_url(),
        )

        circulation_data = CirculationData(
            data_source_name=DataSource.OA_CONTENT_SERVER,
            primary_identifier_data=identifier,
            links=[link],
        )

        replace = ReplacementPolicy(
            formats=True,
        )

        # This pool starts off as not being open-access.
        pool, ignore = circulation_data.license_pool(
            db.session, db.default_collection()
        )
        assert False == pool.open_access

        circulation_data.apply(db.session, pool.collection, replace)

        # The pool became open-access because it was given a
        # link that came from the OS content server.
        assert True == pool.open_access
        assert 1 == len(pool.delivery_mechanisms)
        # The rights status is the default for the OA content server.
        assert (
            RightsStatus.GENERIC_OPEN_ACCESS
            == pool.delivery_mechanisms[0].rights_status.uri
        )

    def test_rights_status_open_access_link_no_rights_uses_data_source_default(
        self, db
    ):
        identifier = IdentifierData(
            type=Identifier.GUTENBERG_ID,
            identifier="abcd",
        )

        # Here's a CirculationData that will create an open-access
        # LicensePoolDeliveryMechanism.
        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=db.fresh_url(),
        )
        circulation_data = CirculationData(
            data_source_name=DataSource.GUTENBERG,
            primary_identifier_data=identifier,
            links=[link],
        )
        replace_formats = ReplacementPolicy(
            formats=True,
        )

        pool, ignore = circulation_data.license_pool(
            db.session, db.default_collection()
        )
        pool.open_access = False

        # Applying this CirculationData to a LicensePool makes it
        # open-access.
        circulation_data.apply(db.session, pool.collection, replace_formats)
        assert True == pool.open_access
        assert 1 == len(pool.delivery_mechanisms)

        # The delivery mechanism's rights status is the default for
        # the data source.
        assert (
            RightsStatus.PUBLIC_DOMAIN_USA
            == pool.delivery_mechanisms[0].rights_status.uri
        )

        # Even if a commercial source like Overdrive should offer a
        # link with rel="open access", unless we know it's an
        # open-access link we will give it a RightsStatus of
        # IN_COPYRIGHT.
        identifier = IdentifierData(
            type=Identifier.OVERDRIVE_ID,
            identifier="abcd",
        )
        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=db.fresh_url(),
        )

        circulation_data = CirculationData(
            data_source_name=DataSource.OVERDRIVE,
            primary_identifier_data=identifier,
            links=[link],
        )

        pool, ignore = circulation_data.license_pool(
            db.session, db.default_collection()
        )
        pool.open_access = False
        circulation_data.apply(db.session, pool.collection, replace_formats)
        assert (
            RightsStatus.IN_COPYRIGHT == pool.delivery_mechanisms[0].rights_status.uri
        )

        assert False == pool.open_access

    def test_rights_status_open_access_link_with_rights(
        self, db: DatabaseTransactionFixture
    ):
        identifier = IdentifierData(
            type=Identifier.OVERDRIVE_ID,
            identifier="abcd",
        )
        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=db.fresh_url(),
            rights_uri=RightsStatus.CC_BY_ND,
        )

        circulation_data = CirculationData(
            data_source_name=DataSource.OVERDRIVE,
            primary_identifier_data=identifier,
            links=[link],
        )
        replace = ReplacementPolicy(
            formats=True,
        )

        pool, ignore = circulation_data.license_pool(
            db.session, db.default_collection()
        )
        circulation_data.apply(db.session, pool.collection, replace)
        assert True == pool.open_access
        assert 1 == len(pool.delivery_mechanisms)
        assert RightsStatus.CC_BY_ND == pool.delivery_mechanisms[0].rights_status.uri

    def test_rights_status_commercial_link_with_rights(
        self, db: DatabaseTransactionFixture
    ):
        identifier = IdentifierData(
            type=Identifier.OVERDRIVE_ID,
            identifier="abcd",
        )
        link = LinkData(
            rel=Hyperlink.DRM_ENCRYPTED_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=db.fresh_url(),
            rights_uri=RightsStatus.IN_COPYRIGHT,
        )
        format = FormatData(
            content_type=link.media_type,
            drm_scheme=DeliveryMechanism.ADOBE_DRM,
            link=link,
            rights_uri=RightsStatus.IN_COPYRIGHT,
        )

        circulation_data = CirculationData(
            data_source_name=DataSource.OVERDRIVE,
            primary_identifier_data=identifier,
            links=[link],
            formats=[format],
        )

        replace = ReplacementPolicy(
            formats=True,
        )

        pool, ignore = circulation_data.license_pool(
            db.session, db.default_collection()
        )
        circulation_data.apply(db.session, pool.collection, replace)
        assert False == pool.open_access
        assert 1 == len(pool.delivery_mechanisms)
        assert (
            RightsStatus.IN_COPYRIGHT == pool.delivery_mechanisms[0].rights_status.uri
        )

    def test_format_change_may_change_open_access_status(
        self, db: DatabaseTransactionFixture
    ):
        # In this test, whenever we call CirculationData.apply(), we
        # want to destroy the old list of formats and recreate it.
        replace_formats = ReplacementPolicy(formats=True)

        # Here's a seemingly ordinary non-open-access LicensePool.
        edition, pool = db.edition(with_license_pool=True)
        assert False == pool.open_access

        # One day, we learn that it has an open-access delivery mechanism.
        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=db.fresh_url(),
            rights_uri=RightsStatus.CC_BY_ND,
        )

        circulation_data = CirculationData(
            data_source_name=pool.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(pool.identifier),
            links=[link],
        )

        # Applying this information turns the pool into an open-access pool.
        circulation_data.apply(db.session, pool.collection, replace=replace_formats)
        assert True == pool.open_access

        # Then we find out it was a mistake -- the book is in copyright.
        format = FormatData(
            content_type=Representation.EPUB_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.NO_DRM,
            rights_uri=RightsStatus.IN_COPYRIGHT,
        )
        circulation_data = CirculationData(
            data_source_name=pool.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(pool.identifier),
            formats=[format],
        )
        circulation_data.apply(db.session, pool.collection, replace=replace_formats)

        # The original LPDM has been removed and only the new one remains.
        assert False == pool.open_access
        assert 1 == len(pool.delivery_mechanisms)

    def test_license_pool(self, db: DatabaseTransactionFixture):
        collection = db.collection()
        identifier = IdentifierData(type=Identifier.GUTENBERG_ID, identifier="1")

        circulation = CirculationData(
            data_source_name=DataSource.GUTENBERG,
            primary_identifier_data=identifier,
        )

        # Calling without a collection raises an error
        with pytest.raises(ValueError) as excinfo:
            circulation.license_pool(db.session, None)

        # If a pool doesn't exist, one is created
        pool_created, is_new = circulation.license_pool(db.session, collection)
        assert is_new is True
        assert pool_created.collection == collection
        assert pool_created.identifier.type == identifier.type
        assert pool_created.identifier.identifier == identifier.identifier

        # Calling a second time returns the same pool
        pool_existing, is_new = circulation.license_pool(db.session, collection)
        assert is_new is False
        assert pool_existing.id == pool_created.id

        # Unless we call with autocreate=False, then the pool is not automatically created
        identifier = IdentifierData(type="test identifier", identifier="2")
        circulation = CirculationData(
            data_source_name="Test data source",
            primary_identifier_data=identifier,
        )

        pool_looked_up, is_new = circulation.license_pool(
            db.session, collection, autocreate=False
        )
        assert pool_looked_up is None
        assert is_new is False

        # Create the identifier
        identifier.load(db.session)

        # The pool still isn't created
        pool_looked_up, is_new = circulation.license_pool(
            db.session, collection, autocreate=False
        )
        assert pool_looked_up is None
        assert is_new is False

        # Create the datasource
        DataSource.lookup(db.session, "Test data source", autocreate=True)

        # The pool still isn't created
        pool_looked_up, is_new = circulation.license_pool(
            db.session, collection, autocreate=False
        )
        assert pool_looked_up is None
        assert is_new is False

        # Create the pool
        LicensePool.for_foreign_id(
            db.session,
            data_source="Test data source",
            foreign_id_type=identifier.type,
            foreign_id=identifier.identifier,
            collection=collection,
        )

        # Now the pool is found and returned
        pool_looked_up, is_new = circulation.license_pool(
            db.session, collection, autocreate=False
        )
        assert is_new is False
        assert pool_looked_up is not None
        assert pool_looked_up.identifier.type == identifier.type
        assert pool_looked_up.identifier.identifier == identifier.identifier

    def test_has_changed(self, db: DatabaseTransactionFixture):
        collection = db.collection()
        identifier = IdentifierData(type="test identifier", identifier="2")
        circulation = CirculationData(
            data_source_name="Test data source",
            primary_identifier_data=identifier,
            last_checked=None,
        )

        today = utc_now()
        one_day_ago = today - datetime.timedelta(days=1)
        two_days_ago = today - datetime.timedelta(days=2)

        # Since last_updated is None, we always consider the data to have changed
        assert circulation.has_changed(db.session, collection=collection) is True

        # The licensepool does not exist, so we consider the data to have changed
        circulation.last_checked = one_day_ago
        assert circulation.has_changed(db.session, collection=collection) is True

        # Create the pool
        pool, _ = circulation.license_pool(db.session, collection, autocreate=True)
        pool.last_checked = None

        # The pool exists but last_checked is None, so we consider the data to have changed
        assert circulation.has_changed(db.session, collection=collection) is True

        # Set last_checked to 2 days ago
        pool.last_checked = two_days_ago
        assert circulation.has_changed(db.session, collection=collection) is True

        # But if the pool was checked more recently than the data, nothing has changed
        pool.last_checked = today
        assert circulation.has_changed(db.session, collection=collection) is False
