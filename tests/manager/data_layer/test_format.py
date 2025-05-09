from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.link import LinkData
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, RightsStatus
from palace.manager.sqlalchemy.model.resource import Representation
from tests.fixtures.database import DatabaseTransactionFixture


class TestFormatData:
    def test_hash(self) -> None:
        # Test that FormatData is hashable
        hash(FormatData(content_type="foo", drm_scheme="bar"))

    def test_apply_to_loan(self, db: DatabaseTransactionFixture) -> None:
        # Here's a LicensePool with one non-open-access delivery mechanism.
        session = db.session
        pool = db.licensepool(None)
        assert pool.open_access is False
        [mechanism] = [lpdm.delivery_mechanism for lpdm in pool.delivery_mechanisms]
        assert mechanism.content_type == Representation.EPUB_MEDIA_TYPE
        assert mechanism.drm_scheme == DeliveryMechanism.ADOBE_DRM

        # This patron has the book out on loan, but as far as we know,
        # no delivery mechanism has been set.
        patron = db.patron()
        loan, ignore = pool.loan_to(patron)

        # When consulting with the source of the loan, we learn that
        # the patron has locked the delivery mechanism to a previously
        # unknown mechanism.
        format_data = FormatData(
            content_type=Representation.PDF_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.NO_DRM,
            rights_uri=RightsStatus.IN_COPYRIGHT,
        )
        format_data.apply_to_loan(session, loan)

        # This results in the addition of a new delivery mechanism to
        # the LicensePool.
        [new_mechanism] = [
            lpdm.delivery_mechanism
            for lpdm in pool.delivery_mechanisms
            if lpdm.delivery_mechanism != mechanism
        ]
        assert new_mechanism.content_type == Representation.PDF_MEDIA_TYPE
        assert new_mechanism.drm_scheme == DeliveryMechanism.NO_DRM
        assert loan.fulfillment.delivery_mechanism == new_mechanism
        assert loan.fulfillment.rights_status.uri == RightsStatus.IN_COPYRIGHT

        # Calling apply_to_loan() again with the same arguments does nothing.
        format_data.apply_to_loan(session, loan)
        assert len(pool.delivery_mechanisms) == 2

    def test_gather_data_from_link(self) -> None:
        link = LinkData(
            rel="test",
            href="http://example.com",
            media_type="application/pdf",
            rights_uri=RightsStatus.IN_COPYRIGHT,
        )

        # If FormatData is created with rights_uri and content_type
        # they are used directly.
        format_data = FormatData(
            content_type="application/epub+zip",
            drm_scheme=DeliveryMechanism.ADOBE_DRM,
            link=link,
            rights_uri=RightsStatus.CC0,
        )
        assert format_data.content_type == "application/epub+zip"
        assert format_data.drm_scheme == DeliveryMechanism.ADOBE_DRM
        assert format_data.rights_uri == RightsStatus.CC0

        # However, if FormatData is created with a LinkData object
        # and no content_type or rights_uri, the content_type and
        # rights_uri are taken from the LinkData object.
        format_data = FormatData(content_type=None, drm_scheme=None, link=link)
        assert format_data.content_type == "application/pdf"
        assert format_data.drm_scheme is None
        assert format_data.rights_uri == RightsStatus.IN_COPYRIGHT
