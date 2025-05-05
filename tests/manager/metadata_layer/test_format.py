from palace.manager.metadata_layer.format import FormatData
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, RightsStatus
from palace.manager.sqlalchemy.model.resource import Representation
from tests.fixtures.database import DatabaseTransactionFixture


class TestFormatData:
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
