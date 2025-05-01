from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from palace.manager.metadata_layer.link import LinkData
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.model.resource import Resource
from palace.manager.util.log import LoggerMixin


@dataclass(frozen=True, kw_only=True)
class FormatData(LoggerMixin):
    content_type: str | None
    drm_scheme: str | None
    link: LinkData | None = None
    rights_uri: str | None = None
    available: bool = True
    # By default, we don't update a formats availability, we only set it when
    # creating a new one, this can be overridden by setting this flag to True.
    update_available: bool = False

    def __post_init__(self) -> None:
        if self.link and not isinstance(self.link, LinkData):
            raise TypeError("Expected LinkData object, got %s" % type(self.link))

        # We can't use direct assignment because of the frozen=True flag, so
        # we have to use object.__setattr__.
        # https://stackoverflow.com/questions/53756788/how-to-set-the-value-of-dataclass-field-in-post-init-when-frozen-true
        if self.link:
            if not self.rights_uri and self.link.rights_uri:
                object.__setattr__(self, "rights_uri", self.link.rights_uri)

            if not self.content_type and self.link.media_type:
                object.__setattr__(self, "content_type", self.link.media_type)

    def apply(
        self,
        db: Session,
        data_source: DataSource,
        identifier: Identifier,
        resource: Resource | None = None,
        default_rights_uri: str | None = None,
    ) -> LicensePoolDeliveryMechanism:
        """Apply this FormatData. Creating a new LicensePoolDeliveryMechanism
        if necessary.

        :param db: Use this database connection. If this is not supplied
            the database connection will be taken from the data_source.
        :param data_source: A DataSource identifying the distributor.
        :param identifier: An Identifier identifying the title.
        :param resource: A Resource representing the book itself in
            a freely redistributable form, if any.
        :param default_rights_uri: The default rights URI to use if none is
            specified in the FormatData.

        :return: A LicensePoolDeliveryMechanism.
        """
        return LicensePoolDeliveryMechanism.set(
            data_source,
            identifier,
            rights_uri=self.rights_uri or default_rights_uri,
            resource=resource,
            content_type=self.content_type,
            drm_scheme=self.drm_scheme,
            available=self.available,
            update_available=self.update_available,
            db=db,
        )

    def apply_to_loan(
        self,
        db: Session,
        loan: Loan,
    ) -> LicensePoolDeliveryMechanism | None:
        """Set an appropriate LicensePoolDeliveryMechanism on the given
        `Loan`, creating the DeliveryMechanism and LicensePoolDeliveryMechanism
         if necessary.

        :param db: A database session.
        :param loan: A Loan object.
        :return: A LicensePoolDeliveryMechanism if one could be set on the
            given Loan; None otherwise.
        """

        # Create or update the DeliveryMechanism.
        delivery_mechanism, _ = DeliveryMechanism.lookup(
            db, self.content_type, self.drm_scheme
        )

        if (
            loan.fulfillment
            and loan.fulfillment.delivery_mechanism == delivery_mechanism
        ):
            # The work has already been done. Do nothing.
            return None

        # At this point we know we need to update the local delivery
        # mechanism.
        pool = loan.license_pool
        if not pool:
            # This shouldn't happen, but bail out if it does.
            self.log.warning(
                f"No license pool for loan (id:{loan.id}), can't set delivery mechanism."
            )
            return None

        # Apply this FormatData, looking up or creating a LicensePoolDeliveryMechanism.
        lpdm = self.apply(
            db,
            pool.data_source,
            pool.identifier,
        )
        loan.fulfillment = lpdm
        return lpdm
