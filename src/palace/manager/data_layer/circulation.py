from __future__ import annotations

import datetime
from typing import Literal, Self, overload

from pydantic import model_validator
from sqlalchemy.orm import Session

from palace.manager.data_layer.base.mutable import BaseMutableData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.license import LicenseData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
    LicensePoolStatus,
    LicensePoolType,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.datetime_helpers import utc_now


class CirculationData(BaseMutableData):
    """Information about actual copies of a book that can be delivered to
    patrons.

    As distinct from BibliographicData, which is a container for information
    about a book.

    Basically,
        BibliographicData : Edition :: CirculationData : Licensepool
    """

    licenses_owned: int | None = None
    licenses_available: int | None = None
    licenses_reserved: int | None = None
    patrons_in_hold_queue: int | None = None
    default_rights_uri: str | None = None
    links: list[LinkData] = []
    formats: list[FormatData] = []
    licenses: list[LicenseData] | None = None
    last_checked: datetime.datetime | None = None
    should_track_playtime: bool = False

    # The licensing model for the pool (METERED, UNLIMITED, or AGGREGATED).
    # If None, the existing pool type will not be updated.
    type: LicensePoolType | None = None

    # The operational status of the pool (PRE_ORDER, ACTIVE, EXHAUSTED, or REMOVED).
    # If None, the existing pool status will not be updated.
    status: LicensePoolStatus | None = None

    @model_validator(mode="after")
    def _filter_and_set_defaults(self) -> Self:
        # We didn't get rights passed in, so use the default rights for the data source if any.
        default_rights_uri = self.default_rights_uri
        if not default_rights_uri and self.data_source_name is not None:
            default_rights_uri = RightsStatus.DATA_SOURCE_DEFAULT_RIGHTS_STATUS.get(
                self.data_source_name, None
            )

        # We still haven't determined rights, so it's unknown.
        if not default_rights_uri:
            default_rights_uri = RightsStatus.UNKNOWN

        # If got passed all links, indiscriminately, filter out to only those relevant to
        # pools (the rights-related links).

        # TODO:  what about Hyperlink.SAMPLE?
        # only accept the types of links relevant to pools
        links = [l for l in self.links if l.rel in Hyperlink.CIRCULATION_ALLOWED]
        formats = self.formats

        for link in links:
            # An open-access link or open-access rights implies a FormatData object.
            open_access_link = link.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD and link.href
            # try to deduce if the link is open-access, even if it doesn't explicitly say it is
            rights_uri = link.rights_uri or default_rights_uri
            open_access_rights_link = (
                link.media_type in Representation.BOOK_MEDIA_TYPES
                and link.href
                and rights_uri in RightsStatus.OPEN_ACCESS
            )

            if open_access_link or open_access_rights_link:
                if (
                    open_access_link
                    and rights_uri != RightsStatus.IN_COPYRIGHT
                    and not rights_uri in RightsStatus.OPEN_ACCESS
                ):
                    # We don't know exactly what's going on here but
                    # the link said it was an open-access book
                    # and the rights URI doesn't contradict it,
                    # so treat it as a generic open-access book.
                    rights_uri = RightsStatus.GENERIC_OPEN_ACCESS

                format = next(
                    (fmt for fmt in formats if fmt.link and fmt.link.href == link.href),
                    None,
                )
                if format is not None and not format.rights_uri:
                    formats.remove(format)
                    formats.append(format.model_copy(update={"rights_uri": rights_uri}))
                if format is None:
                    formats.append(
                        FormatData(
                            content_type=link.media_type,
                            drm_scheme=DeliveryMechanism.NO_DRM,
                            link=link,
                            rights_uri=rights_uri,
                        )
                    )

        # We do this to work around a recursion error, where setting these properties, triggers
        # validation again. Causing an infinite loop.
        # See:
        #  - https://github.com/pydantic/pydantic/issues/6597
        #  - https://github.com/pydantic/pydantic/issues/8185
        self.__dict__["links"] = links
        self.__dict__["formats"] = formats
        self.__dict__["default_rights_uri"] = default_rights_uri

        return self

    @overload
    def license_pool(
        self,
        _db: Session,
        collection: Collection | None,
        autocreate: Literal[True] = ...,
    ) -> tuple[LicensePool, bool]: ...

    @overload
    def license_pool(
        self, _db: Session, collection: Collection | None, autocreate: bool
    ) -> tuple[LicensePool | None, bool]: ...

    def license_pool(
        self, _db: Session, collection: Collection | None, autocreate: bool = True
    ) -> tuple[LicensePool | None, bool]:
        """Find or create a LicensePool object for this CirculationData.

        :param collection: The LicensePool object will be associated with
            the given Collection.
        """
        if not collection:
            raise ValueError("Cannot find license pool: no collection provided.")
        identifier = self.load_primary_identifier(_db, autocreate=autocreate)
        if identifier is None:
            return None, False

        data_source_obj = self.load_data_source(_db, autocreate=autocreate)
        if data_source_obj is None:
            return None, False

        license_pool, is_new = LicensePool.for_foreign_id(
            _db,
            data_source=data_source_obj,
            foreign_id_type=identifier.type,
            foreign_id=identifier.identifier,
            collection=collection,
            autocreate=autocreate,
        )

        if license_pool is not None and is_new:
            license_pool.availability_time = (
                self.last_checked if self.last_checked else utc_now()
            )
            license_pool.last_checked = None
            license_pool.open_access = self.has_open_access_link
            license_pool.should_track_playtime = self.should_track_playtime

        return license_pool, is_new

    @property
    def has_open_access_link(self) -> bool:
        """Does this Circulation object have an associated open-access link?"""
        return any(
            [
                x
                for x in self.links
                if x.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD
                and x.href
                and x.rights_uri != RightsStatus.IN_COPYRIGHT
            ]
        )

    def apply(
        self,
        _db: Session,
        collection: Collection | None,
        replace: ReplacementPolicy | None = None,
    ) -> tuple[LicensePool | None, bool]:
        """Update the title with this CirculationData's information.

        :param collection: A Collection representing actual copies of
            this title. Availability information (e.g. number of copies)
            will be associated with a LicensePool in this Collection. If
            this is not present, only delivery information (e.g. format
            information and open-access downloads) will be processed.

        """
        # Immediately raise an exception if there is information that
        # can only be stored in a LicensePool, but we have no
        # Collection to tell us which LicensePool to use. This is
        # indicative of an error in programming.
        if not collection and (
            self.licenses_owned is not None
            or self.licenses_available is not None
            or self.licenses_reserved is not None
            or self.patrons_in_hold_queue is not None
        ):
            raise ValueError(
                "Cannot store circulation information because no "
                "Collection was provided."
            )

        made_changes = False
        if replace is None:
            replace = ReplacementPolicy()

        pool = None
        if collection:
            pool, ignore = self.license_pool(_db, collection)

        data_source = self.load_data_source(_db)
        identifier = self.load_primary_identifier(_db)
        # First, make sure all links in self.links are associated with the book's identifier.

        # TODO: be able to handle the case where the URL to a link changes or
        # a link disappears.
        link_objects: dict[LinkData, Hyperlink] = {}
        for link in self.links:
            if link.rel in Hyperlink.CIRCULATION_ALLOWED and identifier is not None:
                link_obj, ignore = identifier.add_link(
                    rel=link.rel,
                    href=link.href,
                    data_source=data_source,
                    media_type=link.media_type,
                    content=link.content,
                    db=_db,
                )
                link_objects[link] = link_obj

        # Next, make sure the DeliveryMechanisms associated
        # with the book reflect the formats in self.formats.
        old_lpdms: list[LicensePoolDeliveryMechanism] = []
        new_lpdms: list[LicensePoolDeliveryMechanism] = []
        if pool:
            pool.should_track_playtime = self.should_track_playtime
            old_lpdms = list(pool.delivery_mechanisms)

        # Before setting and unsetting delivery mechanisms, which may
        # change the open-access status of the work, see what it the
        # status currently is.
        pools = identifier.licensed_through if identifier is not None else []
        old_open_access = any(pool.open_access for pool in pools)

        for format in self.formats:
            if format.link:
                link_obj = link_objects[format.link]
                resource = link_obj.resource
            else:
                resource = None
            # This can cause a non-open-access LicensePool to go open-access.
            lpdm = format.apply(
                _db,
                data_source,
                identifier,
                resource,
                default_rights_uri=self.default_rights_uri,
            )
            new_lpdms.append(lpdm)

        if replace.formats:
            # If any preexisting LicensePoolDeliveryMechanisms were
            # not mentioned in self.formats, remove the corresponding
            # LicensePoolDeliveryMechanisms.
            for lpdm in old_lpdms:
                if lpdm not in new_lpdms:
                    for loan in lpdm.fulfills:
                        self.log.info(
                            "Loan %i is associated with a format that is no longer available. Deleting its delivery mechanism."
                            % loan.id
                        )
                        loan.fulfillment = None
                    # This can cause an open-access LicensePool to go
                    # non-open-access.
                    lpdm.delete()

        new_open_access = any(pool.open_access for pool in pools)
        open_access_status_changed = old_open_access != new_open_access

        # Finally, if we have data for a specific Collection's license
        # for this book, find its LicensePool and update it.
        changed_availability = False
        if pool and (
            replace.even_if_not_apparently_updated or self.has_changed(_db, pool=pool)
        ):
            # Update availability information. This may result in
            # the issuance of additional circulation events.

            # Update license pool type if it differs from the incoming data.
            if self.type is not None and pool.type != self.type:
                self.log.info(
                    f"License pool type changing from {pool.type} to {self.type} for {pool.identifier!r}"
                )
                pool.type = self.type

            # Update license pool status if it differs from the incoming data.
            # Status changes track the operational state of the pool
            # (e.g., active → removed when a title is withdrawn by the vendor).
            if self.status is not None and pool.status != self.status:
                self.log.info(
                    f"License pool status changing from {pool.status} to {self.status} for {pool.identifier!r}"
                )
                pool.status = self.status

            if self.licenses is not None:
                # If we have licenses set, use those to set our availability
                old_licenses = list(pool.licenses or [])
                new_licenses = [
                    license.add_to_pool(_db, pool) for license in self.licenses
                ]
                for license in old_licenses:
                    if license not in new_licenses:
                        self.log.warning(
                            f"License {license.identifier} has been removed from feed."
                        )
                changed_availability = pool.update_availability_from_licenses(
                    as_of=self.last_checked,
                )
            else:
                # Otherwise update the availability directly
                changed_availability = pool.update_availability(
                    new_licenses_owned=self.licenses_owned,
                    new_licenses_available=self.licenses_available,
                    new_licenses_reserved=self.licenses_reserved,
                    new_patrons_in_hold_queue=self.patrons_in_hold_queue,
                    as_of=self.last_checked,
                )

        # If this is the first time we've seen this pool, or we never
        # made a Work for it, make one now.
        work_changed = False
        if pool and not pool.work:
            work, work_changed = pool.calculate_work()
            if work:
                work.set_presentation_ready()
                work_changed = True

        made_changes = (
            made_changes
            or changed_availability
            or open_access_status_changed
            or work_changed
        )

        return pool, made_changes

    @overload
    def has_changed(
        self,
        session: Session,
        *,
        collection: Collection,
    ) -> bool: ...

    @overload
    def has_changed(
        self,
        session: Session,
        *,
        pool: LicensePool,
    ) -> bool: ...

    def has_changed(
        self,
        session: Session,
        *,
        collection: Collection | None = None,
        pool: LicensePool | None = None,
    ) -> bool:
        """
        Does this CirculationData represent information more recent than
        what we have for the given LicensePool?

        One of `collection` or `pool` must be provided.
        """
        if not self.last_checked:
            # Assume that our data represents the state of affairs right now.
            return True

        if pool is None:
            pool, _ = self.license_pool(session, collection, autocreate=False)
        if pool is None:
            # We don't have an existing license pool, so we need to create one.
            return True

        if not pool.last_checked:
            # It looks like the LicensePool has never been checked.
            return True

        return self.last_checked > pool.last_checked
