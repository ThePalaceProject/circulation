from __future__ import annotations

import dataclasses
import datetime
from collections.abc import Sequence
from typing import Any

from sqlalchemy.orm import Session

from palace.manager.metadata_layer.format import FormatData
from palace.manager.metadata_layer.identifier import IdentifierData
from palace.manager.metadata_layer.license import LicenseData
from palace.manager.metadata_layer.link import LinkData
from palace.manager.metadata_layer.policy.replacement import ReplacementPolicy
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin


class CirculationData(LoggerMixin):
    """Information about actual copies of a book that can be delivered to
    patrons.

    As distinct from Metadata, which is a container for information
    about a book.

    Basically,
        Metadata : Edition :: CirculationData : Licensepool
    """

    def __init__(
        self,
        data_source: str | DataSource,
        primary_identifier: Identifier | IdentifierData | None,
        licenses_owned: int | None = None,
        licenses_available: int | None = None,
        licenses_reserved: int | None = None,
        patrons_in_hold_queue: int | None = None,
        formats: list[FormatData] | None = None,
        default_rights_uri: str | None = None,
        links: list[LinkData] | None = None,
        licenses: list[LicenseData] | None = None,
        last_checked: datetime.datetime | None = None,
        should_track_playtime: bool = False,
    ) -> None:
        """Constructor.

        :param data_source: The authority providing the lending licenses.
            This may be a DataSource object or the name of the data source.
        :param primary_identifier: An Identifier or IdentifierData representing
            how the lending authority distinguishes this book from others.
        """
        self._data_source = data_source
        if isinstance(self._data_source, DataSource):
            self.data_source_obj: DataSource | None = self._data_source
            self.data_source_name = self.data_source_obj.name
        else:
            self.data_source_obj = None
            self.data_source_name = self._data_source

        if isinstance(primary_identifier, Identifier):
            self.primary_identifier_obj: Identifier | None = primary_identifier
            self._primary_identifier: IdentifierData | None = IdentifierData(
                primary_identifier.type, primary_identifier.identifier
            )
        else:
            self.primary_identifier_obj = None
            self._primary_identifier = primary_identifier
        self.licenses_owned = licenses_owned
        self.licenses_available = licenses_available
        self.licenses_reserved = licenses_reserved
        self.patrons_in_hold_queue = patrons_in_hold_queue

        # If no 'last checked' data was provided, assume the data was
        # just gathered.
        self.last_checked: datetime.datetime = last_checked or utc_now()

        # format contains pdf/epub, drm, link
        self.formats: list[FormatData] = formats or []

        self.default_rights_uri: str | None = None
        self.set_default_rights_uri(
            data_source_name=self.data_source_name,
            default_rights_uri=default_rights_uri,
        )

        self.__links: list[LinkData] | None = None
        # The type ignore here is necessary because mypy does not like when a property setter and
        # getter have different types. A PR just went in to fix this in mypy, so this should be able
        # to be removed once mypy 1.16 is released.
        # See: https://github.com/python/mypy/pull/18510
        self.links = links  # type: ignore[assignment]

        # Information about individual terms for each license in a pool. If we are
        # given licenses then they are used to calculate values for the LicensePool
        # instead of directly using the values that are given to CirculationData.
        self.licenses: list[LicenseData] | None = licenses

        # Whether the license should contain a playtime tracking link
        self.should_track_playtime: bool = should_track_playtime

    @property
    def links(self) -> Sequence[LinkData]:
        return self.__links or []

    @links.setter
    def links(self, arg_links: list[LinkData] | None) -> None:
        """If got passed all links, indiscriminately, filter out to only those relevant to
        pools (the rights-related links).
        """
        # start by deleting any old links
        self.__links = []

        if not arg_links:
            return

        for link in arg_links:
            if link.rel in Hyperlink.CIRCULATION_ALLOWED:
                # TODO:  what about Hyperlink.SAMPLE?
                # only accept the types of links relevant to pools
                self.__links.append(link)

                # An open-access link or open-access rights implies a FormatData object.
                open_access_link = (
                    link.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD and link.href
                )
                # try to deduce if the link is open-access, even if it doesn't explicitly say it is
                rights_uri = link.rights_uri or self.default_rights_uri
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
                    format_found = False
                    format = None
                    for format in self.formats:
                        if format and format.link and format.link.href == link.href:
                            format_found = True
                            break
                    if format_found and format and not format.rights_uri:
                        self.formats.remove(format)
                        self.formats.append(
                            dataclasses.replace(format, rights_uri=rights_uri)
                        )
                    if not format_found:
                        self.formats.append(
                            FormatData(
                                content_type=link.media_type,
                                drm_scheme=DeliveryMechanism.NO_DRM,
                                link=link,
                                rights_uri=rights_uri,
                            )
                        )

    def __repr__(self) -> str:
        description_string = "<CirculationData primary_identifier=%(primary_identifier)r| licenses_owned=%(licenses_owned)s|"
        description_string += " licenses_available=%(licenses_available)s| default_rights_uri=%(default_rights_uri)s|"
        description_string += (
            " links=%(links)r| formats=%(formats)r| data_source=%(data_source)s|>"
        )

        description_data: dict[str, Any] = {"licenses_owned": self.licenses_owned}
        if self._primary_identifier:
            description_data["primary_identifier"] = self._primary_identifier
        else:
            description_data["primary_identifier"] = self.primary_identifier_obj
        description_data["licenses_available"] = self.licenses_available
        description_data["default_rights_uri"] = self.default_rights_uri
        description_data["links"] = self.links
        description_data["formats"] = self.formats
        description_data["data_source"] = self.data_source_name

        return description_string % description_data

    def data_source(self, _db: Session) -> DataSource:
        """Find the DataSource associated with this circulation information."""
        if not self.data_source_obj:
            obj = DataSource.lookup(_db, self.data_source_name, autocreate=True)
            self.data_source_obj = obj
        return self.data_source_obj

    def primary_identifier(self, _db: Session) -> Identifier:
        """Find the Identifier associated with this circulation information."""
        if not self.primary_identifier_obj:
            if self._primary_identifier:
                obj, ignore = self._primary_identifier.load(_db)
            else:
                raise ValueError("No primary identifier provided!")
            self.primary_identifier_obj = obj
        return self.primary_identifier_obj

    def license_pool(
        self, _db: Session, collection: Collection | None
    ) -> tuple[LicensePool, bool]:
        """Find or create a LicensePool object for this CirculationData.

        :param collection: The LicensePool object will be associated with
            the given Collection.
        """
        if not collection:
            raise ValueError("Cannot find license pool: no collection provided.")
        identifier = self.primary_identifier(_db)
        if not identifier:
            raise ValueError(
                "Cannot find license pool: CirculationData has no primary identifier."
            )

        data_source_obj = self.data_source(_db)
        license_pool, is_new = LicensePool.for_foreign_id(
            _db,
            data_source=data_source_obj,
            foreign_id_type=identifier.type,
            foreign_id=identifier.identifier,
            collection=collection,
        )

        if is_new:
            license_pool.open_access = self.has_open_access_link
            license_pool.availability_time = self.last_checked
            license_pool.last_checked = self.last_checked
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

    def set_default_rights_uri(
        self, data_source_name: str | None, default_rights_uri: str | None = None
    ) -> None:
        if default_rights_uri:
            self.default_rights_uri = default_rights_uri

        elif data_source_name:
            # We didn't get rights passed in, so use the default rights for the data source if any.
            default = RightsStatus.DATA_SOURCE_DEFAULT_RIGHTS_STATUS.get(
                data_source_name, None
            )
            if default:
                self.default_rights_uri = default

        if not self.default_rights_uri:
            # We still haven't determined rights, so it's unknown.
            self.default_rights_uri = RightsStatus.UNKNOWN

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

        data_source = self.data_source(_db)
        identifier = self.primary_identifier(_db)
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
        if pool and self._availability_needs_update(pool):
            # Update availability information. This may result in
            # the issuance of additional circulation events.
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

    def _availability_needs_update(self, pool: LicensePool) -> bool:
        """Does this CirculationData represent information more recent than
        what we have for the given LicensePool?
        """
        if not self.last_checked:
            # Assume that our data represents the state of affairs
            # right now.
            return True
        if not pool.last_checked:
            # It looks like the LicensePool has never been checked.
            return True
        return self.last_checked >= pool.last_checked
