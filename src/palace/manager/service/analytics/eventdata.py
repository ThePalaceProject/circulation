from __future__ import annotations

from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Self
from uuid import UUID

import flask
from pydantic import AwareDatetime, BaseModel, ConfigDict, computed_field

from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.library import Library
    from palace.manager.sqlalchemy.model.licensing import LicensePool
    from palace.manager.sqlalchemy.model.patron import Patron


class AnalyticsEventData(BaseModel, LoggerMixin):
    """
    This class represents the data that is sent to the analytics provider.

    It is a Pydantic model that is used to validate the data before it is sent. It stores
    all the data without references to any database objects, so it can be serialized, stored
    and sent outside a database transaction.
    """

    type: str
    start: AwareDatetime

    # TODO: We include the 'end' field as a copy of 'start' because that is
    #   what the pre-pydantic implementation did. Since this is just duplicated
    #   we should remove it in a future release.
    @computed_field  # type: ignore[prop-decorator]
    @cached_property
    def end(self) -> AwareDatetime:
        return self.start

    library_id: int
    library_name: str
    library_short_name: str
    old_value: int | None
    new_value: int | None

    @computed_field  # type: ignore[prop-decorator]
    @cached_property
    def delta(self) -> int | None:
        if self.new_value is None or self.old_value is None:
            return None
        return self.new_value - self.old_value

    # TODO: This field has been removed from the database model, but is still included
    #   in the response for backwards compatibility. It should be removed in a future
    #   release.
    location: None = None
    license_pool_id: int | None
    publisher: str | None
    imprint: str | None
    issued: datetime | None
    published: datetime | None
    medium: str | None
    collection: str | None
    collection_id: int | None
    identifier_type: str | None
    identifier: str | None
    data_source: str | None
    distributor: str | None
    audience: str | None
    fiction: bool | None
    summary_text: str | None
    quality: float | None
    rating: int | None
    popularity: int | None
    genre: str | None
    availability_time: AwareDatetime | None
    licenses_owned: int | None
    licenses_available: int | None
    licenses_reserved: int | None
    patrons_in_hold_queue: int | None

    # TODO: We no longer support self-hosted books, so this should always be False.
    #  this value is still included in the response for backwards compatibility,
    #  but should be removed in a future release.
    self_hosted: bool = False
    title: str | None
    author: str | None
    series: str | None
    series_position: int | None
    language: str | None
    open_access: bool | None
    user_agent: str | None
    patron_uuid: UUID | None

    model_config = ConfigDict(
        frozen=True,
    )

    @classmethod
    def create(
        cls,
        library: Library,
        license_pool: LicensePool | None,
        event_type: str,
        time: datetime | None = None,
        old_value: int | None = None,
        new_value: int | None = None,
        patron: Patron | None = None,
        user_agent: str | None = None,
    ) -> Self:
        if user_agent is None:
            try:
                user_agent = flask.request.user_agent.string
                if user_agent == "":
                    user_agent = None
            except RuntimeError:
                # Flask raises a RuntimeError if there is no request context.
                # This can happen if the event is created outside of a flask request
                # context, for example when the event is created in a background task.
                pass
            except Exception as e:
                # If we get any other exception, we log it but do not raise it.
                cls.logger().warning(
                    f"Unable to resolve the user_agent: {repr(e)}", exc_info=e
                )

        if not time:
            time = utc_now()

        data_source = license_pool.data_source if license_pool else None
        identifier = license_pool.identifier if license_pool else None
        collection = license_pool.collection if license_pool else None
        work = license_pool.work if license_pool else None
        edition = work.presentation_edition if work else None
        if not edition and license_pool:
            edition = license_pool.presentation_edition

        return cls(
            type=event_type,
            start=time,
            library_id=library.id,
            library_name=library.name,
            library_short_name=library.short_name,
            old_value=old_value,
            new_value=new_value,
            license_pool_id=license_pool.id if license_pool else None,
            publisher=edition.publisher if edition else None,
            imprint=edition.imprint if edition else None,
            issued=edition.issued if edition else None,
            published=(
                datetime.combine(edition.published, datetime.min.time())
                if edition and edition.published
                else None
            ),
            medium=edition.medium if edition else None,
            collection=collection.name if collection else None,
            collection_id=collection.id if collection else None,
            identifier_type=identifier.type if identifier else None,
            identifier=identifier.identifier if identifier else None,
            data_source=data_source.name if data_source else None,
            distributor=data_source.name if data_source else None,
            audience=work.audience if work else None,
            fiction=work.fiction if work else None,
            summary_text=work.summary_text if work else None,
            quality=work.quality if work else None,
            rating=work.rating if work else None,
            popularity=work.popularity if work else None,
            genre=(
                ", ".join(map(lambda genre: genre.name, work.genres)) if work else None
            ),
            availability_time=(
                license_pool.availability_time if license_pool else None
            ),
            licenses_owned=license_pool.licenses_owned if license_pool else None,
            licenses_available=(
                license_pool.licenses_available if license_pool else None
            ),
            licenses_reserved=(
                license_pool.licenses_reserved if license_pool else None
            ),
            patrons_in_hold_queue=(
                license_pool.patrons_in_hold_queue if license_pool else None
            ),
            title=work.title if work else None,
            author=work.author if work else None,
            series=work.series if work else None,
            series_position=work.series_position if work else None,
            language=work.language if work else None,
            open_access=license_pool.open_access if license_pool else None,
            user_agent=user_agent,
            patron_uuid=patron.uuid if patron else None,
        )
