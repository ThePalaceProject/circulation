import datetime
import json
import uuid
from typing import Dict, List, Optional, Union, cast

import dateutil
from flask_babel import lazy_gettext as _

from core.model.announcements import AnnouncementData
from core.problem_details import *
from core.util.problem_detail import ProblemError


class AnnouncementListValidator:
    DATE_FORMAT = "%Y-%m-%d"

    def __init__(
        self,
        maximum_announcements: int = 3,
        minimum_announcement_length: int = 15,
        maximum_announcement_length: int = 350,
        default_duration_days: int = 60,
    ) -> None:
        super().__init__()
        self.maximum_announcements = maximum_announcements
        self.minimum_announcement_length = minimum_announcement_length
        self.maximum_announcement_length = maximum_announcement_length
        self.default_duration_days = default_duration_days

    def validate_announcements(
        self, announcements: Union[str, List[Dict[str, str]]]
    ) -> Dict[uuid.UUID, AnnouncementData]:
        validated_announcements = {}
        bad_format = INVALID_INPUT.detailed(
            _(
                "Invalid announcement list format: %(announcements)r",
                announcements=announcements,
            )
        )
        if isinstance(announcements, str):
            try:
                announcements = json.loads(announcements)
            except ValueError:
                raise ProblemError(bad_format)
        if not isinstance(announcements, list):
            raise ProblemError(bad_format)
        if len(announcements) > self.maximum_announcements:
            raise ProblemError(
                INVALID_INPUT.detailed(
                    _(
                        "Too many announcements: maximum is %(maximum)d",
                        maximum=self.maximum_announcements,
                    )
                )
            )

        for announcement in announcements:
            validated = self.validate_announcement(announcement)
            id = cast(uuid.UUID, validated.id)
            if id in validated_announcements:
                raise ProblemError(
                    INVALID_INPUT.detailed(_("Duplicate announcement ID: %s" % id))
                )
            validated_announcements[id] = validated
        return validated_announcements

    def validate_announcement(self, announcement: Dict[str, str]) -> AnnouncementData:
        if not isinstance(announcement, dict):
            raise ProblemError(
                INVALID_INPUT.detailed(
                    _(
                        "Invalid announcement format: %(announcement)r",
                        announcement=announcement,
                    )
                )
            )

        id_str = announcement.get("id")
        if id_str:
            try:
                id = uuid.UUID(id_str)
            except ValueError:
                raise ProblemError(
                    INVALID_INPUT.detailed(
                        _("Invalid announcement ID: %(id)s", id=id_str)
                    )
                )
        else:
            id = uuid.uuid4()

        for required_field in ("content",):
            if required_field not in announcement:
                raise ProblemError(
                    INVALID_INPUT.detailed(
                        _("Missing required field: %(field)s", field=required_field)
                    )
                )

        # Validate the content of the announcement.
        content = self.validate_length(
            announcement["content"],
            self.minimum_announcement_length,
            self.maximum_announcement_length,
        )

        # Validate the dates associated with the announcement
        today_local = datetime.date.today()

        start = self.validate_date("start", announcement.get("start", today_local))
        default_finish = start + datetime.timedelta(days=self.default_duration_days)
        day_after_start = start + datetime.timedelta(days=1)
        finish = self.validate_date(
            "finish",
            announcement.get("finish", default_finish),
            minimum=day_after_start,
        )

        # That's it!
        return AnnouncementData(
            id=id,
            content=content,
            start=start,
            finish=finish,
        )

    @classmethod
    def validate_length(self, value: str, minimum: int, maximum: int) -> str:
        """Validate the length of a string value.

        :param value: Proposed value for a field.
        :param minimum: Minimum length.
        :param maximum: Maximum length.

        :return: Raise ProblemError if validation fails; otherwise return the value.
        """
        if len(value) < minimum:
            raise ProblemError(
                INVALID_INPUT.detailed(
                    _(
                        "Value too short (%(length)d versus %(limit)d characters): %(value)s",
                        length=len(value),
                        limit=minimum,
                        value=value,
                    )
                )
            )

        if len(value) > maximum:
            raise ProblemError(
                INVALID_INPUT.detailed(
                    _(
                        "Value too long (%(length)d versus %(limit)d characters): %(value)s",
                        length=len(value),
                        limit=maximum,
                        value=value,
                    )
                )
            )
        return value

    @classmethod
    def validate_date(
        cls,
        field: str,
        value: Union[str, datetime.date],
        minimum: Optional[datetime.date] = None,
    ) -> datetime.date:
        """Validate a date value.

        :param field: Name of the field, used in error details.
        :param value: Proposed value for the field.
        :param minimum: The proposed value must not be earlier than
            this value.

        :return: A ProblemDetail if validation fails; otherwise a datetime.date.
        """
        if isinstance(value, str):
            try:
                # Unlike most of the dates in this application,
                # this is a date entered by an admin, so it should be
                # interpreted as server local time, not UTC.
                value = datetime.datetime.strptime(value, cls.DATE_FORMAT)
                value = value.replace(tzinfo=dateutil.tz.tzlocal())
                value = value.date()
            except ValueError as e:
                raise ProblemError(
                    INVALID_INPUT.detailed(
                        _(
                            "Value for %(field)s is not a date: %(date)s",
                            field=field,
                            date=value,
                        )
                    )
                )
        if minimum and value < minimum:
            raise ProblemError(
                INVALID_INPUT.detailed(
                    _(
                        "Value for %(field)s must be no earlier than %(minimum)s",
                        field=field,
                        minimum=minimum.strftime(cls.DATE_FORMAT),
                    )
                )
            )
        return value
