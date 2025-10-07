from __future__ import annotations

from croniter import CroniterBadCronError, CroniterBadDateError, croniter
from flask_babel import lazy_gettext as _
from pydantic import field_validator

from palace.manager.integration.license.opds.opds1.settings import (
    OPDSImporterLibrarySettings,
    OPDSImporterSettings,
)
from palace.manager.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from palace.manager.opds import opds2
from palace.manager.sqlalchemy.constants import IdentifierType


class OPDS2ImporterSettings(OPDSImporterSettings):
    custom_accept_header: str = FormField(
        default="{}, {};q=0.9, */*;q=0.1".format(
            opds2.PublicationFeed.content_type(), "application/json"
        ),
        form=ConfigurationFormItem(
            label=_("Custom accept header"),
            description=_(
                "Some servers expect an accept header to decide which file to send. You can use */* if the server doesn't expect anything."
            ),
            type=ConfigurationFormItemType.TEXT,
            required=False,
        ),
    )

    ignored_identifier_types: list[str] = FormField(
        default=[],
        form=ConfigurationFormItem(
            label=_("List of identifiers that will be skipped"),
            description=_(
                "Circulation Manager will not be importing publications with identifiers having one of the selected types."
            ),
            type=ConfigurationFormItemType.MENU,
            required=False,
            options={
                identifier_type.value: identifier_type.value
                for identifier_type in IdentifierType
            },
            format="narrow",
        ),
    )

    reap_schedule: str | None = FormField(
        default=None,
        form=ConfigurationFormItem(
            label=_("Reap schedule (cron expression)"),
            description=_(
                "Cron expression for when to perform full import with reaping of identifiers not found in the feed. "
                "All schedules are evaluated in UTC timezone. Leave empty to disable reaping. See "
                "<a href='https://crontab.guru/' target='_blank'>crontab.guru</a> for help creating cron expressions. "
                "<br/> Examples: "
                "'0 0 * * 1' (midnight UTC every Monday), '0 5 3 * *' (5am on the on the 3rd day of the month)."
            ),
            type=ConfigurationFormItemType.TEXT,
            required=False,
        ),
    )

    @field_validator("reap_schedule")
    @classmethod
    def _validate_reap_schedule(cls, value: str | None) -> str | None:
        """
        Validate that the reap_schedule is a valid cron expression.

        The cron expression will be evaluated in UTC timezone.

        :param value: The cron expression to validate.
        :return: The validated cron expression.
        :raises ValueError: If the cron expression is invalid.
        """
        if value is None or value.strip() == "":
            return None

        try:
            # Attempt to create a croniter instance to validate the expression
            croniter(value)
            return value
        except (CroniterBadCronError, CroniterBadDateError) as e:
            # Provide detailed error message about what's wrong with the cron expression
            error_msg = f"Invalid cron expression '{value}': {str(e)}"
            raise ValueError(error_msg) from e


class OPDS2ImporterLibrarySettings(OPDSImporterLibrarySettings):
    pass
