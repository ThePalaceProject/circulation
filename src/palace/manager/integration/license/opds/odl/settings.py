from __future__ import annotations

from typing import Annotated, Self

from flask_babel import lazy_gettext as _
from pydantic import NonNegativeInt, PositiveInt, model_validator

from palace.manager.api.admin.problem_details import INCOMPLETE_CONFIGURATION
from palace.manager.api.circulation.settings import BaseCirculationEbookLoanSettings
from palace.manager.api.lcp.hash import HashingAlgorithm
from palace.manager.integration.license.opds.opds2.settings import OPDS2ImporterSettings
from palace.manager.integration.license.opds.requests import OpdsAuthType
from palace.manager.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    SettingsValidationError,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.pydantic import HttpUrl


class OPDS2WithODLSettings(OPDS2ImporterSettings):
    encryption_algorithm: Annotated[
        HashingAlgorithm,
        ConfigurationFormItem(
            label=_("Passphrase encryption algorithm"),
            description=_("Algorithm used for encrypting the passphrase."),
            type=ConfigurationFormItemType.SELECT,
            required=False,
            options={alg: alg.name for alg in HashingAlgorithm},
        ),
    ] = HashingAlgorithm.SHA256
    passphrase_hint_url: Annotated[
        HttpUrl,
        ConfigurationFormItem(
            label=_("Passphrase hint URL"),
            description=_(
                "Hint URL available to the user when opening an LCP protected publication."
            ),
            type=ConfigurationFormItemType.TEXT,
            required=True,
        ),
    ] = "https://lyrasis.zendesk.com/"
    passphrase_hint: Annotated[
        str,
        ConfigurationFormItem(
            label=_("Passphrase hint"),
            description=_(
                "Hint displayed to the user when opening an LCP protected publication."
            ),
            type=ConfigurationFormItemType.TEXT,
            required=True,
        ),
    ] = "View the help page for more information."
    default_reservation_period: Annotated[
        PositiveInt | None,
        ConfigurationFormItem(
            label=_("Default Reservation Period (in Days)"),
        ),
    ] = Collection.STANDARD_DEFAULT_RESERVATION_PERIOD
    auth_type: Annotated[
        OpdsAuthType,
        ConfigurationFormItem(
            label="Feed authentication type",
            description="Method used to authenticate when interacting with the feed.",
            type=ConfigurationFormItemType.SELECT,
            required=True,
            options={auth: auth.value for auth in OpdsAuthType},
        ),
    ] = OpdsAuthType.BASIC
    password: Annotated[
        str | None,
        ConfigurationFormItem(
            label=_("Library's API password"),
            required=False,
        ),
    ] = None
    username: Annotated[
        str | None,
        ConfigurationFormItem(
            label=_("Library's API username"),
            required=False,
        ),
    ] = None
    external_account_id: Annotated[
        HttpUrl,
        ConfigurationFormItem(
            label=_("ODL feed URL"),
            required=True,
        ),
    ]
    skipped_license_formats: Annotated[
        list[str],
        ConfigurationFormItem(
            label=_("Skipped license formats"),
            description=_(
                "List of license formats that will NOT be imported into Circulation Manager."
            ),
            type=ConfigurationFormItemType.LIST,
            required=False,
        ),
    ] = ["text/html"]

    loan_limit: Annotated[
        PositiveInt | None,
        ConfigurationFormItem(
            label=_("Loan limit per patron"),
            description=_(
                "The maximum number of books a patron can have loaned out at any given time."
            ),
            type=ConfigurationFormItemType.NUMBER,
            required=False,
        ),
    ] = None

    hold_limit: Annotated[
        NonNegativeInt | None,
        ConfigurationFormItem(
            label=_("Hold limit per patron"),
            description=_(
                "The maximum number of books from this collection that a patron can "
                "have on hold at any given time. "
                "<br>A value of 0 means that holds are NOT permitted."
                "<br>No value means that no limit is imposed by this setting."
            ),
            type=ConfigurationFormItemType.NUMBER,
            required=False,
        ),
    ] = None

    @model_validator(mode="after")
    def validate_auth_parameters(self) -> Self:
        missing = []
        if self.auth_type == OpdsAuthType.BASIC or self.auth_type == OpdsAuthType.OAUTH:
            if not self.username:
                missing.append("username")
            if not self.password:
                missing.append("password")

        if missing:
            labels = ", ".join(
                [f"'{self.get_form_field_label(name)}'" for name in missing]
            )
            fields = "fields" if len(missing) > 1 else "field"
            raise SettingsValidationError(
                problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                    f"Missing required {fields} for {self.auth_type.value} authentication: {labels}"
                )
            )
        return self


class OPDS2WithODLLibrarySettings(BaseCirculationEbookLoanSettings):
    pass
