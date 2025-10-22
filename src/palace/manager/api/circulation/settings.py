from __future__ import annotations

import datetime
from typing import Annotated

from flask_babel import lazy_gettext as _
from pydantic import PositiveInt

from palace.manager.api.admin.config import Configuration as AdminConfiguration
from palace.manager.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
)
from palace.manager.sqlalchemy.constants import IntegrationConfigurationConstants
from palace.manager.sqlalchemy.model.collection import Collection


class BaseCirculationEbookLoanSettings(BaseSettings):
    """A mixin for settings that apply to ebook loans."""

    ebook_loan_duration: Annotated[
        PositiveInt | None,
        ConfigurationFormItem(
            label=_("Ebook Loan Duration (in Days)"),
            type=ConfigurationFormItemType.NUMBER,
            description=_(
                "When a patron uses SimplyE to borrow an ebook from this collection, SimplyE will ask for a loan that lasts this number of days. This must be equal to or less than the maximum loan duration negotiated with the distributor."
            ),
        ),
    ] = Collection.STANDARD_DEFAULT_LOAN_PERIOD


class BaseCirculationLoanSettings(BaseSettings):
    """A mixin for settings that apply to loans."""

    default_loan_duration: Annotated[
        PositiveInt | None,
        ConfigurationFormItem(
            label=_("Default Loan Period (in Days)"),
            type=ConfigurationFormItemType.NUMBER,
            description=_(
                "Until it hears otherwise from the distributor, this server will assume that any given loan for this library from this collection will last this number of days. This number is usually a negotiated value between the library and the distributor. This only affects estimates&mdash;it cannot affect the actual length of loans."
            ),
        ),
    ] = Collection.STANDARD_DEFAULT_LOAN_PERIOD


class BaseCirculationApiSettings(BaseSettings):
    _additional_form_fields = {
        "export_marc_records": ConfigurationFormItem(
            label="Generate MARC Records",
            type=ConfigurationFormItemType.SELECT,
            description="Generate MARC Records for this collection. This setting only applies if a MARC Exporter is configured.",
            options={
                "false": "Do not generate MARC records",
                "true": "Generate MARC records",
            },
        )
    }

    subscription_activation_date: Annotated[
        datetime.date | None,
        ConfigurationFormItem(
            label=_("Collection Subscription Activation Date"),
            type=ConfigurationFormItemType.DATE,
            description=(
                "A date before which this collection is considered inactive. Associated libraries"
                " will not be considered to be subscribed until this date). If not specified,"
                " it will not restrict any associated library's subscription status."
            ),
            required=False,
            hidden=AdminConfiguration.admin_client_settings().hide_subscription_config,
        ),
    ] = None
    subscription_expiration_date: Annotated[
        datetime.date | None,
        ConfigurationFormItem(
            label=_("Collection Subscription Expiration Date"),
            type=ConfigurationFormItemType.DATE,
            description=(
                "A date after which this collection is considered inactive. Associated libraries"
                " will not be considered to be subscribed beyond this date). If not specified,"
                " it will not restrict any associated library's subscription status."
            ),
            required=False,
            hidden=AdminConfiguration.admin_client_settings().hide_subscription_config,
        ),
    ] = None

    lane_priority_level: Annotated[
        int,
        ConfigurationFormItem(
            label=_("Lane Priority Level"),
            type=ConfigurationFormItemType.SELECT,
            options={str(index + 1): value for index, value in enumerate(range(1, 11))},
            description=(
                "An integer between 1 (lowest priority) and 10 (highest) inclusive indicating "
                "the priority of collection's contents in lanes.  In other words, a lower number relative to "
                "other collections will push the contents of this collection to the bottom of any lane in which this "
                "collection's contents appear. The default value "
                f"({IntegrationConfigurationConstants.DEFAULT_LANE_PRIORITY_LEVEL}) "
                "will be used if no value is provided here. If two or more collections contain overlapping sets of "
                "books, the highest of the lane priorities will be used when ordering a book's position in a lane."
            ),
            required=False,
        ),
    ] = IntegrationConfigurationConstants.DEFAULT_LANE_PRIORITY_LEVEL
