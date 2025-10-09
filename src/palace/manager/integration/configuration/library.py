from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Annotated, Any, Self, cast

import wcag_contrast_ratio
from pydantic import (
    EmailStr,
    PositiveFloat,
    PositiveInt,
    ValidationInfo,
    field_validator,
    model_validator,
)
from sqlalchemy.orm import Session

from palace.manager.api.admin.problem_details import (
    INCOMPLETE_CONFIGURATION,
    INVALID_CONFIGURATION_OPTION,
    UNKNOWN_LANGUAGE,
)
from palace.manager.core.config import Configuration
from palace.manager.core.entrypoint import EntryPoint
from palace.manager.core.facets import FacetConstants
from palace.manager.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
    SettingsValidationError,
)
from palace.manager.util.languages import LanguageCodes
from palace.manager.util.pydantic import HttpUrl


# The "level" property determines which admins will be able to modify the
# setting. Level 1 settings can be modified by anyone. Level 2 settings can be
# modified only by library managers and system admins (i.e. not by librarians).
# Level 3 settings can be changed only by system admins. If no level is
# specified, the setting will be treated as Level 1 by default.
class Level(IntEnum):
    ALL_ACCESS = 1
    SYS_ADMIN_OR_MANAGER = 2
    SYS_ADMIN_ONLY = 3


@dataclass(frozen=True)
class LibraryConfFormItem(ConfigurationFormItem):
    category: str = "Basic Information"
    level: Level = Level.ALL_ACCESS
    read_only: bool | None = None
    skip: bool | None = None
    paired: str | None = None

    def to_dict(
        self, db: Session, key: str, required: bool = False, default: Any = None
    ) -> tuple[int, dict[str, Any]]:
        """Serialize additional form items specific to library settings."""
        weight, item = super().to_dict(db, key, required, default)
        item["category"] = self.category
        item["level"] = self.level
        if self.read_only is not None:
            item["readOnly"] = self.read_only
        if self.skip is not None:
            item["skip"] = self.skip
        if self.paired is not None:
            item["paired"] = self.paired

        if (
            "default" in item
            and isinstance(item["default"], list)
            and len(item["default"]) == 0
        ):
            del item["default"]

        return weight, item


class LibrarySettings(BaseSettings):
    _additional_form_fields = {
        "name": LibraryConfFormItem(
            label="Name",
            description="The human-readable name of this library.",
            category="Basic Information",
            level=Level.SYS_ADMIN_ONLY,
            required=True,
            weight=-1,
        ),
        "short_name": LibraryConfFormItem(
            label="Short name",
            description="A short name of this library, to use when identifying it "
            "in scripts or URLs, e.g. 'NYPL'.",
            category="Basic Information",
            level=Level.SYS_ADMIN_ONLY,
            required=True,
            weight=-1,
        ),
        "logo": LibraryConfFormItem(
            label="Logo image",
            description="The image should be in GIF, PNG, or JPG format, approximately square, no larger than "
            "135x135 pixels, and look good on a light or dark mode background. "
            "Larger images will be accepted, but scaled down (maintaining aspect ratio) such that "
            "the longest dimension does not exceed 135 pixels.",
            category="Client Interface Customization",
            type=ConfigurationFormItemType.IMAGE,
            level=Level.ALL_ACCESS,
        ),
        "announcements": LibraryConfFormItem(
            label="Scheduled announcements",
            description="Announcements will be displayed to authenticated patrons.",
            type=ConfigurationFormItemType.ANNOUNCEMENTS,
            category="Announcements",
            level=Level.ALL_ACCESS,
        ),
    }

    website: HttpUrl = FormField(
        ...,
        form=LibraryConfFormItem(
            label="URL of the library's website",
            description='The library\'s main website, e.g. "https://www.nypl.org/" '
            "(not this Circulation Manager's URL).",
            category="Basic Information",
            level=Level.SYS_ADMIN_ONLY,
        ),
    )
    allow_holds: bool = FormField(
        True,
        form=LibraryConfFormItem(
            label="Allow books to be put on hold",
            type=ConfigurationFormItemType.SELECT,
            options={
                True: "Allow holds",
                False: "Disable holds",
            },
            category="Loans, Holds, & Fines",
            level=Level.SYS_ADMIN_ONLY,
        ),
    )
    enabled_entry_points: list[str] = FormField(
        [x.INTERNAL_NAME for x in EntryPoint.DEFAULT_ENABLED],
        form=LibraryConfFormItem(
            label="Enabled entry points",
            description="Patrons will see the selected entry points at the "
            "top level and in search results.",
            type=ConfigurationFormItemType.MENU,
            options={
                entrypoint.INTERNAL_NAME: EntryPoint.DISPLAY_TITLES[entrypoint]
                for entrypoint in EntryPoint.ENTRY_POINTS
            },
            category="Lanes & Filters",
            format="narrow",
            read_only=True,
            level=Level.SYS_ADMIN_ONLY,
        ),
    )
    featured_lane_size: PositiveInt = FormField(
        15,
        form=LibraryConfFormItem(
            label="Maximum number of books in the 'featured' lanes",
            category="Lanes & Filters",
            level=Level.ALL_ACCESS,
        ),
    )
    minimum_featured_quality: Annotated[
        float,
        FormField(
            ge=0,
            le=1,
            form=LibraryConfFormItem(
                label="Minimum quality for books that show up in 'featured' lanes",
                description="Between 0 and 1.",
                category="Lanes & Filters",
                level=Level.ALL_ACCESS,
            ),
        ),
    ] = Configuration.DEFAULT_MINIMUM_FEATURED_QUALITY
    facets_enabled_order: list[str] = FormField(
        FacetConstants.DEFAULT_ENABLED_FACETS[FacetConstants.ORDER_FACET_GROUP_NAME],
        form=LibraryConfFormItem(
            label="Allow patrons to sort by",
            type=ConfigurationFormItemType.MENU,
            options={
                facet: FacetConstants.FACET_DISPLAY_TITLES[facet]
                for facet in FacetConstants.ORDER_FACETS
            },
            category="Lanes & Filters",
            paired="facets_default_order",
            level=Level.SYS_ADMIN_OR_MANAGER,
        ),
    )
    facets_default_order: str = FormField(
        FacetConstants.ORDER_AUTHOR,
        form=LibraryConfFormItem(
            label="Default Sort by",
            type=ConfigurationFormItemType.SELECT,
            options={
                facet: FacetConstants.FACET_DISPLAY_TITLES[facet]
                for facet in FacetConstants.ORDER_FACETS
            },
            category="Lanes & Filters",
            skip=True,
        ),
    )
    facets_enabled_available: list[str] = FormField(
        FacetConstants.DEFAULT_ENABLED_FACETS[
            FacetConstants.AVAILABILITY_FACET_GROUP_NAME
        ],
        form=LibraryConfFormItem(
            label="Allow patrons to filter availability to",
            type=ConfigurationFormItemType.MENU,
            options={
                facet: FacetConstants.FACET_DISPLAY_TITLES[facet]
                for facet in FacetConstants.AVAILABILITY_FACETS
            },
            category="Lanes & Filters",
            paired="facets_default_available",
            level=Level.SYS_ADMIN_OR_MANAGER,
        ),
    )
    facets_default_available: str = FormField(
        FacetConstants.AVAILABLE_ALL,
        form=LibraryConfFormItem(
            label="Default Availability",
            type=ConfigurationFormItemType.SELECT,
            options={
                facet: FacetConstants.FACET_DISPLAY_TITLES[facet]
                for facet in FacetConstants.AVAILABILITY_FACETS
            },
            category="Lanes & Filters",
            skip=True,
        ),
    )

    library_description: str | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="A short description of this library",
            description="This will be shown to people who aren't sure they've chosen the right library.",
            category="Basic Information",
            level=Level.SYS_ADMIN_ONLY,
        ),
    )
    help_email: EmailStr | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Patron support email address",
            description="An email address a patron can use if they need help, "
            "e.g. 'palacehelp@yourlibrary.org'.",
            category="Basic Information",
            level=Level.ALL_ACCESS,
        ),
    )
    help_web: HttpUrl | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Patron support website",
            description="A URL for patrons to get help. Either this field or "
            "patron support email address must be provided.",
            category="Basic Information",
            level=Level.ALL_ACCESS,
        ),
    )
    copyright_designated_agent_email_address: EmailStr | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Copyright designated agent email",
            description="Patrons of this library should use this email "
            "address to send a DMCA notification (or other copyright "
            "complaint) to the library.<br/>If no value is specified here, "
            "the general patron support address will be used.",
            category="Patron Support",
            level=Level.SYS_ADMIN_OR_MANAGER,
        ),
    )
    configuration_contact_email_address: EmailStr | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="A point of contact for the organization responsible for configuring this library",
            description="This email address will be shared as part of "
            "integrations that you set up through this interface. It will not "
            "be shared with the general public. This gives the administrator "
            "of the remote integration a way to contact you about problems with "
            "this library's use of that integration.<br/>If no value is specified here, "
            "the general patron support address will be used.",
            category="Patron Support",
            level=Level.SYS_ADMIN_OR_MANAGER,
        ),
    )
    default_notification_email_address: EmailStr = FormField(
        "noreply@thepalaceproject.org",
        form=LibraryConfFormItem(
            label="Write-only email address for vendor hold notifications",
            description="This address must trash all email sent to it. Vendor hold notifications "
            "contain sensitive patron information, but "
            '<a href="https://confluence.nypl.org/display/SIM/About+Hold+Notifications" target="_blank">'
            "cannot be forwarded to patrons</a> because they contain vendor-specific instructions."
            "<br/>The default address will work, but for greater security, set up your own address that "
            "trashes all incoming email.",
            level=Level.SYS_ADMIN_OR_MANAGER,
        ),
    )
    color_scheme: str = FormField(
        "blue",
        form=LibraryConfFormItem(
            label="Mobile color scheme",
            description="This tells mobile applications what color scheme to use when rendering "
            "this library's OPDS feed.",
            type=ConfigurationFormItemType.SELECT,
            options={
                "amber": "Amber",
                "black": "Black",
                "blue": "Blue",
                "bluegray": "Blue Gray",
                "brown": "Brown",
                "cyan": "Cyan",
                "darkorange": "Dark Orange",
                "darkpurple": "Dark Purple",
                "green": "Green",
                "gray": "Gray",
                "indigo": "Indigo",
                "lightblue": "Light Blue",
                "orange": "Orange",
                "pink": "Pink",
                "purple": "Purple",
                "red": "Red",
                "teal": "Teal",
            },
            category="Client Interface Customization",
            level=Level.SYS_ADMIN_OR_MANAGER,
        ),
    )
    web_primary_color: str = FormField(
        "#377F8B",
        form=LibraryConfFormItem(
            label="Web primary color",
            description="This is the brand primary color for the web application. "
            "Must have sufficient contrast with white.",
            category="Client Interface Customization",
            type=ConfigurationFormItemType.COLOR,
            level=Level.SYS_ADMIN_OR_MANAGER,
        ),
    )
    web_secondary_color: str = FormField(
        "#D53F34",
        form=LibraryConfFormItem(
            label="Web secondary color",
            description="This is the brand secondary color for the web application. "
            "Must have sufficient contrast with white.",
            category="Client Interface Customization",
            type=ConfigurationFormItemType.COLOR,
            level=Level.SYS_ADMIN_OR_MANAGER,
        ),
    )
    web_css_file: HttpUrl | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Custom CSS file for web",
            description="Give web applications a CSS file to customize the catalog display.",
            category="Client Interface Customization",
            level=Level.SYS_ADMIN_ONLY,
        ),
    )
    web_header_links: list[str] = FormField(
        [],
        form=LibraryConfFormItem(
            label="Web header links",
            description="This gives web applications a list of links to display in the header. "
            "Specify labels for each link in the same order under 'Web header labels'.",
            category="Client Interface Customization",
            type=ConfigurationFormItemType.LIST,
            level=Level.SYS_ADMIN_OR_MANAGER,
        ),
    )
    web_header_labels: list[str] = FormField(
        [],
        form=LibraryConfFormItem(
            label="Web header labels",
            description="Labels for each link under 'Web header links'.",
            category="Client Interface Customization",
            type=ConfigurationFormItemType.LIST,
            level=Level.SYS_ADMIN_OR_MANAGER,
        ),
    )
    hidden_content_types: list[str] = FormField(
        [],
        form=LibraryConfFormItem(
            label="Hidden content types",
            description="A list of content types to hide from all clients, e.g. "
            "<code>application/pdf</code>. This can be left blank except to "
            "solve specific problems.",
            category="Client Interface Customization",
            type=ConfigurationFormItemType.LIST,
            level=Level.SYS_ADMIN_ONLY,
        ),
    )
    max_outstanding_fines: PositiveFloat | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Maximum amount in fines a patron can have before losing lending privileges",
            category="Loans, Holds, & Fines",
            level=Level.ALL_ACCESS,
        ),
    )
    loan_limit: PositiveInt | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Maximum number of books a patron can have on loan at once",
            description="Note: depending on distributor settings, a patron may be able to exceed "
            "the limit by checking out books directly from a distributor's app. They may also get "
            "a limit exceeded error before they reach these limits if a distributor has a smaller limit.",
            category="Loans, Holds, & Fines",
            level=Level.ALL_ACCESS,
        ),
    )
    hold_limit: PositiveInt | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Maximum number of books a patron can have on hold at once",
            description="Note: depending on distributor settings, a patron may be able to exceed "
            "the limit by placing holds directly from a distributor's app. They may also get "
            "a limit exceeded error before they reach these limits if a distributor has a smaller limit.",
            category="Loans, Holds, & Fines",
            level=Level.ALL_ACCESS,
        ),
    )
    terms_of_service: HttpUrl | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Terms of service URL",
            category="Links",
            level=Level.ALL_ACCESS,
        ),
    )
    privacy_policy: HttpUrl | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Privacy policy URL",
            category="Links",
            level=Level.ALL_ACCESS,
        ),
    )
    copyright: HttpUrl | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Copyright URL",
            category="Links",
            level=Level.SYS_ADMIN_OR_MANAGER,
        ),
    )
    about: HttpUrl | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="About URL",
            category="Links",
            level=Level.ALL_ACCESS,
        ),
    )
    license: HttpUrl | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="License URL",
            category="Links",
            level=Level.SYS_ADMIN_OR_MANAGER,
        ),
    )
    registration_url: HttpUrl | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Patron registration URL",
            description="A URL where someone who doesn't have a library card yet can sign up for one.",
            category="Patron Support",
            level=Level.ALL_ACCESS,
        ),
    )
    patron_password_reset: HttpUrl | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Password Reset Link",
            description="A link to a web page where a user can reset their virtual library card password",
            category="Patron Support",
            level=Level.SYS_ADMIN_ONLY,
        ),
    )
    large_collection_languages: list[str] | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="The primary languages represented in this library's collection",
            type=ConfigurationFormItemType.LIST,
            format="language-code",
            description="Each value can be either the full name of a language or an "
            '<a href="https://www.loc.gov/standards/iso639-2/php/code_list.php" target="_blank">'
            "ISO-639-2</a> language code.",
            category="Languages",
            level=Level.ALL_ACCESS,
        ),
    )
    small_collection_languages: list[str] | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Other major languages represented in this library's collection",
            type=ConfigurationFormItemType.LIST,
            format="language-code",
            description="Each value can be either the full name of a language or an "
            '<a href="https://www.loc.gov/standards/iso639-2/php/code_list.php" target="_blank">'
            "ISO-639-2</a> language code.",
            category="Languages",
            level=Level.ALL_ACCESS,
        ),
    )
    tiny_collection_languages: list[str] | None = FormField(
        None,
        form=LibraryConfFormItem(
            label="Other languages in this library's collection",
            type=ConfigurationFormItemType.LIST,
            format="language-code",
            description="Each value can be either the full name of a language or an "
            '<a href="https://www.loc.gov/standards/iso639-2/php/code_list.php" target="_blank">'
            "ISO-639-2</a> language code.",
            category="Languages",
            level=Level.ALL_ACCESS,
        ),
    )

    @model_validator(mode="after")
    def validate_require_help_email_or_website(self) -> Self:
        if self.help_email is None and self.help_web is None:
            help_email_label = self.get_form_field_label("help_email")
            help_website_label = self.get_form_field_label("help_web")
            raise SettingsValidationError(
                problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                    f"You must provide either '{help_email_label}' or '{help_website_label}'."
                )
            )

        return self

    @model_validator(mode="after")
    def validate_header_links(self) -> Self:
        """Verify that header links and labels are the same length."""
        header_links = self.web_header_links
        header_labels = self.web_header_labels
        if header_links and header_labels and len(header_links) != len(header_labels):
            raise SettingsValidationError(
                problem_detail=INVALID_CONFIGURATION_OPTION.detailed(
                    "There must be the same number of web header links and web header labels."
                )
            )
        return self

    @field_validator("web_primary_color", "web_secondary_color")
    @classmethod
    def validate_web_color_contrast(cls, value: str, info: ValidationInfo) -> str:
        """
        Verify that the web primary and secondary color both contrast
        well on white, as these colors will serve as button backgrounds with
        white test, as well as text color on white backgrounds.
        """

        def hex_to_rgb(hex: str) -> tuple[float, ...]:
            hex = hex.lstrip("#")
            return tuple(int(hex[i : i + 2], 16) / 255.0 for i in (0, 2, 4))

        passes = wcag_contrast_ratio.passes_AA(
            wcag_contrast_ratio.rgb(hex_to_rgb(value), hex_to_rgb("#ffffff"))
        )
        if not passes:
            check_url = (
                "https://contrast-ratio.com/#%23"
                + value[1:]
                + "-on-%23"
                + "#ffffff"[1:]
            )
            field_label = cls.get_form_field_label(cast(str, info.field_name))
            raise SettingsValidationError(
                problem_detail=INVALID_CONFIGURATION_OPTION.detailed(
                    f"The {field_label} doesn't have enough contrast to pass the WCAG 2.0 AA guidelines and "
                    f"will be difficult for some patrons to read. Check contrast <a href='{check_url}' "
                    "target='_blank'>here</a>."
                )
            )
        return value

    @field_validator(
        "large_collection_languages",
        "small_collection_languages",
        "tiny_collection_languages",
    )
    @classmethod
    def validate_language_codes(
        cls, value: list[str] | None, info: ValidationInfo
    ) -> list[str] | None:
        """Verify that collection languages are valid."""
        if value is not None:
            languages = set()
            for language in value:
                validated_language = LanguageCodes.string_to_alpha_3(language)
                if validated_language is None:
                    field_label = cls.get_form_field_label(cast(str, info.field_name))
                    raise SettingsValidationError(
                        problem_detail=UNKNOWN_LANGUAGE.detailed(
                            f'"{field_label}": "{language}" is not a valid language code.'
                        )
                    )
                languages.add(validated_language)
            return sorted(languages)
        return value
