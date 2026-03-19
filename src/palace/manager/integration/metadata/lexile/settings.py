"""Settings for the MetaMetrics Lexile DB integration."""

from typing import Annotated

from palace.manager.integration.metadata.base import MetadataServiceSettings
from palace.manager.integration.settings import FormMetadata

# Default sample ISBN for self-test: "The Hobbit" - widely available in Lexile DB
DEFAULT_SAMPLE_ISBN = "9780547928227"


class LexileDBSettings(MetadataServiceSettings):
    """Settings for the MetaMetrics Lexile DB API."""

    username: Annotated[
        str,
        FormMetadata(label="Username"),
    ]
    password: Annotated[
        str,
        FormMetadata(label="Password"),
    ]
    base_url: Annotated[
        str,
        FormMetadata(
            label="Base URL",
            description="The Lexile API base URL provided by MetaMetrics (e.g. https://api.example.com)",
        ),
    ]
    sample_identifier: Annotated[
        str,
        FormMetadata(
            label="Sample ISBN for self-test",
            description="ISBN used when running the connection self-test. "
            "Leave blank to use the default (9780547928227).",
        ),
    ] = DEFAULT_SAMPLE_ISBN
