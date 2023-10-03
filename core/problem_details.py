from flask_babel import lazy_gettext as _

from core.util.problem_detail import ProblemDetail as pd

# Generic problem detail documents that recapitulate HTTP errors.
# call detailed() to add more specific information.

INVALID_INPUT = pd(
    "http://librarysimplified.org/terms/problem/invalid-input",
    400,
    _("Invalid input."),
    _("You provided invalid or unrecognized input."),
)

INVALID_CREDENTIALS = pd(
    "http://librarysimplified.org/terms/problem/credentials-invalid",
    401,
    _("Invalid credentials"),
    _("Valid credentials are required."),
)

METHOD_NOT_ALLOWED = pd(
    "http://librarysimplified.org/terms/problem/method-not-allowed",
    405,
    _("Method not allowed"),
    _("The HTTP method you used is not allowed on this resource."),
)

UNSUPPORTED_MEDIA_TYPE = pd(
    "http://librarysimplified.org/terms/problem/unsupported-media-type",
    415,
    _("Unsupported media type"),
    _("You submitted an unsupported media type."),
)

PAYLOAD_TOO_LARGE = pd(
    "http://librarysimplified.org/terms/problem/unsupported-media-type",
    413,
    _("Payload too large"),
    _("You submitted a document that was too large."),
)

INTERNAL_SERVER_ERROR = pd(
    "http://librarysimplified.org/terms/problem/internal-server-error",
    500,
    _("Internal server error."),
    _("Internal server error"),
)

# Problem detail documents that are specific to the Library Simplified
# domain.

INVALID_URN = pd(
    "http://librarysimplified.org/terms/problem/could-not-parse-urn",
    400,
    _("Invalid URN"),
    _("Could not parse identifier."),
)

UNRECOGNIZED_DATA_SOURCE = pd(
    "http://librarysimplified.org/terms/problem/unrecognized-data-source",
    400,
    _("Unrecognized data source."),
    _("I don't know anything about that data source."),
)

INTEGRATION_ERROR = pd(
    "http://librarysimplified.org/terms/problem/remote-integration-failed",
    502,
    _("Third-party service failed."),
    _("A third-party service has failed."),
)

INVALID_RESET_PASSWORD_TOKEN = pd(
    "http://librarysimplified.org/terms/problem/reset-password-token-expired",
    status_code=401,
    title=_("Invalid reset password token"),
    detail=_("Provided reset password token is not valid."),
)
