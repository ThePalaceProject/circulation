from flask_babel import lazy_gettext as _

from palace.manager.util.problem_detail import ProblemDetail as pd

SAML_METADATA_NOT_CONFIGURED = pd(
    "http://palaceproject.io/terms/problem/saml/metadata-not-configured",
    status_code=404,
    title=_("SAML metadata not configured."),
    detail=_("SAML metadata is not configured."),
)

SAML_INCORRECT_METADATA = pd(
    "http://librarysimplified.org/terms/problem/saml/incorrect-metadata-format",
    status_code=400,
    title=_("SAML metadata has an incorrect format."),
    detail=_("SAML metadata has an incorrect format."),
)

SAML_INCORRECT_PRIVATE_KEY = pd(
    "http://palaceproject.io/terms/problem/saml/incorrect-private-key-format",
    status_code=400,
    title=_("SAML private key has an incorrect format."),
    detail=_("SAML private key has an incorrect format."),
)

SAML_GENERIC_PARSING_ERROR = pd(
    "http://librarysimplified.org/terms/problem/saml/generic-parsing-error",
    status_code=500,
    title=_("Unexpected error."),
    detail=_(
        "An unexpected error occurred during validation of SAML authentication settings."
    ),
)

SAML_INCORRECT_FILTRATION_EXPRESSION = pd(
    "http://librarysimplified.org/terms/problem/saml/incorrect-filtration-expression-format",
    status_code=400,
    title=_("SAML filtration expression has an incorrect format."),
    detail=_("SAML filtration expression has an incorrect format."),
)

SAML_INCORRECT_PATRON_ID_REGULAR_EXPRESSION = pd(
    "http://librarysimplified.org/terms/problem/saml/incorrect-patron-id-regex",
    status_code=400,
    title=_("SAML patron ID regular expression has an incorrect format."),
    detail=_("SAML patron ID regular expression has an incorrect format."),
)
