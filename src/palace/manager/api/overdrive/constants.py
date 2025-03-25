from __future__ import annotations

from palace.manager.sqlalchemy.constants import DataSourceConstants


class OverdriveConstants:
    PRODUCTION_SERVERS = "production"
    TESTING_SERVERS = "testing"

    # When associating an Overdrive account with a library, it's
    # necessary to also specify an "ILS name" obtained from
    # Overdrive. Components that don't authenticate patrons (such as
    # the metadata wrangler) don't need to set this value.
    ILS_NAME_KEY = "ils_name"
    ILS_NAME_DEFAULT = "default"


# An OverDrive defined constant indicating the "main" or parent account
# associated with an OverDrive collection.
OVERDRIVE_MAIN_ACCOUNT_ID = -1

OVERDRIVE_LABEL = DataSourceConstants.OVERDRIVE

# These are the formats that Overdrive allows in their normal public feeds.
OVERDRIVE_PUBLIC_FORMATS = {"ebook-overdrive", "audiobook-overdrive"}

# These formats can be "locked in" via an Overdrive API call
# if you have permissions to do so.
OVERDRIVE_LOCK_IN_FORMATS = {"ebook-epub-open", "ebook-epub-adobe", "ebook-pdf-open"}

# These are not real Overdrive formats; we use them internally so
# we can distinguish between (e.g.) using "audiobook-overdrive"
# to get into Overdrive Read, and using it to get a link to a
# manifest file.
OVERDRIVE_PALACE_MANIFEST_FORMATS = {
    "audiobook-overdrive-manifest": "audiobook-overdrive",
}

OVERDRIVE_FORMATS = (
    OVERDRIVE_PUBLIC_FORMATS
    | OVERDRIVE_LOCK_IN_FORMATS
    | OVERDRIVE_PALACE_MANIFEST_FORMATS.keys()
)

# The formats that indicate the book has been fulfilled on an
# incompatible platform and just can't be fulfilled in Place.
OVERDRIVE_INCOMPATIBLE_FORMATS = {"ebook-kindle"}

OVERDRIVE_STREAMING_FORMATS = {"ebook-overdrive", "audiobook-overdrive"}

OVERDRIVE_OPEN_FORMATS = {"ebook-epub-open", "ebook-pdf-open"}
