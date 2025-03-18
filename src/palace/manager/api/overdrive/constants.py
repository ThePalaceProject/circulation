from __future__ import annotations

from palace.manager.sqlalchemy.constants import DataSourceConstants


class OverdriveConstants:
    PRODUCTION_SERVERS = "production"
    TESTING_SERVERS = "testing"

    # The formats we care about.
    FORMATS = "ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open,audiobook-overdrive".split(
        ","
    )

    # These are not real Overdrive formats; we use them internally so
    # we can distinguish between (e.g.) using "audiobook-overdrive"
    # to get into Overdrive Read, and using it to get a link to a
    # manifest file.
    MANIFEST_INTERNAL_FORMATS = {
        "audiobook-overdrive-manifest",
        "ebook-overdrive-manifest",
    }

    # These formats can be delivered either as manifest files or as
    # links to websites that stream the content.
    STREAMING_FORMATS = [
        "ebook-overdrive",
        "audiobook-overdrive",
    ]

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
