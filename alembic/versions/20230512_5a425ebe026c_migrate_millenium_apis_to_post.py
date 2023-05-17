"""Migrate millenium APIs to POST

Revision ID: 5a425ebe026c
Revises: f9985f6b7767
Create Date: 2023-05-12 08:36:16.603825+00:00

"""
import logging
import re

from alembic import op

# revision identifiers, used by Alembic.
revision = "5a425ebe026c"
down_revision = "f9985f6b7767"
branch_labels = None
depends_on = None


log = logging.getLogger(f"palace.migration.{revision}")
log.setLevel(logging.INFO)
log.disabled = False

KEY = "use_post_requests"


def match_expression(url: str) -> bool:
    expressions = [
        r"^https?://vlc\.(.*?\.)?palaceproject\.io",
        r"^https?://vlc\.thepalaceproject\.org",
        r"^(http://)?localhost",
    ]
    for expr in expressions:
        match = re.match(expr, url)
        if match is not None:
            return True

    return False


def upgrade() -> None:
    """Set 'use_post_requests' to 'true' for 'api.millenium' integrations.
    But only those that are for the following urls
    - vlc.thepalaceproject.org
    - vlc.*.palaceproject.io
    - localhost
    """
    conn = op.get_bind()
    # Find the relevant external integrations
    result_ids = conn.execute(
        "SELECT id FROM externalintegrations where protocol='api.millenium_patron'"
    )

    # Query to pull specific config values
    config_query = "SELECT value from configurationsettings where external_integration_id={integration_id} and key='{key}'"

    # For each millenium integration found
    for [integration_id] in result_ids or []:
        # Pull the URL setting
        config_results = conn.execute(
            config_query.format(integration_id=integration_id, key="url")
        )
        url_results = list(config_results)
        if config_results and len(url_results) > 0:
            url = url_results[0][0]
        else:
            log.info(f"No URL found for integration: {integration_id}")
            continue

        # Check if it is something we want to change at all
        if not match_expression(url):
            log.info(f"Not an internal millenium implementation: {url}")
            continue

        # Pull the post requests setting
        config_results = conn.execute(
            config_query.format(integration_id=integration_id, key=f"{KEY}")
        )
        post_results = list(config_results)
        # This setting may or may not exist
        if config_results and len(post_results) > 0:
            use_post = post_results[0][0]
        else:
            use_post = None

        # Make the changes
        if use_post is None:
            log.info(f"'{KEY}' setting does not exist for {url}, creating...")
            conn.execute(
                "INSERT INTO configurationsettings(external_integration_id, library_id, key, value)"
                + f" VALUES ({integration_id}, NULL, '{KEY}', 'true')"
            )
        elif use_post == "false":
            log.info(f"'{KEY}' is disabled for {url}, enabling...")
            conn.execute(
                "UPDATE configurationsettings SET value='true'"
                + f"WHERE external_integration_id={integration_id} and key='{KEY}'"
            )
        else:
            log.info(f"'{KEY}' for {url} is already {use_post}, ignoring...")


def downgrade() -> None:
    """Set all internal millenium integrations to not use POST"""
    conn = op.get_bind()
    result_ids = conn.execute(
        "SELECT id FROM externalintegrations where protocol='api.millenium_patron'"
    )
    for [integration_id] in result_ids:
        log.info(f"Forcing '{KEY}' to 'false' for {integration_id}")
        conn.execute(
            "UPDATE configurationsettings SET value='false'"
            + f" WHERE external_integration_id={integration_id} AND key='{KEY}'"
        )


if __name__ == "__main__":
    # Some testing code
    assert match_expression("http://vlc.dev.palaceproject.io/api") == True
    assert match_expression("https://vlc.staging.palaceproject.io/PATRONAPI") == True
    assert match_expression("localhost:6500/PATRONAPI") == True
    assert match_expression("http://localhost:6500/api") == True
    assert match_expression("https://vlc.thepalaceproject.org/anything...") == True
    assert match_expression("https://vendor.millenium.com/PATRONAPI") == False

    import sys

    log.addHandler(logging.StreamHandler(sys.stdout))
    log.info("Match expression tests passed!!")
