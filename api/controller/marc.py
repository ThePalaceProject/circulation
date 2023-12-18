from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

import flask
from flask import Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from core.integration.goals import Goals
from core.marc import MARCExporter
from core.model import (
    Collection,
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
    Library,
    MarcFile,
)
from core.service.storage.s3 import S3Service


@dataclass
class MarcFileDeltaResult:
    key: str
    since: datetime
    created: datetime


@dataclass
class MarcFileFullResult:
    key: str
    created: datetime


@dataclass
class MarcFileCollectionResult:
    full: MarcFileFullResult | None = None
    deltas: list[MarcFileDeltaResult] = field(default_factory=list)


class MARCRecordController:
    DOWNLOAD_TEMPLATE = """
<html lang="en">
<head><meta charset="utf8"></head>
<body>
%(body)s
</body>
</html>"""

    def __init__(self, storage_service: S3Service | None) -> None:
        self.storage_service = storage_service

    @staticmethod
    def library() -> Library:
        return flask.request.library  # type: ignore[no-any-return,attr-defined]

    @staticmethod
    def has_integration(session: Session, library: Library) -> bool:
        integration_query = (
            select(IntegrationLibraryConfiguration)
            .join(IntegrationConfiguration)
            .where(
                IntegrationConfiguration.goal == Goals.CATALOG_GOAL,
                IntegrationConfiguration.protocol == MARCExporter.__name__,
                IntegrationLibraryConfiguration.library == library,
            )
        )
        integration = session.execute(integration_query).one_or_none()
        return integration is not None

    @staticmethod
    def get_files(
        session: Session, library: Library
    ) -> dict[str, MarcFileCollectionResult]:
        marc_files = session.execute(
            select(
                IntegrationConfiguration.name,
                MarcFile.key,
                MarcFile.since,
                MarcFile.created,
            )
            .select_from(MarcFile)
            .join(Collection)
            .join(IntegrationConfiguration)
            .join(IntegrationLibraryConfiguration)
            .where(
                MarcFile.library == library,
                Collection.export_marc_records == True,
                IntegrationLibraryConfiguration.library == library,
            )
            .order_by(
                IntegrationConfiguration.name,
                MarcFile.created.desc(),
            )
        ).all()

        files_by_collection: dict[str, MarcFileCollectionResult] = defaultdict(
            MarcFileCollectionResult
        )
        for file_row in marc_files:
            if file_row.since is None:
                full_file_result = MarcFileFullResult(
                    key=file_row.key,
                    created=file_row.created,
                )
                if files_by_collection[file_row.name].full is not None:
                    # We already have a newer full file, so skip this one.
                    continue
                files_by_collection[file_row.name].full = full_file_result
            else:
                delta_file_result = MarcFileDeltaResult(
                    key=file_row.key,
                    since=file_row.since,
                    created=file_row.created,
                )
                files_by_collection[file_row.name].deltas.append(delta_file_result)
        return files_by_collection

    def download_page_body(self, session: Session, library: Library) -> str:
        time_format = "%B %-d, %Y"

        # Check if a MARC exporter is configured, so we can show a
        # message if it's not.
        integration = self.has_integration(session, library)

        if not integration:
            return (
                "<p>"
                + "No MARC exporter is currently configured for this library."
                + "</p>"
            )

        if not self.storage_service:
            return "<p>" + "No storage service is currently configured." + "</p>"

        # Get the MARC files for this library.
        marc_files = self.get_files(session, library)

        if len(marc_files) == 0:
            # Are there any collections configured to export MARC records?
            if any(c.export_marc_records for c in library.collections):
                return "<p>" + "MARC files aren't ready to download yet." + "</p>"
            else:
                return (
                    "<p>"
                    + "No collections are configured to export MARC records."
                    + "</p>"
                )

        body = ""
        for collection_name, files in marc_files.items():
            body += "<section>"
            body += f"<h3>{collection_name}</h3>"
            if files.full is not None:
                file = files.full
                full_url = self.storage_service.generate_url(file.key)
                full_label = (
                    f"Full file - last updated {file.created.strftime(time_format)}"
                )
                body += f'<a href="{full_url}">{full_label}</a>'

                if files.deltas:
                    body += f"<h4>Update-only files</h4>"
                    body += "<ul>"
                    for update in files.deltas:
                        update_url = self.storage_service.generate_url(update.key)
                        update_label = f"Updates from {update.since.strftime(time_format)} to {update.created.strftime(time_format)}"
                        body += f'<li><a href="{update_url}">{update_label}</a></li>'
                    body += "</ul>"

            body += "</section>"
            body += "<br />"

        return body

    def download_page(self) -> Response:
        library = self.library()
        body = "<h2>Download MARC files for %s</h2>" % library.name

        session = Session.object_session(library)
        body += self.download_page_body(session, library)

        html = self.DOWNLOAD_TEMPLATE % dict(body=body)
        headers = dict()
        headers["Content-Type"] = "text/html"
        return Response(html, 200, headers)
