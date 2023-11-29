from __future__ import annotations

import urllib.error
import urllib.parse
import urllib.request

from pymarc import Field, Record, Subfield
from sqlalchemy import select

from core.config import Configuration
from core.marc import Annotator, MarcExporterLibrarySettings
from core.model import (
    ConfigurationSetting,
    Edition,
    Identifier,
    Library,
    LicensePool,
    Session,
    Work,
)
from core.model.discovery_service_registration import DiscoveryServiceRegistration


class LibraryAnnotator(Annotator):
    def __init__(self, library: Library) -> None:
        super().__init__()
        self.library = library
        _db = Session.object_session(library)
        self.base_url = ConfigurationSetting.sitewide(
            _db, Configuration.BASE_URL_KEY
        ).value

    def annotate_work_record(
        self,
        work: Work,
        active_license_pool: LicensePool,
        edition: Edition,
        identifier: Identifier,
        record: Record,
        settings: MarcExporterLibrarySettings | None,
    ) -> None:
        super().annotate_work_record(
            work, active_license_pool, edition, identifier, record, settings
        )

        if settings is None:
            return

        if settings.organization_code:
            self.add_marc_organization_code(record, settings.organization_code)

        if settings.include_summary:
            self.add_summary(record, work)

        if settings.include_genres:
            self.add_simplified_genres(record, work)

        self.add_web_client_urls(record, self.library, identifier, settings)

    def add_web_client_urls(
        self,
        record: Record,
        library: Library,
        identifier: Identifier,
        exporter_settings: MarcExporterLibrarySettings,
    ) -> None:
        _db = Session.object_session(library)
        settings = []

        marc_setting = exporter_settings.web_client_url
        if marc_setting:
            settings.append(marc_setting)

        settings += [
            s.web_client
            for s in _db.execute(
                select(DiscoveryServiceRegistration.web_client).where(
                    DiscoveryServiceRegistration.library == library,
                    DiscoveryServiceRegistration.web_client != None,
                )
            ).all()
        ]

        qualified_identifier = urllib.parse.quote(
            f"{identifier.type}/{identifier.identifier}", safe=""
        )

        for web_client_base_url in settings:
            link = "{}/{}/works/{}".format(
                self.base_url,
                library.short_name,
                qualified_identifier,
            )
            encoded_link = urllib.parse.quote(link, safe="")
            url = f"{web_client_base_url}/book/{encoded_link}"
            record.add_field(
                Field(
                    tag="856",
                    indicators=["4", "0"],
                    subfields=[Subfield(code="u", value=url)],
                )
            )
