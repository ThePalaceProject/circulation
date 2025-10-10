from __future__ import annotations

import datetime
from functools import partial

import pytest

from palace.manager.integration.license.opds.odl.extractor import OPDS2WithODLExtractor
from palace.manager.opds import opds2, rwpm
from palace.manager.opds.lcp.status import LoanStatus
from palace.manager.opds.odl.info import Checkouts, LicenseInfo, LicenseStatus
from palace.manager.opds.odl.odl import License, LicenseMetadata
from palace.manager.opds.odl.terms import Terms
from palace.manager.opds.opds2 import PublicationFeedNoValidation, StrictLink
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.files import OPDS2FilesFixture


class TestOPDS2WithODLExtractor:
    def test__extract_odl_license_data(self) -> None:
        create_metadata = partial(
            LicenseMetadata,
            identifier="identifier",
            created=utc_now(),
        )

        links = [
            StrictLink(
                rel=rwpm.LinkRelations.self,
                type=LicenseInfo.content_type(),
                href="self link",
            ),
            StrictLink(
                rel=opds2.AcquisitionLinkRelations.borrow,
                type=LoanStatus.content_type(),
                href="checkout link",
            ),
        ]

        create_license = partial(
            License,
            metadata=create_metadata(),
            links=links,
        )

        create_license_info = partial(
            LicenseInfo,
            identifier="identifier",
            status=LicenseStatus.available,
            checkouts=Checkouts(
                available=10,
            ),
        )

        # Expiry mismatch makes license unavailable
        license_info = create_license_info(
            terms=Terms(
                expires=utc_now() + datetime.timedelta(days=1),
            )
        )
        license = create_license(
            metadata=create_metadata(
                terms=Terms(
                    expires=utc_now() + datetime.timedelta(days=2),
                )
            ),
        )
        license_data = OPDS2WithODLExtractor._extract_odl_license_data(
            license_info, license
        )
        assert license_data is not None
        assert license_data.status == LicenseStatus.unavailable

        # Concurrency mismatch makes license unavailable
        license_info = create_license_info(terms=Terms(concurrency=12))
        license = create_license(
            metadata=create_metadata(terms=Terms(concurrency=11)),
        )
        license_data = OPDS2WithODLExtractor._extract_odl_license_data(
            license_info, license
        )
        assert license_data is not None
        assert license_data.status == LicenseStatus.unavailable

        # Good data returns LicenseData
        license_info = create_license_info()
        license = create_license()
        license_data = OPDS2WithODLExtractor._extract_odl_license_data(
            license_info, license
        )
        assert license_data is not None
        assert license_data.status == LicenseStatus.available

    def test__extract_contributor_roles(self) -> None:
        _extract_contributor_roles = OPDS2WithODLExtractor._extract_contributor_roles

        # If there are no roles, the function returns the default
        assert _extract_contributor_roles([], Contributor.Role.AUTHOR) == [
            Contributor.Role.AUTHOR
        ]

        # If the role is unknown, the default is used
        assert _extract_contributor_roles(["invalid"], Contributor.Role.AUTHOR) == [
            Contributor.Role.AUTHOR
        ]

        # Roles are not duplicated
        assert _extract_contributor_roles(
            [Contributor.Role.AUTHOR, Contributor.Role.AUTHOR], Contributor.Role.AUTHOR
        ) == [Contributor.Role.AUTHOR]
        assert _extract_contributor_roles(
            ["invalid", "invalid"], Contributor.Role.AUTHOR
        ) == [Contributor.Role.AUTHOR]

        # Role lookup is not case-sensitive
        assert _extract_contributor_roles(["aUtHoR"], Contributor.Role.ILLUSTRATOR) == [
            Contributor.Role.AUTHOR
        ]

        # Roles can be looked up via marc codes
        assert _extract_contributor_roles(["AUT"], Contributor.Role.ILLUSTRATOR) == [
            Contributor.Role.AUTHOR
        ]

    @pytest.mark.parametrize(
        "published,expected",
        [
            pytest.param(
                datetime.datetime(2015, 9, 29, 17, 0, tzinfo=datetime.UTC),
                datetime.date(2015, 9, 29),
                id="datetime with time info",
            ),
            pytest.param(
                datetime.datetime(2015, 9, 29, 0, 0),
                datetime.date(2015, 9, 29),
                id="datetime with no time info",
            ),
            pytest.param(
                datetime.date(2015, 9, 29),
                datetime.date(2015, 9, 29),
                id="date",
            ),
            pytest.param(
                None,
                None,
                id="none",
            ),
        ],
    )
    def test__extract_published_date(
        self,
        published: datetime.datetime | datetime.date | None,
        expected: datetime.date | None,
    ) -> None:
        assert OPDS2WithODLExtractor._extract_published_date(published) == expected

    def test_feed_next_url(
        self,
        opds2_files_fixture: OPDS2FilesFixture,
    ) -> None:
        # No next links
        feed = PublicationFeedNoValidation.model_validate_json(
            opds2_files_fixture.sample_data("feed.json")
        )
        assert OPDS2WithODLExtractor.feed_next_url(feed) is None

        # Feed has next link
        feed = PublicationFeedNoValidation.model_validate_json(
            opds2_files_fixture.sample_data("feed2.json")
        )
        assert (
            OPDS2WithODLExtractor.feed_next_url(feed)
            == "http://bookshelf-feed-demo.us-east-1.elasticbeanstalk.com/v1/publications?page=2&limit=100"
        )
