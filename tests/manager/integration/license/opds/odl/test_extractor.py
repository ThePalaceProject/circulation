from __future__ import annotations

import datetime
from functools import partial

from palace.manager.integration.license.opds.odl.extractor import OPDS2WithODLExtractor
from palace.manager.opds import opds2, rwpm
from palace.manager.opds.lcp.status import LoanStatus
from palace.manager.opds.odl.info import Checkouts, LicenseInfo, LicenseStatus
from palace.manager.opds.odl.odl import License, LicenseMetadata
from palace.manager.opds.odl.terms import Terms
from palace.manager.opds.opds2 import StrictLink
from palace.manager.util.datetime_helpers import utc_now


class TestOPDS2WithODLExtractor:
    def test__extract_license_data(self) -> None:
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

        # Identifier mismatch returns None
        license_info = create_license_info(
            identifier="two identifier",
        )
        license = create_license(
            metadata=create_metadata(identifier="one identifier"),
        )
        assert (
            OPDS2WithODLExtractor._extract_license_data(license_info, license) is None
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
        license_data = OPDS2WithODLExtractor._extract_license_data(
            license_info, license
        )
        assert license_data is not None
        assert license_data.status == LicenseStatus.unavailable

        # Concurrency mismatch makes license unavailable
        license_info = create_license_info(terms=Terms(concurrency=12))
        license = create_license(
            metadata=create_metadata(terms=Terms(concurrency=11)),
        )
        license_data = OPDS2WithODLExtractor._extract_license_data(
            license_info, license
        )
        assert license_data is not None
        assert license_data.status == LicenseStatus.unavailable

        # Good data returns LicenseData
        license_info = create_license_info()
        license = create_license()
        license_data = OPDS2WithODLExtractor._extract_license_data(
            license_info, license
        )
        assert license_data is not None
        assert license_data.status == LicenseStatus.available
