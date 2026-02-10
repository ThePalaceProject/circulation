from __future__ import annotations

import json
from collections.abc import Callable
from unittest.mock import MagicMock

import pytest

from palace.manager.integration.license.opds.settings.format_priority import (
    FormatPriorities,
)
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolDeliveryMechanism,
)


class TestFormatPriorities:
    @pytest.fixture
    def mock_delivery(
        self,
    ) -> Callable[[str | None, str | None], DeliveryMechanism]:
        def delivery_mechanism(
            drm_scheme: str | None = None,
            content_type: str | None = "application/epub+zip",
        ) -> DeliveryMechanism:
            def _delivery_eq(self, other):
                return (
                    self.drm_scheme == other.drm_scheme
                    and self.content_type == other.content_type
                )

            def _delivery_repr(self):
                return f"DeliveryMechanism(drm_scheme={self.drm_scheme}, content_type={self.content_type})"

            _delivery = MagicMock(spec=DeliveryMechanism)
            _delivery.drm_scheme = drm_scheme
            _delivery.content_type = content_type
            setattr(_delivery, "__eq__", _delivery_eq)
            setattr(_delivery, "__repr__", _delivery_repr)

            return _delivery

        return delivery_mechanism

    @pytest.fixture
    def mock_mechanism(
        self, mock_delivery
    ) -> Callable[[str | None, str | None], LicensePoolDeliveryMechanism]:
        def mechanism(
            drm_scheme: str | None = None,
            content_type: str | None = "application/epub+zip",
        ) -> LicensePoolDeliveryMechanism:
            def _mechanism_eq(self, other):
                return self.delivery_mechanism == other.delivery_mechanism

            def _mechanism_repr(self):
                return f"LicensePoolDeliveryMechanism(delivery_mechanism={self.delivery_mechanism})"

            _mechanism = MagicMock(spec=LicensePoolDeliveryMechanism)
            _mechanism.delivery_mechanism = mock_delivery(drm_scheme, content_type)
            setattr(_mechanism, "__eq__", _mechanism_eq)
            setattr(_mechanism, "__repr__", _mechanism_repr)
            return _mechanism

        return mechanism

    @pytest.fixture
    def sample_data_0(self, mock_mechanism):
        """An arrangement of delivery mechanisms taken from a working database."""
        return [
            mock_mechanism("application/vnd.adobe.adept+xml", "application/epub+zip"),
            mock_mechanism(
                "Libby DRM",
                "application/vnd.overdrive.circulation.api+json;profile=audiobook",
            ),
            mock_mechanism(None, "application/audiobook+json"),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json", "application/pdf"
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/epub+zip",
            ),
            mock_mechanism(None, "application/epub+zip"),
            mock_mechanism(None, "application/pdf"),
            mock_mechanism(
                "application/vnd.librarysimplified.findaway.license+json", None
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/audiobook+json",
            ),
            mock_mechanism(None, "application/kepub+zip"),
            mock_mechanism(None, "application/x-mobipocket-ebook"),
            mock_mechanism(None, "application/x-mobi8-ebook"),
            mock_mechanism(None, "text/plain; charset=utf-8"),
            mock_mechanism(None, "application/octet-stream"),
            mock_mechanism(None, "text/html; charset=utf-8"),
            mock_mechanism(
                "http://www.feedbooks.com/audiobooks/access-restriction",
                "application/audiobook+json",
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/audiobook+lcp",
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json", "application/epub+zip"
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/pdf",
            ),
        ]

    def test_identity_empty(self):
        priorities = FormatPriorities(
            prioritized_drm_schemes=[],
            prioritized_content_types=[],
        )
        assert [] == priorities.prioritize_mechanisms([])

    def test_identity_one(self, mock_mechanism):
        priorities = FormatPriorities(
            prioritized_drm_schemes=[],
            prioritized_content_types=[],
        )
        mechanism_0 = mock_mechanism()
        assert [mechanism_0] == priorities.prioritize_mechanisms([mechanism_0])

    def test_non_prioritized_drm_0(self, sample_data_0):
        priorities = FormatPriorities(
            prioritized_drm_schemes=[],
            prioritized_content_types=[],
        )
        expected = sample_data_0.copy()
        assert expected == priorities.prioritize_mechanisms(sample_data_0)

    def test_prioritized_content_type_0(self, mock_mechanism, sample_data_0):
        """A simple configuration where an unusual content type is prioritized."""
        priorities = FormatPriorities(
            prioritized_drm_schemes=[],
            prioritized_content_types=["application/x-mobi8-ebook"],
        )

        # We expect the mobi8-ebook format to be pushed to the front of the list.
        # All other non-DRM formats are moved to the start of the list in a more or less arbitrary order.
        expected = [
            mock_mechanism(None, "application/x-mobi8-ebook"),
            mock_mechanism(None, "application/audiobook+json"),
            mock_mechanism(None, "application/epub+zip"),
            mock_mechanism(None, "application/pdf"),
            mock_mechanism(None, "application/kepub+zip"),
            mock_mechanism(None, "application/x-mobipocket-ebook"),
            mock_mechanism(None, "text/plain; charset=utf-8"),
            mock_mechanism(None, "application/octet-stream"),
            mock_mechanism(None, "text/html; charset=utf-8"),
            mock_mechanism("application/vnd.adobe.adept+xml", "application/epub+zip"),
            mock_mechanism(
                "Libby DRM",
                "application/vnd.overdrive.circulation.api+json;profile=audiobook",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json", "application/pdf"
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/epub+zip",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.findaway.license+json", None
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/audiobook+json",
            ),
            mock_mechanism(
                "http://www.feedbooks.com/audiobooks/access-restriction",
                "application/audiobook+json",
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/audiobook+lcp",
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json", "application/epub+zip"
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json", "application/pdf"
            ),
        ]

        received = priorities.prioritize_mechanisms(sample_data_0)
        assert expected == received
        assert len(sample_data_0) == len(received)

    def test_prioritized_content_type_1(self, mock_mechanism, sample_data_0):
        """A test of a more aggressive configuration where multiple content types
        and DRM schemes are prioritized."""
        priorities = FormatPriorities(
            prioritized_drm_schemes=[
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/vnd.librarysimplified.bearer-token+json",
                "application/vnd.adobe.adept+xml",
            ],
            prioritized_content_types=[
                "application/epub+zip",
                "application/audiobook+json",
                "application/audiobook+lcp",
                "application/pdf",
            ],
        )
        expected = [
            mock_mechanism(None, "application/epub+zip"),
            mock_mechanism(None, "application/audiobook+json"),
            mock_mechanism(None, "application/pdf"),
            mock_mechanism(None, "application/kepub+zip"),
            mock_mechanism(None, "application/x-mobipocket-ebook"),
            mock_mechanism(None, "application/x-mobi8-ebook"),
            mock_mechanism(None, "text/plain; charset=utf-8"),
            mock_mechanism(None, "application/octet-stream"),
            mock_mechanism(None, "text/html; charset=utf-8"),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json", "application/epub+zip"
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/audiobook+lcp",
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/pdf",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/epub+zip",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/audiobook+json",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json", "application/pdf"
            ),
            mock_mechanism("application/vnd.adobe.adept+xml", "application/epub+zip"),
            mock_mechanism(
                "http://www.feedbooks.com/audiobooks/access-restriction",
                "application/audiobook+json",
            ),
            mock_mechanism(
                "Libby DRM",
                "application/vnd.overdrive.circulation.api+json;profile=audiobook",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.findaway.license+json", None
            ),
        ]
        received = priorities.prioritize_mechanisms(sample_data_0)
        assert expected == received

    @staticmethod
    def _show(mechanisms):
        output = []
        for mechanism in mechanisms:
            item = {}
            if mechanism.delivery_mechanism.drm_scheme:
                item["drm"] = mechanism.delivery_mechanism.drm_scheme
            if mechanism.delivery_mechanism.content_type:
                item["type"] = mechanism.delivery_mechanism.content_type
            output.append(item)
        print(json.dumps(output, indent=2))
