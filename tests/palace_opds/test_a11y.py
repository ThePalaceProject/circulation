"""Tests for accessibility metadata models."""

import logging

import pytest

from palace.opds.a11y import (
    Accessibility,
    AccessibilityFeature,
    AccessibilityHazard,
    Certification,
    Exemption,
)


class TestCertification:
    def test_parse(self) -> None:
        """Test parsing a Certification from a dict with aliased keys."""
        cert = Certification.model_validate(
            {
                "certifiedBy": "Benetech",
                "credential": "GCA Certified",
                "report": "https://example.com/report",
            }
        )
        assert cert.certified_by == "Benetech"
        assert cert.credential == "GCA Certified"
        assert cert.report == "https://example.com/report"

    def test_serialize(self) -> None:
        """Test that Certification serializes with correct aliases."""
        cert = Certification(
            certified_by="Benetech",
            credential="GCA Certified",
            report="https://example.com/report",
        )
        data = cert.serialize()
        assert data == {
            "certifiedBy": "Benetech",
            "credential": "GCA Certified",
            "report": "https://example.com/report",
        }

    def test_partial_fields(self) -> None:
        """Test that Certification accepts partial data with remaining fields as None."""
        cert = Certification.model_validate({"certifiedBy": "Benetech"})
        assert cert.certified_by == "Benetech"
        assert cert.credential is None
        assert cert.report is None


class TestAccessibility:
    def test_hazard_round_trip(self) -> None:
        """Hazard values are parsed and serialized correctly."""
        a11y = Accessibility.model_validate(
            {
                "hazard": ["noFlashingHazard", "noSoundHazard"],
            }
        )
        assert a11y.hazard == [
            AccessibilityHazard.no_flashing_hazard,
            AccessibilityHazard.no_sound_hazard,
        ]
        data = a11y.serialize()
        assert data["hazard"] == ["noFlashingHazard", "noSoundHazard"]

    def test_exemption_round_trip(self) -> None:
        """Exemption values are parsed and serialized correctly."""
        for exemption in Exemption:
            a11y = Accessibility.model_validate({"exemption": exemption.value})
            assert a11y.exemption == exemption
            data = a11y.serialize()
            assert data["exemption"] == exemption.value

    def test_certification_round_trip(self) -> None:
        """Certification is parsed and serialized correctly."""
        a11y = Accessibility.model_validate(
            {
                "certification": {
                    "certifiedBy": "Benetech",
                    "credential": "GCA Certified",
                    "report": "https://example.com/report",
                },
            }
        )
        assert a11y.certification is not None
        assert a11y.certification.certified_by == "Benetech"
        assert a11y.certification.credential == "GCA Certified"
        assert a11y.certification.report == "https://example.com/report"
        data = a11y.serialize()
        assert data["certification"] == {
            "certifiedBy": "Benetech",
            "credential": "GCA Certified",
            "report": "https://example.com/report",
        }

    def test_certification_none_dropped(self) -> None:
        """Certification is omitted from serialized output when None."""
        a11y = Accessibility()
        data = a11y.serialize()
        assert "certification" not in data

    def test_hazard_empty_dropped(self) -> None:
        """Empty hazard list is omitted from serialized output."""
        a11y = Accessibility()
        data = a11y.serialize()
        assert "hazard" not in data

    def test_exemption_none_dropped(self) -> None:
        """Exemption is omitted from serialized output when None."""
        a11y = Accessibility()
        data = a11y.serialize()
        assert "exemption" not in data

    def test_feature_miscased_coerced_with_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Miscased feature values are coerced to canonical form and logged."""
        with caplog.at_level(logging.WARNING, logger="palace.opds.a11y"):
            a11y = Accessibility.model_validate(
                {"feature": ["TaggedPDF", "mathml", "ARIA"]}
            )
        assert a11y.feature == [
            AccessibilityFeature.tagged_pdf,
            AccessibilityFeature.math_ml,
            AccessibilityFeature.ARIA,
        ]
        assert a11y.serialize()["feature"] == ["taggedPDF", "MathML", "ARIA"]
        warnings = [r.getMessage() for r in caplog.records]
        assert any("'TaggedPDF'" in m and "'taggedPDF'" in m for m in warnings)
        assert any("'mathml'" in m and "'MathML'" in m for m in warnings)
        # ARIA was already canonical, so no warning for it.
        assert not any("'ARIA'" in m for m in warnings)

    def test_feature_unknown_dropped_with_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Unknown feature values are dropped and logged."""
        with caplog.at_level(logging.WARNING, logger="palace.opds.a11y"):
            a11y = Accessibility.model_validate(
                {"feature": ["taggedPDF", "notARealFeature"]}
            )
        assert a11y.feature == [AccessibilityFeature.tagged_pdf]
        warnings = [r.getMessage() for r in caplog.records]
        assert any("Dropping" in m and "'notARealFeature'" in m for m in warnings)

    def test_hazard_unknown_dropped_with_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Unknown hazard values are dropped and logged."""
        with caplog.at_level(logging.WARNING, logger="palace.opds.a11y"):
            a11y = Accessibility.model_validate(
                {"hazard": ["noFlashingHazard", "bogus"]}
            )
        assert a11y.hazard == [AccessibilityHazard.no_flashing_hazard]
        warnings = [r.getMessage() for r in caplog.records]
        assert any("Dropping" in m and "'bogus'" in m for m in warnings)
