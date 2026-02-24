"""Tests for base authentication models."""

from palace.manager.api.authentication.base import PatronAuthResult


class TestPatronAuthResult:
    def test_serialization_string_details(self):
        result = PatronAuthResult(
            label="Test Step",
            success=True,
            details="Some detail string",
        )
        data = result.model_dump()
        assert data["label"] == "Test Step"
        assert data["success"] is True
        assert data["details"] == "Some detail string"

    def test_serialization_list_details(self):
        result = PatronAuthResult(
            label="Test Step",
            success=False,
            details=["line1", "line2"],
        )
        data = result.model_dump()
        assert data["details"] == ["line1", "line2"]

    def test_serialization_dict_details(self):
        result = PatronAuthResult(
            label="Test Step",
            success=True,
            details={"key": "value", "other": "data"},
        )
        data = result.model_dump()
        assert data["details"] == {"key": "value", "other": "data"}

    def test_serialization_dict_mixed_value_types(self):
        """Dict details can contain string, int, float, bool, and None values."""
        result = PatronAuthResult(
            label="Mixed Types",
            success=True,
            details={
                "name": "Alice",
                "fines": 2.50,
                "max_length": 14,
                "active": True,
                "block_reason": None,
            },
        )
        data = result.model_dump()
        assert data["details"] == {
            "name": "Alice",
            "fines": 2.50,
            "max_length": 14,
            "active": True,
            "block_reason": None,
        }

    def test_serialization_none_details(self):
        result = PatronAuthResult(
            label="Test Step",
            success=True,
        )
        data = result.model_dump()
        assert data["details"] is None

    def test_bool(self):
        """PatronAuthResult is truthy when success=True, falsy when success=False."""
        assert bool(PatronAuthResult(label="ok", success=True)) is True
        assert bool(PatronAuthResult(label="fail", success=False)) is False
