from palace.manager.data_layer.subject import SubjectData


class TestSubjectData:
    def test_identifier(self) -> None:
        """Test that we strip whitespace from the identifier."""
        subject = SubjectData.model_validate({"type": "test", "identifier": " 12345 "})
        assert subject.type == "test"
        assert subject.identifier == "12345"

    def test_name(self) -> None:
        """Test that we strip whitespace from the name."""
        subject = SubjectData.model_validate(
            {"type": "test", "identifier": None, "name": "  Test Name  "}
        )
        assert subject.type == "test"
        assert subject.identifier is None
        assert subject.name == "Test Name"

    def test_hash(self) -> None:
        """Test that SubjectData is hashable."""
        subject = SubjectData.model_validate({"type": "test", "identifier": "12345"})
        assert hash(subject)
