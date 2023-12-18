from pathlib import Path

from customlists.customlist_explain import CustomListImportExplainer


class TestExplains:
    @staticmethod
    def _customlists_resource_path(name) -> str:
        """The path to the customlists resource with the given filename."""
        base_path = Path(__file__).parent.parent.parent
        resource_path = base_path / "customlists"
        return str(resource_path / name)

    @staticmethod
    def _test_customlists_resource_path(name) -> str:
        """The path to the customlists test resource with the given filename."""
        base_path = Path(__file__).parent.parent
        resource_path = base_path / "customlists" / "files"
        return str(resource_path / name)

    @staticmethod
    def _test_customlists_resource_bytes(name) -> bytes:
        with open(TestExplains._test_customlists_resource_path(name), "rb") as f:
            return f.read()

    def test_explain_simple_report(self, tmpdir):
        schema_path = TestExplains._customlists_resource_path(
            "customlists-report.schema.json"
        )
        report_path = TestExplains._test_customlists_resource_path(
            "example-report.json"
        )
        output_path = TestExplains._test_customlists_resource_path(
            "example-report-output.csv"
        )

        CustomListImportExplainer.create(
            [
                "--report-schema-file",
                schema_path,
                "--report-file",
                report_path,
                "--output-csv-file",
                str(tmpdir.join("output.csv")),
            ]
        ).execute()

        text_expected: list[str] = open(output_path).readlines()
        text_received: list[str] = open(tmpdir.join("output.csv")).readlines()
        assert len(text_expected) == len(text_received)
        for i in range(0, len(text_expected)):
            assert text_expected[i] == text_received[i]
