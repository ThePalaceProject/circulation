from palace.manager.customlists.explain import CustomListImportExplainer
from tests.manager.customlists.conftest import CustomListsFilesFixture


class TestExplains:
    def test_explain_simple_report(
        self, customlists_files: CustomListsFilesFixture, tmpdir
    ):
        report_path = customlists_files.sample_path_str("example-report.json")
        output_path = customlists_files.sample_path_str("example-report-output.csv")

        CustomListImportExplainer.create(
            [
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
