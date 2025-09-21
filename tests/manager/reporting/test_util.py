import io
import logging
from collections.abc import MutableMapping
from functools import partial
from typing import Any

import _csv
import pytest

from palace.manager.reporting.util import (
    RequestIdLoggerAdapter,
    row_counter_wrapper,
    write_csv,
)


class TestReportTableProcessors:
    @pytest.mark.parametrize(
        "rows, headings, delimiter, expected",
        [
            pytest.param(
                [["row1", "data1"], ["row2", "data2"]],
                None,
                ",",
                "row1,data1\r\nrow2,data2\r\n",
                id="no-headings",
            ),
            pytest.param(
                [["data1", "data2"], ["data3", "data4"]],
                ["header1", "header2"],
                ",",
                "header1,header2\r\ndata1,data2\r\ndata3,data4\r\n",
                id="with-headings",
            ),
            pytest.param(
                [["row1", "data1"], ["row2", "data2"]],
                ["header1", "header2"],
                "|",
                "header1|header2\r\nrow1|data1\r\nrow2|data2\r\n",
                id="different-delimiter",
            ),
            pytest.param(
                [], ["header1", "header2"], ",", "header1,header2\r\n", id="empty-rows"
            ),
            pytest.param([], None, ",", "", id="empty-rows-no-headings"),
            pytest.param(
                [["data1"]],
                ["header1"],
                ",",
                "header1\r\ndata1\r\n",
                id="single-row-single-column",
            ),
            pytest.param(
                [[1, "data1", 3.14]],
                None,
                ",",
                "1,data1,3.14\r\n",
                id="different-data-types",
            ),
        ],
    )
    def test_write_csv(
        self,
        rows: list[list[Any]],
        headings: list[str] | None,
        delimiter: str,
        expected: str,
    ) -> None:
        output = io.StringIO()

        write_csv(file=output, rows=rows, headings=headings, delimiter=delimiter)

        output_str = output.getvalue()
        assert output_str == expected

    @pytest.mark.parametrize(
        "rows, headings, delimiter, expected_error, expected_match",
        [
            pytest.param(
                123,
                ["header1", "header2"],
                ",",
                TypeError,
                "object is not iterable",
                id="rows-not-iterable",
            ),
            pytest.param(
                [1, 2, 3],
                None,
                ",",
                _csv.Error,
                "iterable expected",
                id="rows-not-iterable-of-iterables",
            ),
        ],
    )
    def test_write_csv_error_cases(
        self,
        rows: Any,
        headings: list[str] | None,
        delimiter: str,
        expected_error: type[Exception],
        expected_match: str,
    ) -> None:
        output = io.StringIO()

        with pytest.raises(expected_error, match=expected_match):
            write_csv(file=output, rows=rows, headings=headings, delimiter=delimiter)

    @pytest.mark.parametrize(
        "rows, headings, expected",
        [
            pytest.param(
                [["row1", "data1"], ["row2", "data2"]],
                None,
                "row1|data1\r\nrow2|data2\r\n",
                id="no-headings",
            ),
            pytest.param(
                [["data1", "data2"], ["data3", "data4"]],
                ["header1", "header2"],
                "header1|header2\r\ndata1|data2\r\ndata3|data4\r\n",
                id="with-headings",
            ),
        ],
    )
    def test_write_csv_partial(
        self, rows: list[list[Any]], headings: list[str] | None, expected: str
    ):
        output = io.StringIO()
        csv_to_output_w_pipe_sep = partial(write_csv, file=output, delimiter="|")
        csv_to_output_w_pipe_sep(rows=rows, headings=headings)

        assert output.getvalue() == expected

    @pytest.mark.parametrize(
        "rows, headings, expected_output, expected_count",
        [
            pytest.param(
                [["row1", "data1"], ["row2", "data2"]],
                None,
                "row1,data1\r\nrow2,data2\r\n",
                2,
                id="no-headings",
            ),
            pytest.param(
                [["data1", "data2"], ["data3", "data4"]],
                ["header1", "header2"],
                "header1,header2\r\ndata1,data2\r\ndata3,data4\r\n",
                2,
                id="with-headings",
            ),
            pytest.param(
                [], ["header1", "header2"], "header1,header2\r\n", 0, id="no-rows"
            ),
            pytest.param(
                [["data1"]], ["header1"], "header1\r\ndata1\r\n", 1, id="single-row"
            ),
        ],
    )
    def test_write_csv_row_counter_wrapper(
        self,
        rows: list[list[Any]],
        headings: list[str] | None,
        expected_output: str,
        expected_count: int,
    ):
        output = io.StringIO()
        table_data_processor = partial(write_csv, file=output)
        counting_table_data_processor = row_counter_wrapper(table_data_processor)

        counted_rows, result = counting_table_data_processor(
            rows=rows, headings=headings
        )

        assert counted_rows.count == expected_count
        assert output.getvalue() == expected_output


class TestRequestIdLoggerAdapter:
    @pytest.mark.parametrize(
        "extra_data, expected_suffix",
        (
            pytest.param(
                {"id": "abc123"}, " (request ID: abc123)", id="with_request_id"
            ),
            pytest.param({}, "", id="no_request_id"),
            pytest.param({"id": None}, "", id="none_request_id"),
            pytest.param({"id": ""}, "", id="empty_string_request_id"),
            pytest.param({"id": 0}, " (request ID: 0)", id="zero_request_id"),
            pytest.param({"id": False}, " (request ID: False)", id="false_request_id"),
            pytest.param({"id": 42}, " (request ID: 42)", id="numeric_request_id"),
            pytest.param({"other_key": "value"}, "", id="extra_without_id_key"),
            pytest.param(
                {"id": "req-456", "other": "data"},
                " (request ID: req-456)",
                id="id_with_other_data",
            ),
        ),
    )
    def test_process_with_extra_data(
        self,
        extra_data: dict[str, Any],
        expected_suffix: str,
    ):
        logger = logging.getLogger("test_logger")
        adapter = RequestIdLoggerAdapter(logger, extra_data)

        msg = "Test message"
        kwargs: MutableMapping[str, Any] = {}

        result_msg, result_kwargs = adapter.process(msg, kwargs)

        assert result_msg == f"{msg}{expected_suffix}"
        assert result_kwargs is kwargs

    def test_process_with_none_extra(self):
        extra_data = None
        logger = logging.getLogger("test_logger")
        adapter = RequestIdLoggerAdapter(logger, extra_data)

        msg = "Test message"
        kwargs: MutableMapping[str, Any] = {}

        result_msg, result_kwargs = adapter.process(msg, kwargs)

        assert result_msg == msg
        assert result_kwargs is kwargs

    @pytest.mark.parametrize(
        "extra_data, log_level, expected_in_output",
        (
            pytest.param(
                {"id": "test-123"},
                "info",
                "Original message (request ID: test-123)",
                id="info_with_id",
            ),
            pytest.param({}, "warning", "Original message", id="warning_without_id"),
            pytest.param(
                {"id": "error-456"},
                "error",
                "Original message (request ID: error-456)",
                id="error_with_id",
            ),
        ),
    )
    def test_logging_integration(
        self,
        extra_data: dict[str, Any],
        log_level: str,
        expected_in_output: str,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(logging.INFO)
        logger = logging.getLogger("test_request_logger")
        adapter = RequestIdLoggerAdapter(logger, extra_data)

        log_method = getattr(adapter, log_level)
        log_method("Original message")

        assert expected_in_output in caplog.text

    def test_kwargs_pass_through(self):
        logger = logging.getLogger("test_logger")
        adapter = RequestIdLoggerAdapter(logger, {"id": "test"})

        msg = "Test message"
        original_kwargs: MutableMapping[str, Any] = {"original": {"custom": "data"}}

        result_msg, result_kwargs = adapter.process(msg, original_kwargs)

        # Kwargs should make it through unscathed.
        assert result_kwargs is original_kwargs
        assert result_kwargs == {"original": {"custom": "data"}}
