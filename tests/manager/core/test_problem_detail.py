import json

from palace.manager.util.problem_detail import ProblemDetail


class TestProblemDetail:
    def test_with_debug(self):
        detail = ProblemDetail("http://uri/", title="Title", detail="Detail")
        with_debug = detail.with_debug("Debug Message")
        assert "Detail" == with_debug.detail
        assert "Debug Message" == with_debug.debug_message
        assert "Title" == with_debug.title
        json_data, status, headers = with_debug.response
        data = json.loads(json_data)
        assert "Debug Message" == data["debug_message"]
        assert "Detail" == data["detail"]

    def test_json_includes_title(self):
        detail = ProblemDetail("http://uri/", 400, title="Title", detail="Detail")
        data = json.loads(detail.response[0])
        assert "Title" == data["title"]

    def test_json_omits_missing_title(self):
        """A ProblemDetail with no title serializes without a title member,
        rather than with the string "None"."""
        detail = ProblemDetail("http://uri/", 400, detail="Detail")
        data = json.loads(detail.response[0])
        assert "title" not in data
        assert "Detail" == data["detail"]
        assert 400 == data["status"]
        assert "http://uri/" == data["type"]

    def test_show_title_defaults_to_true(self):
        """By default nothing is added to the document, so clients keep
        rendering the title they always have."""
        detail = ProblemDetail("http://uri/", 400, title="Title", detail="Detail")
        assert detail.show_title is True
        data = json.loads(detail.response[0])
        assert "show_title" not in data

    def test_show_title_false_serialized(self):
        """show_title=False is passed on to clients as an extension member.

        The document keeps its title — Palace clients render their own title for
        a known problem type and ignore this one — so the flag is what tells them
        to show only the detail message.
        """
        detail = ProblemDetail(
            "http://uri/", 403, title="Title", detail="Detail", show_title=False
        )
        assert detail.show_title is False

        data = json.loads(detail.response[0])
        assert data["show_title"] is False
        assert "Title" == data["title"]
        assert "Detail" == data["detail"]

    def test_detailed_inherits_show_title(self):
        parent = ProblemDetail("http://uri/", 403, title="Title", show_title=False)
        assert parent.detailed("Detail").show_title is False

    def test_detailed_overrides_show_title(self):
        parent = ProblemDetail("http://uri/", 403, title="Title")
        assert parent.detailed("Detail", show_title=False).show_title is False
        # ...and the parent is left alone.
        assert parent.show_title is True
        assert parent.detailed("Detail").show_title is True

    def test_with_debug_preserves_show_title(self):
        detail = ProblemDetail("http://uri/", 403, title="Title", show_title=False)
        assert detail.with_debug("Debug Message").show_title is False

    def test_equality_includes_show_title(self):
        args = ("http://uri/", 403, "Title", "Detail")
        assert ProblemDetail(*args) == ProblemDetail(*args)
        assert ProblemDetail(*args) != ProblemDetail(*args, show_title=False)
