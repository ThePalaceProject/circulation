from __future__ import annotations

from typing import Dict, Optional

from lxml.etree import _Element

from core.util.xmlparser import XMLProcessor


class MockProcessor(XMLProcessor[_Element]):
    """A mock XMLParser that just returns every tag it hears about."""

    def __init__(self, xpath_expression: str) -> None:
        self._xpath_expression = xpath_expression

    @property
    def xpath_expression(self) -> str:
        return self._xpath_expression

    def process_one(
        self, tag: _Element, namespaces: Optional[Dict[str, str]]
    ) -> _Element:
        return tag


class TestXMLProcessor:
    def test_process_all(self) -> None:
        # Verify that process_all can handle either XML markup
        # or an already-parsed tag object.
        data = "<atag>This is a tag.</atag>"

        # Try it with markup.
        parser = MockProcessor("/*")
        [tag] = parser.process_all(data)
        assert "atag" == tag.tag
        assert "This is a tag." == tag.text

        # Try it with a tag.
        [tag2] = parser.process_all(tag)
        assert tag == tag2

    def test_process_all_with_xpath(self) -> None:
        # Verify that process_all processes only tags that
        # match the given XPath expression.
        data = "<parent><a>First</a><b>Second</b><a>Third</a></parent><a>Fourth</a>"

        parser = MockProcessor("/parent/a")

        # Only process the <a> tags beneath the <parent> tag.
        [tag1, tag3] = parser.process_all(data)
        assert "First" == tag1.text
        assert "Third" == tag3.text

    def test_invalid_characters_are_stripped(self) -> None:
        data = b'<?xml version="1.0" encoding="utf-8"><tag>I enjoy invalid characters, such as \x00\x01 and \x1F. But I also like \xe2\x80\x9csmart quotes\xe2\x80\x9d.</tag>'
        parser = MockProcessor("/tag")
        [tag] = parser.process_all(data)
        assert (
            "I enjoy invalid characters, such as  and . But I also like “smart quotes”."
            == tag.text
        )

    def test_invalid_entities_are_stripped(self) -> None:
        data = '<?xml version="1.0" encoding="utf-8"><tag>I enjoy invalid entities, such as &#x00;&#x01; and &#x1F;</tag>'
        parser = MockProcessor("/tag")
        [tag] = parser.process_all(data)
        assert "I enjoy invalid entities, such as  and " == tag.text

    def test_process_first_result(self) -> None:
        # Verify that process_all processes only tags that
        # match the given XPath expression.
        data = "<parent><a>First</a><b>Second</b><a>Third</a></parent><a>Fourth</a>"

        parser = MockProcessor("/parent/a")

        # Only process the <a> tags beneath the <parent> tag.
        tag1_first_call = parser.process_first(data)
        assert tag1_first_call is not None
        assert "First" == tag1_first_call.text

        # Calling process first again will return the same tag
        tag1_second_call = parser.process_first(data)
        assert tag1_second_call is not None
        assert "First" == tag1_second_call.text

        # But a different tag instance
        assert tag1_first_call is not tag1_second_call

        # If no tag is found, process_first returns None
        parser = MockProcessor("/parent/c")
        assert parser.process_first(data) is None
