from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from core.util.authentication_for_opds import AuthenticationForOPDSDocument as Doc
from core.util.authentication_for_opds import OPDSAuthenticationFlow as Flow


class MockFlow(Flow):
    """A mock OPDSAuthenticationFlow"""

    @property
    def flow_type(self) -> str:
        return "http://mock1/"

    def __init__(self, description):
        self.description = description

    def _authentication_flow_document(self, argument):
        return {
            "description": self.description,
            "arg": argument,
        }


class TestOPDSAuthenticationFlow:
    def test_flow_sets_type(self):
        """An OPDSAuthenticationFlow object can set `type` during
        to_dict().
        """
        flow = MockFlow("description")
        doc = flow.authentication_flow_document("argument")
        assert {
            "type": "http://mock1/",
            "description": "description",
            "arg": "argument",
        } == doc

    def test_flow_calls__authentication_flow_document(self):
        """An OPDSAuthenticationFlow object can set `type` during
        to_dict().
        """
        db = MagicMock(spec=Session)
        flow = MockFlow("description")
        flow._authentication_flow_document = MagicMock(return_value={})
        doc = flow.authentication_flow_document(db)
        assert {
            "type": "http://mock1/",
        } == doc
        flow._authentication_flow_document.assert_called_once_with(db)


class TestAuthenticationForOPDSDocument:
    def test_good_document(self):
        """Verify that to_dict() works when all the data is in place."""
        doc_obj = Doc(
            id="id",
            title="title",
            authentication_flows=[MockFlow("hello")],
            links=[dict(rel="register", href="http://registration/")],
        )

        doc = doc_obj.to_dict("argument")
        assert {
            "id": "id",
            "title": "title",
            "authentication": [
                {"arg": "argument", "description": "hello", "type": "http://mock1/"}
            ],
            "links": [{"href": "http://registration/", "rel": "register"}],
        } == doc

    def test_bad_document(self):
        """Test that to_dict() raises ValueError when something is
        wrong with the data.
        """

        def cannot_make(document):
            pytest.raises(ValueError, document.to_dict, object())

        # Document must have ID and title.
        cannot_make(Doc(id=None, title="no id"))
        cannot_make(Doc(id="no title", title=None))

        # authentication_flows and links must both be lists.
        cannot_make(Doc(id="id", title="title", authentication_flows="not a list"))
        cannot_make(
            Doc(
                id="id",
                title="title",
                authentication_flows=["a list"],
                links="not a list",
            )
        )

        # A link must be a dict.
        cannot_make(
            Doc(id="id", title="title", authentication_flows=[], links=["not a dict"])
        )

        # A link must have a rel and an href.
        cannot_make(
            Doc(
                id="id",
                title="title",
                authentication_flows=[],
                links=[{"rel": "no href"}],
            )
        )
        cannot_make(
            Doc(
                id="id",
                title="title",
                authentication_flows=[],
                links=[{"href": "no rel"}],
            )
        )
