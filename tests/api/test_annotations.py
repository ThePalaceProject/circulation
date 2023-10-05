import datetime
import json
from typing import Any, Dict

import pytest
from pyld import jsonld

from api.annotations import AnnotationParser, AnnotationWriter
from api.problem_details import *
from core.model import Annotation, create
from core.util.datetime_helpers import utc_now
from tests.fixtures.api_controller import ControllerFixture


class AnnotationFixture:
    def __init__(self, controller_fixture: ControllerFixture):
        self.controller = controller_fixture
        self.db = controller_fixture.db

    def patron(self):
        """Create a test patron who has opted in to annotation sync."""
        patron = self.db.patron()
        patron.synchronize_annotations = True
        return patron


@pytest.fixture(scope="function")
def annotation_fixture(controller_fixture: ControllerFixture) -> AnnotationFixture:
    return AnnotationFixture(controller_fixture)


class TestAnnotationWriter:
    def test_annotations_for(self, annotation_fixture: AnnotationFixture):
        patron = annotation_fixture.patron()

        # The patron doesn't have any annotations yet.
        assert [] == AnnotationWriter.annotations_for(patron)

        identifier = annotation_fixture.db.identifier()
        annotation, ignore = create(
            annotation_fixture.db.session,
            Annotation,
            patron=patron,
            identifier=identifier,
            motivation=Annotation.IDLING,
        )

        # The patron has one annotation.
        assert [annotation] == AnnotationWriter.annotations_for(patron)
        assert [annotation] == AnnotationWriter.annotations_for(patron, identifier)

        identifier2 = annotation_fixture.db.identifier()
        annotation2, ignore = create(
            annotation_fixture.db.session,
            Annotation,
            patron=patron,
            identifier=identifier2,
            motivation=Annotation.IDLING,
        )

        # The patron has two annotations for different identifiers.
        assert {annotation, annotation2} == set(
            AnnotationWriter.annotations_for(patron)
        )
        assert [annotation] == AnnotationWriter.annotations_for(patron, identifier)
        assert [annotation2] == AnnotationWriter.annotations_for(patron, identifier2)

    def test_annotation_container_for(self, annotation_fixture: AnnotationFixture):
        patron = annotation_fixture.patron()

        with annotation_fixture.controller.app.test_request_context("/"):
            container, timestamp = AnnotationWriter.annotation_container_for(patron)

            assert {
                AnnotationWriter.JSONLD_CONTEXT,
                AnnotationWriter.LDP_CONTEXT,
            } == set(container["@context"])
            assert "annotations" in container["id"]
            assert {"BasicContainer", "AnnotationCollection"} == set(container["type"])
            assert 0 == container["total"]

            first_page = container["first"]
            assert "AnnotationPage" == first_page["type"]

            # The page doesn't have a context, since it's in the container.
            assert None == first_page.get("@context")

            # The patron doesn't have any annotations yet.
            assert 0 == container["total"]

            # There's no timestamp since the container is empty.
            assert None == timestamp

            # Now, add an annotation.
            identifier = annotation_fixture.db.identifier()
            annotation, ignore = create(
                annotation_fixture.db.session,
                Annotation,
                patron=patron,
                identifier=identifier,
                motivation=Annotation.IDLING,
            )
            annotation.timestamp = utc_now()

            container, timestamp = AnnotationWriter.annotation_container_for(patron)

            # The context, type, and id stay the same.
            assert {
                AnnotationWriter.JSONLD_CONTEXT,
                AnnotationWriter.LDP_CONTEXT,
            } == set(container["@context"])
            assert "annotations" in container["id"]
            assert identifier.identifier not in container["id"]
            assert {"BasicContainer", "AnnotationCollection"} == set(container["type"])

            # But now there is one item.
            assert 1 == container["total"]

            first_page = container["first"]

            assert 1 == len(first_page["items"])

            # The item doesn't have a context, since it's in the container.
            first_item = first_page["items"][0]
            assert None == first_item.get("@context")

            # The timestamp is the annotation's timestamp.
            assert annotation.timestamp == timestamp

            # If the annotation is deleted, the container will be empty again.
            annotation.active = False

            container, timestamp = AnnotationWriter.annotation_container_for(patron)
            assert 0 == container["total"]
            assert None == timestamp

    def test_annotation_container_for_with_identifier(
        self, annotation_fixture: AnnotationFixture
    ):
        patron = annotation_fixture.patron()
        identifier = annotation_fixture.db.identifier()

        with annotation_fixture.controller.app.test_request_context("/"):
            container, timestamp = AnnotationWriter.annotation_container_for(
                patron, identifier
            )

            assert {
                AnnotationWriter.JSONLD_CONTEXT,
                AnnotationWriter.LDP_CONTEXT,
            } == set(container["@context"])
            assert "annotations" in container["id"]
            assert identifier.identifier in container["id"]
            assert {"BasicContainer", "AnnotationCollection"} == set(container["type"])
            assert 0 == container["total"]

            first_page = container["first"]
            assert "AnnotationPage" == first_page["type"]

            # The page doesn't have a context, since it's in the container.
            assert None == first_page.get("@context")

            # The patron doesn't have any annotations yet.
            assert 0 == container["total"]

            # There's no timestamp since the container is empty.
            assert None == timestamp

            # Now, add an annotation for this identifier, and one for a different identifier.
            annotation, ignore = create(
                annotation_fixture.db.session,
                Annotation,
                patron=patron,
                identifier=identifier,
                motivation=Annotation.IDLING,
            )
            annotation.timestamp = utc_now()

            other_annotation, ignore = create(
                annotation_fixture.db.session,
                Annotation,
                patron=patron,
                identifier=annotation_fixture.db.identifier(),
                motivation=Annotation.IDLING,
            )

            container, timestamp = AnnotationWriter.annotation_container_for(
                patron, identifier
            )

            # The context, type, and id stay the same.
            assert {
                AnnotationWriter.JSONLD_CONTEXT,
                AnnotationWriter.LDP_CONTEXT,
            } == set(container["@context"])
            assert "annotations" in container["id"]
            assert identifier.identifier in container["id"]
            assert {"BasicContainer", "AnnotationCollection"} == set(container["type"])

            # But now there is one item.
            assert 1 == container["total"]

            first_page = container["first"]

            assert 1 == len(first_page["items"])

            # The item doesn't have a context, since it's in the container.
            first_item = first_page["items"][0]
            assert None == first_item.get("@context")

            # The timestamp is the annotation's timestamp.
            assert annotation.timestamp == timestamp

            # If the annotation is deleted, the container will be empty again.
            annotation.active = False

            container, timestamp = AnnotationWriter.annotation_container_for(
                patron, identifier
            )
            assert 0 == container["total"]
            assert None == timestamp

    def test_annotation_page_for(self, annotation_fixture: AnnotationFixture):
        patron = annotation_fixture.patron()

        with annotation_fixture.controller.app.test_request_context("/"):
            page = AnnotationWriter.annotation_page_for(patron)

            # The patron doesn't have any annotations, so the page is empty.
            assert AnnotationWriter.JSONLD_CONTEXT == page["@context"]
            assert "annotations" in page["id"]
            assert "AnnotationPage" == page["type"]
            assert 0 == len(page["items"])

            # If we add an annotation, the page will have an item.
            identifier = annotation_fixture.db.identifier()
            annotation, ignore = create(
                annotation_fixture.db.session,
                Annotation,
                patron=patron,
                identifier=identifier,
                motivation=Annotation.IDLING,
            )

            page = AnnotationWriter.annotation_page_for(patron)

            assert 1 == len(page["items"])

            # But if the annotation is deleted, the page will be empty again.
            annotation.active = False

            page = AnnotationWriter.annotation_page_for(patron)

            assert 0 == len(page["items"])

    def test_annotation_page_for_with_identifier(
        self, annotation_fixture: AnnotationFixture
    ):
        patron = annotation_fixture.patron()
        identifier = annotation_fixture.db.identifier()

        with annotation_fixture.controller.app.test_request_context("/"):
            page = AnnotationWriter.annotation_page_for(patron, identifier)

            # The patron doesn't have any annotations, so the page is empty.
            assert AnnotationWriter.JSONLD_CONTEXT == page["@context"]
            assert "annotations" in page["id"]
            assert identifier.identifier in page["id"]
            assert "AnnotationPage" == page["type"]
            assert 0 == len(page["items"])

            # If we add an annotation, the page will have an item.
            annotation, ignore = create(
                annotation_fixture.db.session,
                Annotation,
                patron=patron,
                identifier=identifier,
                motivation=Annotation.IDLING,
            )

            page = AnnotationWriter.annotation_page_for(patron, identifier)
            assert 1 == len(page["items"])

            # If a different identifier has an annotation, the page will still have one item.
            other_annotation, ignore = create(
                annotation_fixture.db.session,
                Annotation,
                patron=patron,
                identifier=annotation_fixture.db.identifier(),
                motivation=Annotation.IDLING,
            )

            page = AnnotationWriter.annotation_page_for(patron, identifier)
            assert 1 == len(page["items"])

            # But if the annotation is deleted, the page will be empty again.
            annotation.active = False

            page = AnnotationWriter.annotation_page_for(patron, identifier)
            assert 0 == len(page["items"])

    def test_detail_target(self, annotation_fixture: AnnotationFixture):
        patron = annotation_fixture.patron()
        identifier = annotation_fixture.db.identifier()
        target = {
            "http://www.w3.org/ns/oa#hasSource": {"@id": identifier.urn},
            "http://www.w3.org/ns/oa#hasSelector": {
                "@type": "http://www.w3.org/ns/oa#FragmentSelector",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#value": "epubcfi(/6/4[chap01ref]!/4[body01]/10[para05]/3:10)",
            },
        }

        annotation, ignore = create(
            annotation_fixture.db.session,
            Annotation,
            patron=patron,
            identifier=identifier,
            motivation=Annotation.IDLING,
            target=json.dumps(target),
        )
        assert annotation is not None

        with annotation_fixture.controller.app.test_request_context("/"):
            detail = AnnotationWriter.detail(annotation)

            assert "annotations/%i" % (annotation.id or 0) in detail["id"]
            assert "Annotation" == detail["type"]
            assert Annotation.IDLING == detail["motivation"]
            compacted_target = {
                "source": identifier.urn,
                "selector": {
                    "type": "FragmentSelector",
                    "value": "epubcfi(/6/4[chap01ref]!/4[body01]/10[para05]/3:10)",
                },
            }
            assert compacted_target == detail["target"]

    def test_detail_body(self, annotation_fixture: AnnotationFixture):
        patron = annotation_fixture.patron()
        identifier = annotation_fixture.db.identifier()
        body = {
            "@type": "http://www.w3.org/ns/oa#TextualBody",
            "http://www.w3.org/ns/oa#bodyValue": "A good description of the topic that bears further investigation",
            "http://www.w3.org/ns/oa#hasPurpose": {
                "@id": "http://www.w3.org/ns/oa#describing"
            },
        }

        annotation, ignore = create(
            annotation_fixture.db.session,
            Annotation,
            patron=patron,
            identifier=identifier,
            motivation=Annotation.IDLING,
            content=json.dumps(body),
        )
        assert annotation is not None

        with annotation_fixture.controller.app.test_request_context("/"):
            detail = AnnotationWriter.detail(annotation)

            assert "annotations/%i" % (annotation.id or 0) in detail["id"]
            assert "Annotation" == detail["type"]
            assert Annotation.IDLING == detail["motivation"]
            compacted_body = {
                "type": "TextualBody",
                "bodyValue": "A good description of the topic that bears further investigation",
                "purpose": "describing",
            }
            assert compacted_body == detail["body"]


class AnnotationParserFixture(AnnotationFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)
        self.pool = self.db.licensepool(None)
        self.identifier = self.pool.identifier
        self.patron_value = self.patron()


@pytest.fixture(scope="function")
def annotation_parser_fixture(
    controller_fixture: ControllerFixture,
) -> AnnotationParserFixture:
    return AnnotationParserFixture(controller_fixture)


class TestAnnotationParser:
    @staticmethod
    def _sample_jsonld(
        annotation_parser_fixture: AnnotationParserFixture, motivation=Annotation.IDLING
    ):
        data: Dict[Any, Any] = dict()
        data["@context"] = [
            AnnotationWriter.JSONLD_CONTEXT,
            {"ls": Annotation.LS_NAMESPACE},
        ]
        data["type"] = "Annotation"
        motivation = motivation.replace(Annotation.LS_NAMESPACE, "ls:")
        motivation = motivation.replace(Annotation.OA_NAMESPACE, "oa:")
        data["motivation"] = motivation
        data["body"] = {
            "type": "TextualBody",
            "bodyValue": "A good description of the topic that bears further investigation",
            "purpose": "describing",
        }
        data["target"] = {
            "source": annotation_parser_fixture.identifier.urn,
            "selector": {
                "type": "oa:FragmentSelector",
                "value": "epubcfi(/6/4[chap01ref]!/4[body01]/10[para05]/3:10)",
            },
        }
        return data

    def test_parse_invalid_json(
        self, annotation_parser_fixture: AnnotationParserFixture
    ):
        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            "not json",
            annotation_parser_fixture.patron_value,
        )
        assert INVALID_ANNOTATION_FORMAT == annotation

    def test_invalid_identifier(
        self, annotation_parser_fixture: AnnotationParserFixture
    ):
        # If the target source can't be parsed as a URN we send
        # INVALID_ANNOTATION_TARGET
        data = self._sample_jsonld(annotation_parser_fixture)
        data["target"]["source"] = "not a URN"
        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            json.dumps(data),
            annotation_parser_fixture.patron_value,
        )
        assert INVALID_ANNOTATION_TARGET == annotation

    def test_null_id(self, annotation_parser_fixture: AnnotationParserFixture):
        # A JSON-LD document can have its @id set to null -- it's the
        # same as if the @id wasn't present -- but the jsonld library
        # can't handle this, so we need to test it specially.
        annotation_parser_fixture.pool.loan_to(annotation_parser_fixture.patron_value)
        data = self._sample_jsonld(annotation_parser_fixture)
        data["id"] = None
        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            json.dumps(data),
            annotation_parser_fixture.patron_value,
        )
        assert isinstance(annotation, Annotation)

    def test_parse_expanded_jsonld(
        self, annotation_parser_fixture: AnnotationParserFixture
    ):
        annotation_parser_fixture.pool.loan_to(annotation_parser_fixture.patron_value)

        data: Dict[Any, Any] = dict()
        data["@type"] = ["http://www.w3.org/ns/oa#Annotation"]
        data["http://www.w3.org/ns/oa#motivatedBy"] = [{"@id": Annotation.IDLING}]
        data["http://www.w3.org/ns/oa#hasBody"] = [
            {
                "@type": ["http://www.w3.org/ns/oa#TextualBody"],
                "http://www.w3.org/ns/oa#bodyValue": [
                    {
                        "@value": "A good description of the topic that bears further investigation"
                    }
                ],
                "http://www.w3.org/ns/oa#hasPurpose": [
                    {"@id": "http://www.w3.org/ns/oa#describing"}
                ],
            }
        ]
        data["http://www.w3.org/ns/oa#hasTarget"] = [
            {
                "http://www.w3.org/ns/oa#hasSelector": [
                    {
                        "@type": ["http://www.w3.org/ns/oa#FragmentSelector"],
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#value": [
                            {
                                "@value": "epubcfi(/6/4[chap01ref]!/4[body01]/10[para05]/3:10)"
                            }
                        ],
                    }
                ],
                "http://www.w3.org/ns/oa#hasSource": [
                    {"@id": annotation_parser_fixture.identifier.urn}
                ],
            }
        ]

        data_json = json.dumps(data)

        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            data_json,
            annotation_parser_fixture.patron_value,
        )
        assert annotation_parser_fixture.patron_value.id == annotation.patron_id
        assert annotation_parser_fixture.identifier.id == annotation.identifier_id
        assert Annotation.IDLING == annotation.motivation
        assert True == annotation.active
        assert (
            json.dumps(data["http://www.w3.org/ns/oa#hasTarget"][0])
            == annotation.target
        )
        assert (
            json.dumps(data["http://www.w3.org/ns/oa#hasBody"][0]) == annotation.content
        )

    def test_parse_compacted_jsonld(
        self, annotation_parser_fixture: AnnotationParserFixture
    ):
        annotation_parser_fixture.pool.loan_to(annotation_parser_fixture.patron_value)

        data: Dict[Any, Any] = dict()
        data["@type"] = "http://www.w3.org/ns/oa#Annotation"
        data["http://www.w3.org/ns/oa#motivatedBy"] = {"@id": Annotation.IDLING}
        data["http://www.w3.org/ns/oa#hasBody"] = {
            "@type": "http://www.w3.org/ns/oa#TextualBody",
            "http://www.w3.org/ns/oa#bodyValue": "A good description of the topic that bears further investigation",
            "http://www.w3.org/ns/oa#hasPurpose": {
                "@id": "http://www.w3.org/ns/oa#describing"
            },
        }
        data["http://www.w3.org/ns/oa#hasTarget"] = {
            "http://www.w3.org/ns/oa#hasSource": {
                "@id": annotation_parser_fixture.identifier.urn
            },
            "http://www.w3.org/ns/oa#hasSelector": {
                "@type": "http://www.w3.org/ns/oa#FragmentSelector",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#value": "epubcfi(/6/4[chap01ref]!/4[body01]/10[para05]/3:10)",
            },
        }

        data_json = json.dumps(data)
        expanded = jsonld.expand(data)[0]

        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            data_json,
            annotation_parser_fixture.patron_value,
        )
        assert annotation_parser_fixture.patron_value.id == annotation.patron_id
        assert annotation_parser_fixture.identifier.id == annotation.identifier_id
        assert Annotation.IDLING == annotation.motivation
        assert True == annotation.active
        assert (
            json.dumps(expanded["http://www.w3.org/ns/oa#hasTarget"][0])
            == annotation.target
        )
        assert (
            json.dumps(expanded["http://www.w3.org/ns/oa#hasBody"][0])
            == annotation.content
        )

    def test_parse_jsonld_with_context(
        self, annotation_parser_fixture: AnnotationParserFixture
    ):
        annotation_parser_fixture.pool.loan_to(annotation_parser_fixture.patron_value)

        data = self._sample_jsonld(annotation_parser_fixture)
        data_json = json.dumps(data)
        expanded = jsonld.expand(data)[0]

        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            data_json,
            annotation_parser_fixture.patron_value,
        )

        assert annotation_parser_fixture.patron_value.id == annotation.patron_id
        assert annotation_parser_fixture.identifier.id == annotation.identifier_id
        assert Annotation.IDLING == annotation.motivation
        assert True == annotation.active
        assert (
            json.dumps(expanded["http://www.w3.org/ns/oa#hasTarget"][0])
            == annotation.target
        )
        assert (
            json.dumps(expanded["http://www.w3.org/ns/oa#hasBody"][0])
            == annotation.content
        )

    def test_parse_jsonld_with_bookmarking_motivation(
        self, annotation_parser_fixture: AnnotationParserFixture
    ):
        """You can create multiple bookmarks in a single book."""
        annotation_parser_fixture.pool.loan_to(annotation_parser_fixture.patron_value)

        data = self._sample_jsonld(
            annotation_parser_fixture, motivation=Annotation.BOOKMARKING
        )
        data_json = json.dumps(data)
        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            data_json,
            annotation_parser_fixture.patron_value,
        )
        assert Annotation.BOOKMARKING == annotation.motivation

        # You can't create another bookmark at the exact same location --
        # you just get the same annotation again.
        annotation2 = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            data_json,
            annotation_parser_fixture.patron_value,
        )
        assert annotation == annotation2

        # But unlike with IDLING, you _can_ create multiple bookmarks
        # for the same identifier, so long as the selector value
        # (ie. the location within the book) is different.
        data["target"]["selector"][
            "value"
        ] = "epubcfi(/3/4[chap01ref]!/4[body01]/15[para05]/3:10)"
        data_json = json.dumps(data)
        annotation3 = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            data_json,
            annotation_parser_fixture.patron_value,
        )
        assert annotation3 != annotation
        assert 2 == len(annotation_parser_fixture.patron_value.annotations)

    def test_parse_jsonld_with_invalid_motivation(
        self, annotation_parser_fixture: AnnotationParserFixture
    ):
        annotation_parser_fixture.pool.loan_to(annotation_parser_fixture.patron_value)

        data = self._sample_jsonld(annotation_parser_fixture)
        data["motivation"] = "not-a-valid-motivation"
        data_json = json.dumps(data)

        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            data_json,
            annotation_parser_fixture.patron_value,
        )

        assert INVALID_ANNOTATION_MOTIVATION == annotation

    def test_parse_jsonld_with_no_loan(
        self, annotation_parser_fixture: AnnotationParserFixture
    ):
        data = self._sample_jsonld(annotation_parser_fixture)
        data_json = json.dumps(data)

        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            data_json,
            annotation_parser_fixture.patron_value,
        )

        assert INVALID_ANNOTATION_TARGET == annotation

    def test_parse_jsonld_with_no_target(
        self, annotation_parser_fixture: AnnotationParserFixture
    ):
        data = self._sample_jsonld(annotation_parser_fixture)
        del data["target"]
        data_json = json.dumps(data)

        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            data_json,
            annotation_parser_fixture.patron_value,
        )

        assert INVALID_ANNOTATION_TARGET == annotation

    def test_parse_updates_existing_annotation(
        self, annotation_parser_fixture: AnnotationParserFixture
    ):
        annotation_parser_fixture.pool.loan_to(annotation_parser_fixture.patron_value)

        original_annotation, ignore = create(
            annotation_parser_fixture.db.session,
            Annotation,
            patron_id=annotation_parser_fixture.patron_value.id,
            identifier_id=annotation_parser_fixture.identifier.id,
            motivation=Annotation.IDLING,
        )
        original_annotation.active = False
        yesterday = utc_now() - datetime.timedelta(days=1)
        original_annotation.timestamp = yesterday

        data = self._sample_jsonld(annotation_parser_fixture)
        data = json.dumps(data)

        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            data,
            annotation_parser_fixture.patron_value,
        )

        assert original_annotation == annotation
        assert True == annotation.active
        assert annotation.timestamp > yesterday

    def test_parse_treats_duplicates_as_interchangeable(
        self, annotation_parser_fixture: AnnotationParserFixture
    ):
        annotation_parser_fixture.pool.loan_to(annotation_parser_fixture.patron_value)

        # Due to an earlier race condition, two duplicate annotations
        # were put in the database.
        a1, ignore = create(
            annotation_parser_fixture.db.session,
            Annotation,
            patron_id=annotation_parser_fixture.patron_value.id,
            identifier_id=annotation_parser_fixture.identifier.id,
            motivation=Annotation.IDLING,
        )

        a2, ignore = create(
            annotation_parser_fixture.db.session,
            Annotation,
            patron_id=annotation_parser_fixture.patron_value.id,
            identifier_id=annotation_parser_fixture.identifier.id,
            motivation=Annotation.IDLING,
        )

        assert a1 != a2

        # Parsing the annotation again retrieves one or the other
        # of the annotations rather than crashing or creating a third
        # annotation.
        data = self._sample_jsonld(annotation_parser_fixture)
        data = json.dumps(data)
        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            data,
            annotation_parser_fixture.patron_value,
        )
        assert annotation in (a1, a2)

    def test_parse_jsonld_with_patron_opt_out(
        self, annotation_parser_fixture: AnnotationParserFixture
    ):
        annotation_parser_fixture.pool.loan_to(annotation_parser_fixture.patron_value)
        data = self._sample_jsonld(annotation_parser_fixture)
        data_json = json.dumps(data)

        annotation_parser_fixture.patron_value.synchronize_annotations = False
        annotation = AnnotationParser.parse(
            annotation_parser_fixture.db.session,
            data_json,
            annotation_parser_fixture.patron_value,
        )
        assert PATRON_NOT_OPTED_IN_TO_ANNOTATION_SYNC == annotation
