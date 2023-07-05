import datetime
import json
from time import mktime
from typing import Union
from wsgiref.handlers import format_date_time

import pytest

from api.annotations import AnnotationWriter
from core.model import Annotation, create
from core.util.datetime_helpers import utc_now
from tests.fixtures.api_controller import CirculationControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture


class AnnotationFixture(CirculationControllerFixture):
    def __init__(self, db: DatabaseTransactionFixture):
        super().__init__(db)
        self.pool = self.english_1.license_pools[0]
        self.edition = self.pool.presentation_edition
        self.identifier = self.edition.primary_identifier


@pytest.fixture(scope="function")
def annotation_fixture(db: DatabaseTransactionFixture):
    return AnnotationFixture(db)


class TestAnnotationController:
    def test_get_empty_container(self, annotation_fixture: AnnotationFixture):
        with annotation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=annotation_fixture.valid_auth)
        ):
            annotation_fixture.manager.loans.authenticated_patron_from_request()
            response = annotation_fixture.manager.annotations.container()
            assert 200 == response.status_code

            # We've been given an annotation container with no items.
            container = json.loads(response.get_data(as_text=True))
            assert [] == container["first"]["items"]
            assert 0 == container["total"]

            # The response has the appropriate headers.
            allow_header = response.headers["Allow"]
            for method in ["GET", "HEAD", "OPTIONS", "POST"]:
                assert method in allow_header

            assert AnnotationWriter.CONTENT_TYPE == response.headers["Accept-Post"]
            assert AnnotationWriter.CONTENT_TYPE == response.headers["Content-Type"]
            assert 'W/""' == response.headers["ETag"]

    def test_get_container_with_item(self, annotation_fixture: AnnotationFixture):
        annotation_fixture.pool.loan_to(annotation_fixture.default_patron)

        annotation, ignore = create(
            annotation_fixture.db.session,
            Annotation,
            patron=annotation_fixture.default_patron,
            identifier=annotation_fixture.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True
        annotation.timestamp = utc_now()

        with annotation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=annotation_fixture.valid_auth)
        ):
            annotation_fixture.manager.annotations.authenticated_patron_from_request()
            response = annotation_fixture.manager.annotations.container()
            assert 200 == response.status_code

            # We've been given an annotation container with one item.
            container = json.loads(response.get_data(as_text=True))
            assert 1 == container["total"]
            item = container["first"]["items"][0]
            assert annotation.motivation == item["motivation"]

            # The response has the appropriate headers.
            allow_header = response.headers["Allow"]
            for method in ["GET", "HEAD", "OPTIONS", "POST"]:
                assert method in allow_header

            assert AnnotationWriter.CONTENT_TYPE == response.headers["Accept-Post"]
            assert AnnotationWriter.CONTENT_TYPE == response.headers["Content-Type"]
            expected_etag = 'W/"%s"' % annotation.timestamp
            assert expected_etag == response.headers["ETag"]
            assert isinstance(annotation.timestamp, datetime.datetime)
            expected_time = format_date_time(mktime(annotation.timestamp.timetuple()))
            assert expected_time == response.headers["Last-Modified"]

    def test_get_container_for_work(self, annotation_fixture: AnnotationFixture):
        annotation_fixture.pool.loan_to(annotation_fixture.default_patron)

        annotation, ignore = create(
            annotation_fixture.db.session,
            Annotation,
            patron=annotation_fixture.default_patron,
            identifier=annotation_fixture.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True
        annotation.timestamp = utc_now()

        other_annotation, ignore = create(
            annotation_fixture.db.session,
            Annotation,
            patron=annotation_fixture.default_patron,
            identifier=annotation_fixture.db.identifier(),
            motivation=Annotation.IDLING,
        )

        with annotation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=annotation_fixture.valid_auth)
        ):
            annotation_fixture.manager.annotations.authenticated_patron_from_request()
            response = annotation_fixture.manager.annotations.container_for_work(
                annotation_fixture.identifier.type,
                annotation_fixture.identifier.identifier,
            )
            assert 200 == response.status_code

            # We've been given an annotation container with one item.
            container = json.loads(response.get_data(as_text=True))
            assert 1 == container["total"]
            item = container["first"]["items"][0]
            assert annotation.motivation == item["motivation"]

            # The response has the appropriate headers - POST is not allowed.
            allow_header = response.headers["Allow"]
            for method in ["GET", "HEAD", "OPTIONS"]:
                assert method in allow_header

            assert "Accept-Post" not in list(response.headers.keys())
            assert AnnotationWriter.CONTENT_TYPE == response.headers["Content-Type"]
            expected_etag = 'W/"%s"' % annotation.timestamp
            assert expected_etag == response.headers["ETag"]
            assert isinstance(annotation.timestamp, datetime.datetime)
            expected_time = format_date_time(mktime(annotation.timestamp.timetuple()))
            assert expected_time == response.headers["Last-Modified"]

    def test_post_to_container(self, annotation_fixture: AnnotationFixture):
        data: dict[str, Union[str, dict]] = dict()
        data["@context"] = AnnotationWriter.JSONLD_CONTEXT
        data["type"] = "Annotation"
        data["motivation"] = Annotation.IDLING
        data["target"] = dict(
            source=annotation_fixture.identifier.urn,
            selector="epubcfi(/6/4[chap01ref]!/4[body01]/10[para05]/3:10)",
        )

        with annotation_fixture.request_context_with_library(
            "/",
            headers=dict(Authorization=annotation_fixture.valid_auth),
            method="POST",
            data=json.dumps(data),
        ):
            patron = (
                annotation_fixture.manager.annotations.authenticated_patron_from_request()
            )
            patron.synchronize_annotations = True
            # The patron doesn't have any annotations yet.
            annotations = (
                annotation_fixture.db.session.query(Annotation).filter(Annotation.patron == patron).all()  # type: ignore
            )
            assert 0 == len(annotations)

            response = annotation_fixture.manager.annotations.container()

            # The patron doesn't have the pool on loan yet, so the request fails.
            assert 400 == response.status_code
            annotations = (
                annotation_fixture.db.session.query(Annotation).filter(Annotation.patron == patron).all()  # type: ignore
            )
            assert 0 == len(annotations)

            # Give the patron a loan and try again, and the request creates an annotation.
            annotation_fixture.pool.loan_to(patron)
            response = annotation_fixture.manager.annotations.container()
            assert 200 == response.status_code

            annotations = (
                annotation_fixture.db.session.query(Annotation).filter(Annotation.patron == patron).all()  # type: ignore
            )
            assert 1 == len(annotations)
            annotation = annotations[0]
            assert Annotation.IDLING == annotation.motivation
            selector = (
                json.loads(annotation.target)
                .get("http://www.w3.org/ns/oa#hasSelector")[0]
                .get("@id")
            )
            assert isinstance(data["target"], dict)
            assert data["target"]["selector"] == selector

            # The response contains the annotation in the db.
            item = json.loads(response.get_data(as_text=True))
            assert str(annotation.id) in item["id"]
            assert annotation.motivation == item["motivation"]

    def test_detail(self, annotation_fixture: AnnotationFixture):
        annotation_fixture.pool.loan_to(annotation_fixture.default_patron)

        annotation, ignore = create(
            annotation_fixture.db.session,
            Annotation,
            patron=annotation_fixture.default_patron,
            identifier=annotation_fixture.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True

        with annotation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=annotation_fixture.valid_auth)
        ):
            annotation_fixture.manager.annotations.authenticated_patron_from_request()
            response = annotation_fixture.manager.annotations.detail(annotation.id)
            assert 200 == response.status_code

            # We've been given a single annotation item.
            item = json.loads(response.get_data(as_text=True))
            assert str(annotation.id) in item["id"]
            assert annotation.motivation == item["motivation"]

            # The response has the appropriate headers.
            allow_header = response.headers["Allow"]
            for method in ["GET", "HEAD", "OPTIONS", "DELETE"]:
                assert method in allow_header

            assert AnnotationWriter.CONTENT_TYPE == response.headers["Content-Type"]

    def test_detail_for_other_patrons_annotation_returns_404(
        self, annotation_fixture: AnnotationFixture
    ):
        patron = annotation_fixture.db.patron()
        annotation_fixture.pool.loan_to(patron)

        annotation, ignore = create(
            annotation_fixture.db.session,
            Annotation,
            patron=patron,
            identifier=annotation_fixture.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True

        with annotation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=annotation_fixture.valid_auth)
        ):
            annotation_fixture.manager.annotations.authenticated_patron_from_request()

            # The patron can't see that this annotation exists.
            response = annotation_fixture.manager.annotations.detail(annotation.id)
            assert 404 == response.status_code

    def test_detail_for_missing_annotation_returns_404(
        self, annotation_fixture: AnnotationFixture
    ):
        with annotation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=annotation_fixture.valid_auth)
        ):
            annotation_fixture.manager.annotations.authenticated_patron_from_request()

            # This annotation does not exist.
            response = annotation_fixture.manager.annotations.detail(100)
            assert 404 == response.status_code

    def test_detail_for_deleted_annotation_returns_404(
        self, annotation_fixture: AnnotationFixture
    ):
        annotation_fixture.pool.loan_to(annotation_fixture.default_patron)

        annotation, ignore = create(
            annotation_fixture.db.session,
            Annotation,
            patron=annotation_fixture.default_patron,
            identifier=annotation_fixture.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = False

        with annotation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=annotation_fixture.valid_auth)
        ):
            annotation_fixture.manager.annotations.authenticated_patron_from_request()
            response = annotation_fixture.manager.annotations.detail(annotation.id)
            assert 404 == response.status_code

    def test_delete(self, annotation_fixture: AnnotationFixture):
        annotation_fixture.pool.loan_to(annotation_fixture.default_patron)

        annotation, ignore = create(
            annotation_fixture.db.session,
            Annotation,
            patron=annotation_fixture.default_patron,
            identifier=annotation_fixture.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True

        with annotation_fixture.request_context_with_library(
            "/",
            method="DELETE",
            headers=dict(Authorization=annotation_fixture.valid_auth),
        ):
            annotation_fixture.manager.annotations.authenticated_patron_from_request()
            response = annotation_fixture.manager.annotations.detail(annotation.id)
            assert 200 == response.status_code

            # The annotation has been marked inactive.
            assert False == annotation.active
