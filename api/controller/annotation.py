from __future__ import annotations

import json
from time import mktime
from wsgiref.handlers import format_date_time

import flask
from flask import Response

from api.annotations import AnnotationParser, AnnotationWriter
from api.controller.circulation_manager import CirculationManagerController
from api.problem_details import NO_ANNOTATION
from core.model import Annotation, Identifier, get_one
from core.util.problem_detail import ProblemDetail


class AnnotationController(CirculationManagerController):
    def container(self, identifier=None, accept_post=True):
        headers = dict()
        if accept_post:
            headers["Allow"] = "GET,HEAD,OPTIONS,POST"
            headers["Accept-Post"] = AnnotationWriter.CONTENT_TYPE
        else:
            headers["Allow"] = "GET,HEAD,OPTIONS"

        if flask.request.method == "HEAD":
            return Response(status=200, headers=headers)

        patron = flask.request.patron

        if flask.request.method == "GET":
            headers["Link"] = [
                '<http://www.w3.org/ns/ldp#BasicContainer>; rel="type"',
                '<http://www.w3.org/TR/annotation-protocol/>; rel="http://www.w3.org/ns/ldp#constrainedBy"',
            ]
            headers["Content-Type"] = AnnotationWriter.CONTENT_TYPE

            container, timestamp = AnnotationWriter.annotation_container_for(
                patron, identifier=identifier
            )
            etag = 'W/""'
            if timestamp:
                etag = 'W/"%s"' % timestamp
                headers["Last-Modified"] = format_date_time(
                    mktime(timestamp.timetuple())
                )
            headers["ETag"] = etag

            content = json.dumps(container)
            return Response(content, status=200, headers=headers)

        data = flask.request.data
        annotation = AnnotationParser.parse(self._db, data, patron)

        if isinstance(annotation, ProblemDetail):
            return annotation

        content = json.dumps(AnnotationWriter.detail(annotation))
        status_code = 200
        headers["Link"] = '<http://www.w3.org/ns/ldp#Resource>; rel="type"'
        headers["Content-Type"] = AnnotationWriter.CONTENT_TYPE
        return Response(content, status_code, headers)

    def container_for_work(self, identifier_type, identifier):
        id_obj, ignore = Identifier.for_foreign_id(
            self._db, identifier_type, identifier
        )
        return self.container(identifier=id_obj, accept_post=False)

    def detail(self, annotation_id):
        headers = dict()
        headers["Allow"] = "GET,HEAD,OPTIONS,DELETE"

        if flask.request.method == "HEAD":
            return Response(status=200, headers=headers)

        patron = flask.request.patron

        annotation = get_one(
            self._db, Annotation, patron=patron, id=annotation_id, active=True
        )

        if not annotation:
            return NO_ANNOTATION

        if flask.request.method == "DELETE":
            annotation.set_inactive()
            return Response()

        content = json.dumps(AnnotationWriter.detail(annotation))
        status_code = 200
        headers["Link"] = '<http://www.w3.org/ns/ldp#Resource>; rel="type"'
        headers["Content-Type"] = AnnotationWriter.CONTENT_TYPE
        return Response(content, status_code, headers)
