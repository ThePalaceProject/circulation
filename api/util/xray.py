import logging
import os
from typing import Optional

from aws_xray_sdk.core import AWSXRayRecorder
from aws_xray_sdk.core import patch as xray_patch
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.models.segment import Segment
from aws_xray_sdk.ext.flask.middleware import XRayMiddleware
from aws_xray_sdk.ext.httplib import add_ignored as httplib_add_ignored
from flask import Flask, Response, request, session

from core.config import Configuration


class PalaceXrayUtils:
    XRAY_ENV_ENABLE = "PALACE_XRAY"
    XRAY_ENV_NAME = "PALACE_XRAY_NAME"
    XRAY_ENV_ANNOTATE = "PALACE_XRAY_ANNOTATE_"

    @classmethod
    def put_annotations(
        cls, segment: Optional[Segment], seg_type: Optional[str] = None
    ):
        if seg_type is not None:
            segment.put_annotation("type", seg_type)

        for env, value in os.environ.items():
            if env.startswith(cls.XRAY_ENV_ANNOTATE):
                name = env.replace(cls.XRAY_ENV_ANNOTATE, "").lower()
                segment.put_annotation(name, value)

        if Configuration.app_version() != Configuration.NO_APP_VERSION_FOUND:
            segment.put_annotation("version", Configuration.app_version())

    @classmethod
    def setup_xray(cls):
        name = os.environ.get(cls.XRAY_ENV_NAME, "Palace")
        xray_recorder.configure(
            service=name,
            streaming_threshold=5,
            context_missing="LOG_ERROR",
            plugins=["EC2Plugin"],
        )
        xray_patch(("httplib", "sqlalchemy_core", "requests"))
        httplib_add_ignored(hostname="logs.*.amazonaws.com")

    @classmethod
    def configure_app(cls, app: Flask):
        enable_xray = os.environ.get(cls.XRAY_ENV_ENABLE)
        if enable_xray and enable_xray.lower() == "true":
            logging.getLogger().info("Configuring app with AWS XRAY.")
            cls.setup_xray()
            PalaceXrayMiddleware(app, xray_recorder)


class PalaceXrayMiddleware(XRayMiddleware):
    def __init__(self, app: Flask, recorder: AWSXRayRecorder):
        super().__init__(app, recorder)

        # Add an additional hook to before first request
        self.app.before_first_request(self._before_first_request)

    def _before_first_request(self):
        self._before_request()
        segment = self._recorder.current_segment()

        # Add an annotation for the first request, since it does extra caching work.
        segment.put_annotation("request", "first")
        request._palace_first_request = True

    def _before_request(self):
        if getattr(request, "_palace_first_request", None) is not None:
            # If we are in the first request this work is already done
            return
        super()._before_request()
        PalaceXrayUtils.put_annotations(self._recorder.current_segment(), "web")

    def _after_request(self, response: Response):
        super()._after_request(response)

        segment = self._recorder.current_segment()
        # Add library shortname
        if hasattr(request, "library"):
            segment.put_annotation("library", str(request.library.short_name))

        # Add patron data
        if hasattr(request, "patron"):
            segment.set_user(str(request.patron.authorization_identifier))
            segment.put_annotation(
                "barcode", str(request.patron.authorization_identifier)
            )

        # Add admin UI username
        if "admin_email" in session:
            segment.set_user(session["admin_email"])

        return response
