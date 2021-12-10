import logging
import os
import time
from pathlib import Path
from typing import Optional

from flask import Flask, g, request


class PalaceProfiler:
    ENVIRONMENT_VARIABLE: str

    @classmethod
    def enabled(cls) -> bool:
        return os.environ.get(cls.ENVIRONMENT_VARIABLE, None) is not None

    @classmethod
    def create_profile_dir(cls) -> Optional[Path]:
        if not cls.enabled():
            return None

        profile_dir = Path(os.environ.get(cls.ENVIRONMENT_VARIABLE))
        if not profile_dir.exists():
            profile_dir.mkdir(parents=True)

        return profile_dir

    @classmethod
    def configure(cls, app: Flask):
        raise NotImplementedError


class PalacePyInstrumentProfiler(PalaceProfiler):
    ENVIRONMENT_VARIABLE = "PALACE_PYINSTRUMENT"

    @classmethod
    def configure(cls, app):
        profile_dir = cls.create_profile_dir()
        if profile_dir is None:
            # We are not configured
            return

        # Don't import if we are not profiling
        from pyinstrument import Profiler

        @app.before_first_request
        @app.before_request
        def before_request():
            if "profiler" not in g:
                g.profiler = Profiler()
                g.profiler_starttime = time.time()
                g.profiler.start()

        @app.after_request
        def after_request(response):
            if "profiler" in g:
                session = g.profiler.stop()
                elapsed = (time.time() - g.profiler_starttime) * 1000.0
                request_path = request.path.strip("/").replace("/", ".") or "root"
                filename = f"{time.time():.0f}.{request.method}.{request_path}.{elapsed:.0f}ms.pyisession"
                session.save(profile_dir / filename)
            return response


class PalaceCProfileProfiler(PalaceProfiler):
    ENVIRONMENT_VARIABLE = "PALACE_CPROFILE"

    @classmethod
    def configure(cls, app):
        profile_dir = cls.create_profile_dir()
        if profile_dir is None:
            # We are not configured
            return

        from werkzeug.middleware.profiler import ProfilerMiddleware

        app.config["PROFILE"] = True
        app.wsgi_app = ProfilerMiddleware(app.wsgi_app, profile_dir=str(profile_dir))


class PalaceXrayProfiler(PalaceProfiler):
    ENVIRONMENT_VARIABLE = "PALACE_XRAY"

    @classmethod
    def configure(cls, app):
        if not cls.enabled():
            return

        from aws_xray_sdk.core import xray_recorder

        from api.util.xray import PalaceXrayMiddleware

        logging.getLogger(cls.__name__).info("Configuring app with AWS XRAY.")
        PalaceXrayMiddleware.setup_xray(xray_recorder)
        PalaceXrayMiddleware(app, xray_recorder)
