import logging
import os
import time
from pathlib import Path
from typing import Optional

from flask import Flask, g, request


class PalaceProfiler:
    ENVIRONMENT_VARIABLE: str
    FILENAME_TEMPLATE = "{time:.0f}.{method}.{path}.{elapsed:.0f}ms"

    @classmethod
    def enabled(cls) -> bool:
        return os.environ.get(cls.ENVIRONMENT_VARIABLE, None) is not None

    @classmethod
    def create_profile_dir(cls) -> Optional[Path]:
        if not cls.enabled():
            return None

        profile_dir = Path(os.environ.get(cls.ENVIRONMENT_VARIABLE, ""))
        if not profile_dir.exists():
            profile_dir.mkdir(parents=True)

        return profile_dir

    @classmethod
    def configure(cls, app: Flask):
        raise NotImplementedError


class PalacePyInstrumentProfiler(PalaceProfiler):
    ENVIRONMENT_VARIABLE = "PALACE_PYINSTRUMENT"

    @classmethod
    def configure(cls, app: Flask):
        profile_dir = cls.create_profile_dir()
        if profile_dir is None:
            # We are not configured
            return

        # Don't import if we are not profiling
        from pyinstrument import Profiler

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
                filename = cls.FILENAME_TEMPLATE.format(
                    time=time.time(),
                    method=request.method,
                    path=request_path,
                    elapsed=elapsed,
                )
                filename += ".pyisession"
                session.save(profile_dir / filename)
            return response


class PalaceCProfileProfiler(PalaceProfiler):
    ENVIRONMENT_VARIABLE = "PALACE_CPROFILE"

    @classmethod
    def configure(cls, app: Flask):
        profile_dir = cls.create_profile_dir()
        if profile_dir is None:
            # We are not configured
            return

        from werkzeug.middleware.profiler import ProfilerMiddleware

        filename = cls.FILENAME_TEMPLATE + ".prof"
        app.config["PROFILE"] = True
        app.wsgi_app = ProfilerMiddleware(  # type: ignore
            app.wsgi_app, profile_dir=str(profile_dir), filename_format=filename
        )


class PalaceXrayProfiler(PalaceProfiler):
    ENVIRONMENT_VARIABLE = "PALACE_XRAY"

    @classmethod
    def configure(cls, app: Flask):
        if not cls.enabled():
            return

        from aws_xray_sdk.core import xray_recorder

        from api.util.xray import PalaceXrayMiddleware

        logging.getLogger(cls.__name__).info("Configuring app with AWS XRAY.")
        PalaceXrayMiddleware.setup_xray(xray_recorder)
        PalaceXrayMiddleware(app, xray_recorder)
