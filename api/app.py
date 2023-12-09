import logging
import os
import urllib.parse

import flask_babel
from flask import request
from flask_babel import Babel
from flask_pydantic_spec import FlaskPydanticSpec

from api.admin.controller import setup_admin_controllers
from api.circulation_manager import CirculationManager
from api.config import Configuration
from api.util.flask import PalaceFlask
from api.util.profilers import (
    PalaceCProfileProfiler,
    PalacePyInstrumentProfiler,
    PalaceXrayProfiler,
)
from core.app_server import ErrorHandler
from core.flask_sqlalchemy_session import flask_scoped_session
from core.model import (
    LOCK_ID_APP_INIT,
    ConfigurationSetting,
    SessionManager,
    pg_advisory_lock,
)
from core.service.container import Services, container_instance
from core.util import LanguageCodes
from core.util.cache import CachedData
from core.util.http import HTTP
from scripts import InstanceInitializationScript


def get_locale():
    """The localization selection function to be used with flask-babel"""
    languages = Configuration.localization_languages()
    return request.accept_languages.best_match(languages, "en")


app = PalaceFlask(__name__)
app._db = None  # type: ignore [assignment]
app.config["BABEL_DEFAULT_LOCALE"] = LanguageCodes.three_to_two[
    Configuration.localization_languages()[0]
]
app.config["BABEL_TRANSLATION_DIRECTORIES"] = "../translations"
babel = Babel(app, locale_selector=get_locale)

# The autodoc spec, can be accessed at "/apidoc/swagger"
api_spec = FlaskPydanticSpec(
    "Palace Manager", mode="strict", title="Palace Manager API"
)
api_spec.register(app)

# We use URIs as identifiers throughout the application, meaning that
# we never want werkzeug's merge_slashes feature.
app.url_map.merge_slashes = False

# Optionally setup any profilers that are enabled
PalacePyInstrumentProfiler.configure(app)
PalaceCProfileProfiler.configure(app)
PalaceXrayProfiler.configure(app)


def initialize_admin(_db=None):
    if getattr(app, "manager", None) is not None:
        setup_admin_controllers(app.manager)
    _db = _db or app._db
    # The secret key is used for signing cookies for admin login
    app.secret_key = ConfigurationSetting.sitewide_secret(_db, Configuration.SECRET_KEY)


def initialize_circulation_manager(container: Services):
    if os.environ.get("AUTOINITIALIZE") == "False":
        # It's the responsibility of the importing code to set app.manager
        # appropriately.
        pass
    else:
        if getattr(app, "manager", None) is None:
            try:
                app.manager = CirculationManager(app._db)
            except Exception:
                logging.exception("Error instantiating circulation manager!")
                raise
            # Make sure that any changes to the database (as might happen
            # on initial setup) are committed before continuing.
            app.manager._db.commit()

            # setup the cache data object
            CachedData.initialize(app._db)


def initialize_database():
    session_factory = SessionManager.sessionmaker()
    _db = flask_scoped_session(session_factory, app)
    app._db = _db


from api import routes  # noqa
from api.admin import routes as admin_routes  # noqa


def initialize_application() -> PalaceFlask:
    HTTP.set_quick_failure_settings()
    with app.app_context(), flask_babel.force_locale("en"):
        initialize_database()

        # Load the application service container
        container = container_instance()

        # Initialize the application services container, this will make sure
        # that the logging system is initialized.
        container.init_resources()

        # Initialize the applications error handler.
        error_handler = ErrorHandler(app, container.config.logging.level())
        app.register_error_handler(Exception, error_handler.handle)

        # TODO: Remove this lock once our settings are moved to integration settings.
        # We need this lock, so that only one instance of the application is
        # initialized at a time. This prevents database conflicts when multiple
        # CM instances try to create the same configurationsettings at the same
        # time during initialization. This should be able to go away once we
        # move our settings off the configurationsettings system.
        with pg_advisory_lock(app._db, LOCK_ID_APP_INIT):
            initialize_circulation_manager(container)
            initialize_admin()
    return app


def run(url=None):
    base_url = url or "http://localhost:6500/"
    scheme, netloc, path, parameters, query, fragment = urllib.parse.urlparse(base_url)
    if ":" in netloc:
        host, port = netloc.split(":")
        port = int(port)
    else:
        host = netloc
        port = 80

    # Required for subdomain support.
    app.config["SERVER_NAME"] = netloc

    debug = True

    # Workaround for a "Resource temporarily unavailable" error when
    # running in debug mode with the global socket timeout set by isbnlib
    if debug:
        import socket

        socket.setdefaulttimeout(None)

    # Setup database by initializing it or running migrations
    InstanceInitializationScript().run()
    initialize_application()
    logging.info("Starting app on %s:%s", host, port)

    sslContext = "adhoc" if scheme == "https" else None
    app.run(debug=debug, host=host, port=port, threaded=True, ssl_context=sslContext)
