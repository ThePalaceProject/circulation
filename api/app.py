import logging
import os
import urllib.parse

import flask_babel
from flask_babel import Babel
from flask_pydantic_spec import FlaskPydanticSpec

from api.config import Configuration
from core.flask_sqlalchemy_session import flask_scoped_session
from core.local_analytics_provider import LocalAnalyticsProvider
from core.log import LogConfiguration
from core.model import ConfigurationSetting, SessionManager
from core.model.configuration import access_exclusive_lock_configurationsettings
from core.util import LanguageCodes
from core.util.cache import CachedData

from .admin.controller import setup_admin_controllers
from .controller import CirculationManager
from .util.flask import PalaceFlask
from .util.profilers import (
    PalaceCProfileProfiler,
    PalacePyInstrumentProfiler,
    PalaceXrayProfiler,
)

app = PalaceFlask(__name__)
app._db = None  # type: ignore [assignment]
app.config["BABEL_DEFAULT_LOCALE"] = LanguageCodes.three_to_two[
    Configuration.localization_languages()[0]
]
app.config["BABEL_TRANSLATION_DIRECTORIES"] = "../translations"
babel = Babel(app)

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


def initialize_application() -> PalaceFlask:
    with app.app_context(), flask_babel.force_locale("en"):
        initialize_database()
        initialize_circulation_manager()
        initialize_admin()
    return app


def initialize_database(autoinitialize=True):
    testing = "TESTING" in os.environ
    db_url = Configuration.database_url()
    if autoinitialize:
        SessionManager.initialize(db_url)
    session_factory = SessionManager.sessionmaker(db_url)
    _db = flask_scoped_session(session_factory, app)
    app._db = _db
    log_level = LogConfiguration.initialize(_db, testing=testing)
    debug = log_level == "DEBUG"
    app.config["DEBUG"] = debug
    app.debug = debug
    _db.commit()

    logging.getLogger().info("Application debug mode==%r" % app.debug)


def initialize_admin(_db=None):
    if getattr(app, "manager", None) is not None:
        setup_admin_controllers(app.manager)
    _db = _db or app._db

    with access_exclusive_lock_configurationsettings(_db) as db_session:
        # The secret key is used for signing cookies for admin login
        app.secret_key = ConfigurationSetting.sitewide_secret(
            db_session, Configuration.SECRET_KEY
        )
        # Create a default Local Analytics service if one does not
        # already exist.
        local_analytics = LocalAnalyticsProvider.initialize(db_session)


def initialize_circulation_manager():
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
            app._db.commit()

            # setup the cache data object
            CachedData.initialize(app._db)


from . import routes  # noqa
from .admin import routes as admin_routes  # noqa


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

    initialize_application()
    logging.info("Starting app on %s:%s", host, port)
    sslContext = "adhoc" if scheme == "https" else None
    app.run(debug=debug, host=host, port=port, threaded=True, ssl_context=sslContext)
