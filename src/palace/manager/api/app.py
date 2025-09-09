import logging
import os

import flask_babel
from flask import request
from flask_babel import Babel
from sqlalchemy.orm import Session

from palace.manager.api.admin.controller import setup_admin_controllers
from palace.manager.api.circulation_manager import CirculationManager
from palace.manager.api.config import Configuration
from palace.manager.api.util.flask import PalaceFlask
from palace.manager.api.util.profilers import (
    PalaceCProfileProfiler,
    PalacePyInstrumentProfiler,
    PalaceXrayProfiler,
)
from palace.manager.core.app_server import ErrorHandler
from palace.manager.service.container import container_instance
from palace.manager.sqlalchemy.flask_sqlalchemy_session import flask_scoped_session
from palace.manager.sqlalchemy.model.key import Key, KeyType
from palace.manager.sqlalchemy.session import SessionManager
from palace.manager.util.cache import CachedData
from palace.manager.util.http.http import HTTP
from palace.manager.util.languages import LanguageCodes


def get_locale():
    """The localization selection function to be used with flask-babel"""
    languages = Configuration.localization_languages()
    return request.accept_languages.best_match(languages, "en")


template_dir = os.path.join(os.path.dirname(__file__), "templates")
app = PalaceFlask(__name__, template_folder=template_dir)
app._db = None  # type: ignore [assignment]
app.config["BABEL_DEFAULT_LOCALE"] = LanguageCodes.three_to_two[
    Configuration.localization_languages()[0]
]
app.config["BABEL_TRANSLATION_DIRECTORIES"] = "../translations"
# TODO: Temporary fix to handle form data larger than 500,000 bytes.
#  Sometimes custom list form data was too large, resulting in 413 response.
#  The value here is chosen to roughly match the Docker nginx config.
app.config["MAX_FORM_MEMORY_SIZE"] = 75 * 1024 * 1024
babel = Babel(app, locale_selector=get_locale)

# We use URIs as identifiers throughout the application, meaning that
# we never want werkzeug's merge_slashes feature.
app.url_map.merge_slashes = False

# Optionally setup any profilers that are enabled
PalacePyInstrumentProfiler.configure(app)
PalaceCProfileProfiler.configure(app)
PalaceXrayProfiler.configure(app)


def initialize_admin(_db: Session | None = None):
    if getattr(app, "manager", None) is not None:
        setup_admin_controllers(app.manager)
    _db = _db or app._db
    # The secret key is used for signing cookies for admin login
    app.secret_key = Key.get_key(
        _db, KeyType.ADMIN_SECRET_KEY, raise_exception=True
    ).value


def initialize_circulation_manager():
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
    session_factory = SessionManager.sessionmaker(application_name="manager")
    _db = flask_scoped_session(session_factory, app)
    app._db = _db


from palace.manager.api import routes  # noqa
from palace.manager.api.admin import routes as admin_routes  # noqa


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
        error_handler = ErrorHandler(app)
        app.register_error_handler(Exception, error_handler.handle)

        # Initialize the circulation manager
        initialize_circulation_manager()
        initialize_admin()
    return app
