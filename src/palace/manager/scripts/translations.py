from __future__ import annotations

import logging
import os

from palace.manager.api.config import Configuration
from palace.manager.scripts.base import Script


class CompileTranslationsScript(Script):
    """A script to combine translation files for circulation, core
    and the admin interface, and compile the result to be used by the
    app. The combination step is necessary because Flask-Babel does not
    support multiple domains yet.
    """

    def run(self):
        languages = Configuration.localization_languages()
        for language in languages:
            base_path = "translations/%s/LC_MESSAGES" % language
            if not os.path.exists(base_path):
                logging.warn("No translations for configured language %s" % language)
                continue

            os.system("rm %(path)s/messages.po" % dict(path=base_path))
            os.system("cat %(path)s/*.po > %(path)s/messages.po" % dict(path=base_path))

        os.system("pybabel compile -f -d translations")
