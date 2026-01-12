from __future__ import annotations

import os

from palace.manager.api.config import Configuration
from palace.manager.scripts.base import Script


class CompileTranslationsScript(Script):
    """A script to combine translation files for circulation, core
    and the admin interface, and compile the result to be used by the
    app. The combination step is necessary because Flask-Babel does not
    support multiple domains yet.
    """

    def run(self) -> None:
        languages = Configuration.localization_languages()
        for language in languages:
            base_path = f"translations/{language}/LC_MESSAGES"
            if not os.path.exists(base_path):
                self.log.warning("No translations for configured language %s", language)
                continue

            os.system(f"rm {base_path}/messages.po")
            os.system(f"cat {base_path}/*.po > {base_path}/messages.po")

        os.system("pybabel compile -f -d translations")
