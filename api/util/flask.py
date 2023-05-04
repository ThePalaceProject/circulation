from typing import Any

from flask import Flask
from sqlalchemy.orm import Session


class PalaceFlask(Flask):
    """A subclass of Flask sets properties used by Palace."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db: Session
        self.manager: Any
