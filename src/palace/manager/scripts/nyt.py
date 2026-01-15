from __future__ import annotations

import argparse
from typing import Any

from sqlalchemy.orm import Session

from palace.manager.celery.tasks.nyt import update_nyt_best_sellers_lists
from palace.manager.scripts.base import Script


class NYTBestSellerListsScript(Script):
    name = "Update New York Times best-seller lists by kicking off an asynchronous task"

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Rebuild the search index from scratch."
        )
        parser.add_argument(
            "-i",
            "--include-history",
            action="store_true",
            help="Include the history",
        )
        return parser

    def do_run(self, *args: Any, **kwargs: Any) -> None:
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        update_nyt_best_sellers_lists.delay(include_history=parsed.include_history)
        self.log.info(
            f"Successfully queued update_nyt_best_sellers_lists task "
            f"(include_history={str(parsed.include_history)}"
        )
