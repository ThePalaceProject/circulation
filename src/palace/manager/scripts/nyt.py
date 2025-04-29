from __future__ import annotations

import argparse

from palace.manager.celery.tasks.nyt import update_nyt_best_sellers_lists
from palace.manager.scripts.base import Script


class NYTBestSellerListsScript(Script):
    name = "Update New York Times best-seller lists by kicking off an asynchronous task"

    def __init__(self, include_history=False):
        super().__init__()
        self.include_history = include_history

    @classmethod
    def arg_parser(cls) -> argparse.ArgumentParser:
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

    def do_run(self):
        update_nyt_best_sellers_lists.delay(include_history=self.include_history)
