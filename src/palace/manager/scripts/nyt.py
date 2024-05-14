from __future__ import annotations

from palace.manager.api.metadata.nyt import NYTBestSellerAPI
from palace.manager.scripts.timestamp import TimestampScript
from palace.manager.sqlalchemy.model.datasource import DataSource


class NYTBestSellerListsScript(TimestampScript):
    name = "Update New York Times best-seller lists"

    def __init__(self, include_history=False):
        super().__init__()
        self.include_history = include_history

    def do_run(self):
        self.api = NYTBestSellerAPI.from_config(self._db)
        self.data_source = DataSource.lookup(self._db, DataSource.NYT)
        # For every best-seller list...
        names = self.api.list_of_lists()
        for l in sorted(names["results"], key=lambda x: x["list_name_encoded"]):
            name = l["list_name_encoded"]
            self.log.info("Handling list %s" % name)
            best = self.api.best_seller_list(l)

            if self.include_history:
                self.api.fill_in_history(best)
            else:
                self.api.update(best)

            # Mirror the list to the database.
            customlist = best.to_customlist(self._db)
            self.log.info("Now %s entries in the list.", len(customlist.entries))
            self._db.commit()
