import datetime
import json

from palace.manager.core.query.customlist import CustomListQueries
from palace.manager.scripts.base import Script
from palace.manager.scripts.input import LibraryInputScript
from palace.manager.sqlalchemy.model.customlist import CustomList
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.util import get_one_or_create


class CustomListSweeperScript(LibraryInputScript):
    """Do something to each custom list in a library."""

    def process_library(self, library):
        lists = self._db.query(CustomList).filter(CustomList.library_id == library.id)
        for l in lists:
            self.process_custom_list(l)
        self._db.commit()

    def process_custom_list(self, custom_list):
        pass


class CustomListManagementScript(Script):
    """Maintain a CustomList whose membership is determined by a
    MembershipManager.
    """

    def __init__(
        self,
        manager_class,
        data_source_name,
        list_identifier,
        list_name,
        primary_language,
        description,
        **manager_kwargs,
    ):
        data_source = DataSource.lookup(self._db, data_source_name)
        self.custom_list, is_new = get_one_or_create(
            self._db,
            CustomList,
            data_source_id=data_source.id,
            foreign_identifier=list_identifier,
        )
        self.custom_list.primary_language = primary_language
        self.custom_list.description = description
        self.membership_manager = manager_class(self.custom_list, **manager_kwargs)

    def run(self):
        self.membership_manager.update()
        self._db.commit()


class UpdateCustomListSizeScript(CustomListSweeperScript):
    def process_custom_list(self, custom_list):
        custom_list.update_size(self._db)


class CustomListUpdateEntriesScript(CustomListSweeperScript):
    """Traverse all entries and update lists if they have auto_update_enabled"""

    def process_custom_list(self, custom_list: CustomList):
        if not custom_list.auto_update_enabled:
            return
        try:
            self.log.info(f"Auto updating list entries for: {custom_list.name}")
            self._update_list_with_new_entries(custom_list)
        except Exception:
            self.log.exception(f"Could not auto update {custom_list.name}")

    def _update_list_with_new_entries(self, custom_list: CustomList):
        """Run a search on a custom list, assuming we have auto_update_enabled with a valid query
        Only json type queries are supported right now, without any support for additional facets
        """

        start_page = 1
        json_query = None
        if custom_list.auto_update_status == CustomList.INIT:
            # We're in the init phase, we need to back-populate all titles
            # starting from page 2, since page 1 should be already populated
            start_page = 2
        elif custom_list.auto_update_status == CustomList.REPOPULATE:
            # During a repopulate phase we must empty the list
            # and start population from page 1
            for entry in custom_list.entries:
                self._db.delete(entry)
            custom_list.entries = []
        else:
            # Otherwise we are in an update type process, which means we only search for
            # "newer" books from the last time we updated the list
            try:
                if custom_list.auto_update_query:
                    json_query = json.loads(custom_list.auto_update_query)
                else:
                    return
            except json.JSONDecodeError as e:
                self.log.error(
                    f"Could not decode custom list({custom_list.id}) saved query: {e}"
                )
                return
            # Update availability time as a query part that allows us to filter for new licenses
            # Although the last_update should never be null, we're failsafing
            availability_time = (
                custom_list.auto_update_last_update or datetime.datetime.now()
            )
            query_part = json_query["query"]
            query_part = {
                "and": [
                    {
                        "key": "licensepools.availability_time",
                        "op": "gte",
                        "value": availability_time.timestamp(),
                    },
                    query_part,
                ]
            }
            # Update the query as such
            json_query["query"] = query_part

        search = self.services.search.index()
        CustomListQueries.populate_query_pages(
            self._db, search, custom_list, json_query=json_query, start_page=start_page
        )
        custom_list.auto_update_status = CustomList.UPDATED
