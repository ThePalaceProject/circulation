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
