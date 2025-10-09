from __future__ import annotations

import json
from collections.abc import Generator
from typing import Self

from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.manager.integration.base import integration_settings_update
from palace.manager.integration.goals import Goals
from palace.manager.integration.license.overdrive.constants import OVERDRIVE_LABEL
from palace.manager.integration.license.overdrive.settings import OverdriveChildSettings
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration


class OverdriveAdvantageAccount:
    """Holder and parser for data associated with Overdrive Advantage."""

    def __init__(
        self, parent_library_id: str, library_id: str, name: str, token: str
    ) -> None:
        """Constructor.

        :param parent_library_id: The library ID of the parent Overdrive
            account.
        :param library_id: The library ID of the Overdrive Advantage account.
        :param name: The name of the library whose Advantage account this is.
        :param token: The collection token for this Advantage account
        """
        self.parent_library_id = parent_library_id
        self.library_id = library_id
        self.name = name
        self.token = token

    @classmethod
    def from_representation(cls, content: str) -> Generator[Self]:
        """Turn the representation of an advantageAccounts link into a list of
        OverdriveAdvantageAccount objects.

        :param content: The data obtained by following an advantageAccounts
            link.
        :yield: A sequence of OverdriveAdvantageAccount objects.
        """
        data = json.loads(content)
        parent_id = str(data.get("id"))
        accounts = data.get("advantageAccounts", {})
        for account in accounts:
            name = account["name"]
            products_link = account["links"]["products"]["href"]
            library_id = str(account.get("id"))
            name = account.get("name")
            token = account.get("collectionToken")
            yield cls(
                parent_library_id=parent_id,
                library_id=library_id,
                name=name,
                token=token,
            )

    def to_collection(self, _db: Session) -> tuple[Collection, Collection]:
        """Find or create a Collection object for this Overdrive Advantage
        account.

        :return: a 2-tuple of Collections (primary Overdrive
            collection, Overdrive Advantage collection)
        """
        # First find the parent Collection.
        parent = _db.execute(
            select(Collection)
            .join(IntegrationConfiguration)
            .where(
                IntegrationConfiguration.protocol == OVERDRIVE_LABEL,
                IntegrationConfiguration.goal == Goals.LICENSE_GOAL,
                IntegrationConfiguration.settings_dict.contains(
                    {"external_account_id": self.parent_library_id}
                ),
            )
        ).scalar_one_or_none()
        if parent is None:
            # Without the parent's credentials we can't access the child.
            raise ValueError(
                "Cannot create a Collection whose parent does not already exist."
            )
        name = parent.name + " / " + self.name
        child = _db.execute(
            select(Collection)
            .join(IntegrationConfiguration)
            .where(
                Collection.parent_id == parent.id,
                IntegrationConfiguration.protocol == OVERDRIVE_LABEL,
                IntegrationConfiguration.goal == Goals.LICENSE_GOAL,
                IntegrationConfiguration.settings_dict.contains(
                    {"external_account_id" == self.library_id}
                ),
            )
        ).scalar_one_or_none()

        if child is None:
            # The child doesn't exist yet. Create it.
            child, _ = Collection.by_name_and_protocol(_db, name, OVERDRIVE_LABEL)
            child.parent = parent
            child_settings = OverdriveChildSettings.model_construct(
                external_account_id=self.library_id
            )
            integration_settings_update(
                OverdriveChildSettings, child.integration_configuration, child_settings
            )
        else:
            # Set or update the name of the collection to reflect the name of
            # the library, just in case that name has changed.
            child.integration_configuration.name = name

        return parent, child
