import logging
from collections import defaultdict

from sqlalchemy import or_
from sqlalchemy.orm.session import Session

from core.metadata_layer import ReplacementPolicy
from core.model import (
    Classification,
    CustomListEntry,
    Edition,
    Identifier,
    Subject,
    get_one_or_create,
)
from core.util.datetime_helpers import utc_now


class TitleFromExternalList:

    """This class helps you convert data from external lists into Simplified
    Edition and CustomListEntry objects.
    """

    def __init__(self, metadata, first_appearance, most_recent_appearance, annotation):
        self.log = logging.getLogger("Title from external list")
        self.metadata = metadata
        self.first_appearance = first_appearance or most_recent_appearance
        self.most_recent_appearance = most_recent_appearance or utc_now()
        self.annotation = annotation

    def to_custom_list_entry(self, custom_list, overwrite_old_data=False):
        """Turn this object into a CustomListEntry with associated Edition."""
        _db = Session.object_session(custom_list)
        edition = self.to_edition(_db, overwrite_old_data)

        list_entry, is_new = get_one_or_create(
            _db, CustomListEntry, edition=edition, customlist=custom_list
        )

        if (
            not list_entry.first_appearance
            or list_entry.first_appearance > self.first_appearance
        ):
            if list_entry.first_appearance:
                self.log.info(
                    "I thought %s first showed up at %s, but then I saw it earlier, at %s!",
                    self.metadata.title,
                    list_entry.first_appearance,
                    self.first_appearance,
                )
            list_entry.first_appearance = self.first_appearance

        if (
            not list_entry.most_recent_appearance
            or list_entry.most_recent_appearance < self.most_recent_appearance
        ):
            if list_entry.most_recent_appearance:
                self.log.info(
                    "I thought %s most recently showed up at %s, but then I saw it later, at %s!",
                    self.metadata.title,
                    list_entry.most_recent_appearance,
                    self.most_recent_appearance,
                )
            list_entry.most_recent_appearance = self.most_recent_appearance

        list_entry.annotation = self.annotation

        list_entry.set_work(self.metadata)
        return list_entry, is_new

    def to_edition(self, _db, overwrite_old_data=False):
        """Create or update an Edition object for this list item.

        We have two goals here:

        1. Make sure there is an Edition representing the list's view
        of the data.

        2. If at all possible, connect the Edition's primary
        identifier to other identifiers in the system, identifiers
        which may have associated LicensePools. This can happen in two
        ways:

        2a. The Edition's primary identifier, or other identifiers
        associated with the Edition, may be directly associated with
        LicensePools. This can happen if a book's list entry includes
        (e.g.) an Overdrive ID.

        2b. The Edition's permanent work ID may identify it as the
        same work as other Editions in the system. In that case this
        Edition's primary identifier may be associated with the other
        Editions' primary identifiers. (p=0.85)
        """
        self.log.info("Converting %s to an Edition object.", self.metadata.title)

        # Make sure the Metadata object's view of the book is present
        # as an Edition. This will also associate all its identifiers
        # with its primary identifier, and calculate the permanent work
        # ID if possible.
        try:
            edition, is_new = self.metadata.edition(_db)
        except ValueError as e:
            self.log.info("Ignoring %s, no corresponding edition.", self.metadata.title)
            return None
        if overwrite_old_data:
            policy = ReplacementPolicy.from_metadata_source(
                even_if_not_apparently_updated=True
            )
        else:
            policy = ReplacementPolicy.append_only(even_if_not_apparently_updated=True)
        self.metadata.apply(
            edition=edition,
            collection=None,
            replace=policy,
        )
        self.metadata.associate_with_identifiers_based_on_permanent_work_id(_db)
        return edition


class MembershipManager:
    """Manage the membership of a custom list based on some criteria."""

    def __init__(self, custom_list, log=None):
        self.log = log or logging.getLogger(
            "Membership manager for %s" % custom_list.name
        )
        self._db = Session.object_session(custom_list)
        self.custom_list = custom_list

    def update(self, update_time=None):
        update_time = update_time or utc_now()

        # Map each Edition currently in this list to the corresponding
        # CustomListEntry.
        current_membership = defaultdict(list)
        for entry in self.custom_list.entries:
            if not entry.edition:
                continue
            current_membership[entry.edition].append(entry)

        # Find the new membership of the list.
        for new_edition in self.new_membership:
            if new_edition in current_membership:
                # This entry was in the list before, and is still in
                # the list. Update its .most_recent_appearance.
                self.log.debug("Maintaining %s" % new_edition.title)
                entry_list = current_membership[new_edition]
                for entry in entry_list:
                    entry.most_recent_appearance = update_time
                del current_membership[new_edition]
            else:
                # This is a new list entry.
                self.log.debug("Adding %s" % new_edition.title)
                self.custom_list.add_entry(
                    work_or_edition=new_edition, first_appearance=update_time
                )

        # Anything still left in current_membership used to be in the
        # list but is no longer. Remove these entries from the list.
        for entry_list in list(current_membership.values()):
            for entry in entry_list:
                self.log.debug("Deleting %s" % entry.edition.title)
                self._db.delete(entry)

    @property
    def new_membership(self):
        """Iterate over the new membership of the list.

        :yield: a sequence of Edition objects
        """
        raise NotImplementedError()


class ClassificationBasedMembershipManager(MembershipManager):
    """Manage a custom list containing all Editions whose primary
    Identifier is classified under one of the given subject fragments.
    """

    def __init__(self, custom_list, subject_fragments):
        super().__init__(custom_list)
        self.subject_fragments = subject_fragments

    @property
    def new_membership(self):
        """Iterate over the new membership of the list.

        :yield: a sequence of Edition objects
        """
        subject_clause = None
        for i in self.subject_fragments:
            c = Subject.identifier.ilike("%" + i + "%")
            if subject_clause is None:
                subject_clause = c
            else:
                subject_clause = or_(subject_clause, c)
        qu = (
            self._db.query(Edition)
            .distinct(Edition.id)
            .join(Edition.primary_identifier)
            .join(Identifier.classifications)
            .join(Classification.subject)
        )
        qu = qu.filter(subject_clause)
        return qu
