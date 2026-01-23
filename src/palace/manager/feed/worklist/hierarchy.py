from __future__ import annotations

from palace.manager.feed.worklist.base import WorkList


class HierarchyWorkList(WorkList):
    """A WorkList representing part of a hierarchical view of a a
    library's collection. (As opposed to a non-hierarchical view such
    as search results or "books by author X".)
    """

    def accessible_to(self, patron):
        """As a matter of library policy, is the given `Patron` allowed
        to access this `WorkList`?

        Most of the logic is inherited from `WorkList`, but there's also
        a restriction based on the site hierarchy.

        :param patron: A Patron
        :return: A boolean
        """

        # All the rules of WorkList apply.
        if not super().accessible_to(patron):
            return False

        if patron is None:
            return True

        root_lane = patron.root_lane
        if root_lane and not self.is_self_or_descendant(root_lane):
            # In addition, a HierarchyWorkList that's not in
            # scope of the patron's root lane is not accessible,
            # period. Even if all of the books in the WorkList are
            # age-appropriate, it's in a different part of the
            # navigational structure and navigating to it is not
            # allowed.
            return False

        return True
