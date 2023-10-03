import copy
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.feed.annotator.circulation import LibraryAnnotator
from core.feed.types import FeedData, Link, WorkEntry
from core.model.configuration import ExternalIntegration
from core.model.constants import EditionConstants, LinkRelations
from core.model.patron import Hold, Patron


class LibraryLoanAndHoldAnnotator(LibraryAnnotator):
    @staticmethod
    def choose_best_hold_for_work(list_of_holds: List[Hold]) -> Hold:
        # We don't want holds that are connected to license pools without any licenses owned. Also, we want hold that
        # would result in the least wait time for the patron.

        best = list_of_holds[0]

        for hold in list_of_holds:
            # We don't want holds with LPs with 0 licenses owned.
            if hold.license_pool.licenses_owned == 0:
                continue

            # Our current hold's LP owns some licenses but maybe the best one wasn't changed yet.
            if best.license_pool.licenses_owned == 0:
                best = hold
                continue

            # Since these numbers are updated by different processes there might be situation where we don't have
            # all data filled out.
            hold_position = (
                hold.position or hold.license_pool.patrons_in_hold_queue or 0
            )
            best_position = (
                best.position or best.license_pool.patrons_in_hold_queue or 0
            )

            # Both the best hold and current hold own some licenses, try to figure out which one is better.
            if (
                hold_position / hold.license_pool.licenses_owned
                < best_position / best.license_pool.licenses_owned
            ):
                best = hold

        return best

    def drm_device_registration_feed_tags(self, patron: Patron) -> Dict[str, Any]:
        """Return tags that provide information on DRM device deregistration
        independent of any particular loan. These tags will go under
        the <feed> tag.

        This allows us to deregister an Adobe ID, in preparation for
        logout, even if there is no active loan that requires one.
        """
        tags = copy.deepcopy(self.adobe_id_tags(patron))
        attr = "scheme"
        for tag, value in tags.items():
            value.add_attributes(
                {attr: "http://librarysimplified.org/terms/drm/scheme/ACS"}
            )
        return tags

    @property
    def user_profile_management_protocol_link(self) -> Link:
        """Create a <link> tag that points to the circulation
        manager's User Profile Management Protocol endpoint
        for the current patron.
        """
        return Link(
            rel="http://librarysimplified.org/terms/rel/user-profile",
            href=self.url_for(
                "patron_profile",
                library_short_name=self.library.short_name,
                _external=True,
            ),
        )

    def annotate_feed(self, feed: FeedData) -> None:
        """Annotate the feed with top-level DRM device registration tags
        and a link to the User Profile Management Protocol endpoint.
        """
        super().annotate_feed(feed)
        if self.patron:
            tags = self.drm_device_registration_feed_tags(self.patron)
            link = self.user_profile_management_protocol_link
            if link.href is not None:
                feed.add_link(link.href, rel=link.rel)
            if "drm_licensor" in tags:
                feed.metadata.drm_licensor = tags["drm_licensor"]

    def annotate_work_entry(
        self, entry: WorkEntry, updated: Optional[datetime] = None
    ) -> None:
        super().annotate_work_entry(entry, updated=updated)
        if not entry.computed:
            return
        active_license_pool = entry.license_pool
        work = entry.work
        edition = work.presentation_edition
        identifier = edition.primary_identifier
        # Only OPDS for Distributors should get the time tracking link
        # And only if there is an active loan for the work
        if (
            edition.medium == EditionConstants.AUDIO_MEDIUM
            and active_license_pool
            and active_license_pool.collection.protocol
            == ExternalIntegration.OPDS_FOR_DISTRIBUTORS
            and work in self.active_loans_by_work
        ):
            entry.computed.other_links.append(
                Link(
                    rel=LinkRelations.TIME_TRACKING,
                    href=self.url_for(
                        "track_playtime_events",
                        identifier_type=identifier.type,
                        identifier=identifier.identifier,
                        library_short_name=self.library.short_name,
                        collection_id=active_license_pool.collection.id,
                        _external=True,
                    ),
                    type="application/json",
                )
            )
