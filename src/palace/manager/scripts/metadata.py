from __future__ import annotations

from sqlalchemy.orm import Query

from palace.manager.scripts.base import Script
from palace.manager.sqlalchemy.model.contributor import Contribution
from palace.manager.sqlalchemy.model.edition import Edition


class MetadataCalculationScript(Script):
    """Force calculate_presentation() to be called on some set of Editions.

    This assumes that the metadata is in already in the database and
    will fall into place if we just call
    Edition.calculate_presentation() and Edition.calculate_work() and
    Work.calculate_presentation().

    Most of these will be data repair scripts that do not need to be run
    regularly.

    """

    name = "Metadata calculation script"

    def q(self) -> Query[Edition]:
        raise NotImplementedError()

    def run(self) -> None:
        q = self.q()
        self.log.info("Attempting to repair metadata for %d works" % q.count())

        success = 0
        failure = 0
        also_created_work = 0

        def checkpoint() -> None:
            self._db.commit()
            self.log.info(
                "%d successes, %d failures, %d new works.",
                success,
                failure,
                also_created_work,
            )

        i = 0
        for edition in q:
            edition.calculate_presentation()
            if edition.sort_author:
                success += 1
                license_pool = next(iter(edition.license_pools))
                if license_pool:
                    work, is_new = license_pool.calculate_work()
                    if work:
                        work.calculate_presentation()
                        if is_new:
                            also_created_work += 1
            else:
                failure += 1
            i += 1
            if not i % 1000:
                checkpoint()
        checkpoint()


class FillInAuthorScript(MetadataCalculationScript):
    """Fill in Edition.sort_author for Editions that have a list of
    Contributors, but no .sort_author.

    This is a data repair script that should not need to be run
    regularly.
    """

    name = "Fill in missing authors"

    def q(self) -> Query[Edition]:
        return (
            self._db.query(Edition)
            .join(Edition.contributions)
            .join(Contribution.contributor)
            .filter(Edition.sort_author == None)
        )
