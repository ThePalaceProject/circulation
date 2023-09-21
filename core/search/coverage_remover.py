from core.model.coverage import WorkCoverageRecord


class RemovesSearchCoverage:
    """Mix-in class for a script that might remove all coverage records
    for the search engine.
    """

    def remove_search_coverage_records(self):
        """Delete all search coverage records from the database.

        :return: The number of records deleted.
        """
        wcr = WorkCoverageRecord
        clause = wcr.operation == wcr.UPDATE_SEARCH_INDEX_OPERATION
        count = self._db.query(wcr).filter(clause).count()

        # We want records to be updated in ascending order in order to avoid deadlocks.
        # To guarantee lock order, we explicitly acquire locks by using a subquery with FOR UPDATE (with_for_update).
        # Please refer for my details to this SO article:
        # https://stackoverflow.com/questions/44660368/postgres-update-with-order-by-how-to-do-it
        self._db.execute(
            wcr.__table__.delete().where(
                wcr.id.in_(
                    self._db.query(wcr.id)
                    .with_for_update()
                    .filter(clause)
                    .order_by(WorkCoverageRecord.id)
                )
            )
        )

        return count
