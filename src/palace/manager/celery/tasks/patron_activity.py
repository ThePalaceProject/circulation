# def sync_bookshelf(
#     self, patron: Patron, pin: str | None, force: bool = False
# ) -> tuple[list[Loan] | Query[Loan], list[Hold] | Query[Hold]]:
#     """Sync our internal model of a patron's bookshelf with any external
#     vendors that provide books to the patron's library.
#
#     :param patron: A Patron.
#     :param pin: The password authenticating the patron; used by some vendors
#        that perform a cross-check against the library ILS.
#     :param force: If this is True, the method will call out to external
#        vendors even if it looks like the system has up-to-date information
#        about the patron.
#     """
#     # Get our internal view of the patron's current state.
#     local_loans = self.local_loans(patron)
#     local_holds = self.local_holds(patron)
#
#     if patron and patron.last_loan_activity_sync and not force:
#         # Our local data is considered fresh, so we can return it
#         # without calling out to the vendor APIs.
#         return local_loans, local_holds
#
#     # Assuming everything goes well, we will set
#     # Patron.last_loan_activity_sync to this value -- the moment
#     # just before we started contacting the vendor APIs.
#     last_loan_activity_sync: datetime.datetime | None = utc_now()
#
#     # Update the external view of the patron's current state.
#     remote_loans, remote_holds, complete = self.patron_activity(patron, pin)
#     __transaction = self._db.begin_nested()
#
#     if not complete:
#         # We were not able to get a complete picture of the
#         # patron's loan activity. Until we are able to do that, we
#         # should never assume that our internal model of the
#         # patron's loans is good enough to cache.
#         last_loan_activity_sync = None
#
#     now = utc_now()
#     local_loans_by_identifier = {}
#     local_holds_by_identifier = {}
#     for l in local_loans:
#         if not l.license_pool:
#             self.log.error("Active loan with no license pool!")
#             continue
#         i = l.license_pool.identifier
#         if not i:
#             self.log.error(
#                 "Active loan on license pool %s, which has no identifier!",
#                 l.license_pool,
#             )
#             continue
#         key = (i.type, i.identifier)
#         local_loans_by_identifier[key] = l
#     for h in local_holds:
#         if not h.license_pool:
#             self.log.error("Active hold with no license pool!")
#             continue
#         i = h.license_pool.identifier
#         if not i:
#             self.log.error(
#                 "Active hold on license pool %r, which has no identifier!",
#                 h.license_pool,
#             )
#             continue
#         key = (i.type, i.identifier)
#         local_holds_by_identifier[key] = h
#
#     active_loans = []
#     active_holds = []
#     start: datetime.datetime | None
#     end: datetime.datetime | None
#     for loan in remote_loans:
#         # This is a remote loan. Find or create the corresponding
#         # local loan.
#         pool = loan.license_pool(self._db)
#         start = loan.start_date
#         end = loan.end_date
#         key = (loan.identifier_type, loan.identifier)
#         if key in local_loans_by_identifier:
#             # We already have the Loan object, we don't need to look
#             # it up again.
#             local_loan = local_loans_by_identifier[key]
#
#             # But maybe the remote's opinions as to the loan's
#             # start or end date have changed.
#             if start:
#                 local_loan.start = start
#             if end:
#                 local_loan.end = end
#         else:
#             local_loan, new = pool.loan_to(patron, start, end)
#
#         if loan.locked_to:
#             # The loan source is letting us know that the loan is
#             # locked to a specific delivery mechanism. Even if
#             # this is the first we've heard of this loan,
#             # it may have been created in another app or through
#             # a library-website integration.
#             loan.locked_to.apply(local_loan, autocommit=False)
#         active_loans.append(local_loan)
#
#         # Check the local loan off the list we're keeping so we
#         # don't delete it later.
#         key = (loan.identifier_type, loan.identifier)
#         if key in local_loans_by_identifier:
#             del local_loans_by_identifier[key]
#
#     for hold in remote_holds:
#         # This is a remote hold. Find or create the corresponding
#         # local hold.
#         pool = hold.license_pool(self._db)
#         start = hold.start_date
#         end = hold.end_date
#         position = hold.hold_position
#         key = (hold.identifier_type, hold.identifier)
#         if key in local_holds_by_identifier:
#             # We already have the Hold object, we don't need to look
#             # it up again.
#             local_hold = local_holds_by_identifier[key]
#
#             # But maybe the remote's opinions as to the hold's
#             # start or end date have changed.
#             local_hold.update(start, end, position)
#         else:
#             local_hold, new = pool.on_hold_to(patron, start, end, position)
#         active_holds.append(local_hold)
#
#         # Check the local hold off the list we're keeping so that
#         # we don't delete it later.
#         if key in local_holds_by_identifier:
#             del local_holds_by_identifier[key]
#
#     # We only want to delete local loans and holds if we were able to
#     # successfully sync with all the providers. If there was an error,
#     # the provider might still know about a loan or hold that we don't
#     # have in the remote lists.
#     if complete:
#         # Every loan remaining in loans_by_identifier is a hold that
#         # the provider doesn't know about. This usually means it's expired
#         # and we should get rid of it, but it's possible the patron is
#         # borrowing a book and syncing their bookshelf at the same time,
#         # and the local loan was created after we got the remote loans.
#         # If the loan's start date is less than a minute ago, we'll keep it.
#         for local_loan in list(local_loans_by_identifier.values()):
#             if (
#                 local_loan.license_pool.collection_id
#                 in self.collection_ids_for_sync
#             ):
#                 one_minute_ago = utc_now() - datetime.timedelta(minutes=1)
#                 if local_loan.start is None or local_loan.start < one_minute_ago:
#                     logging.info(
#                         "In sync_bookshelf for patron %s, deleting loan %s (patron %s)"
#                         % (
#                             patron.authorization_identifier,
#                             str(local_loan.id),
#                             local_loan.patron.authorization_identifier,
#                         )
#                     )
#                     self._db.delete(local_loan)
#                 else:
#                     logging.info(
#                         "In sync_bookshelf for patron %s, found local loan %s created in the past minute that wasn't in remote loans"
#                         % (patron.authorization_identifier, str(local_loan.id))
#                     )
#
#         # Every hold remaining in holds_by_identifier is a hold that
#         # the provider doesn't know about, which means it's expired
#         # and we should get rid of it.
#         for local_hold in list(local_holds_by_identifier.values()):
#             if (
#                 local_hold.license_pool.collection_id
#                 in self.collection_ids_for_sync
#             ):
#                 self._db.delete(local_hold)
#
#     # Now that we're in sync (or not), set last_loan_activity_sync
#     # to the conservative value obtained earlier.
#     if patron:
#         patron.last_loan_activity_sync = last_loan_activity_sync
#
#     __transaction.commit()
#     return active_loans, active_holds
