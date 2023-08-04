from api.opds import LibraryLoanAndHoldAnnotator


def active_loans_and_holds(patron):
    if not patron:
        return dict(loans_by_work={}, holds_by_work={})

    active_loans_by_work = {}
    for loan in patron.loans:
        work = loan.work
        if work:
            active_loans_by_work[work] = loan

    # There might be multiple holds for the same work so we gather all of them and choose the best one.
    all_holds_by_work = {}
    for hold in patron.holds:
        work = hold.work
        if not work:
            continue

        if work not in all_holds_by_work:
            all_holds_by_work[work] = []

        all_holds_by_work[work].append(hold)

    active_holds_by_work = {}
    for work, list_of_holds in all_holds_by_work.items():
        active_holds_by_work[
            work
        ] = LibraryLoanAndHoldAnnotator.choose_best_hold_for_work(list_of_holds)

    return dict(loans_by_work=active_loans_by_work, holds_by_work=active_holds_by_work)
