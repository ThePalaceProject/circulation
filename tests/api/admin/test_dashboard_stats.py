from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from api.admin.dashboard_stats import generate_statistics
from api.admin.model.dashboard_statistics import InventoryStatistics, PatronStatistics
from core.model import Admin, AdminRole, DataSource, create
from core.util.datetime_helpers import utc_now

if TYPE_CHECKING:
    from core.model import Collection
    from tests.fixtures.database import DatabaseTransactionFixture


class AdminStatisticsSessionFixture:
    admin: Admin
    db: DatabaseTransactionFixture

    def __init__(self, admin: Admin, db: DatabaseTransactionFixture):
        self.admin = admin
        self.db = db

    def get_statistics(self):
        return generate_statistics(self.admin, self.db.session)


@pytest.fixture
def admin_statistics_session(
    db: DatabaseTransactionFixture,
) -> AdminStatisticsSessionFixture:
    admin, _ = create(db.session, Admin, email="example@nypl.org")
    admin.password = "password"
    return AdminStatisticsSessionFixture(admin, db)


def test_stats_patrons(admin_statistics_session: AdminStatisticsSessionFixture):
    session = admin_statistics_session
    admin = session.admin
    db = session.db

    # A `zeroed` PatronStatistics object is the same as one whose
    # properties are individually specified as zero.
    no_patrons = PatronStatistics.zeroed()
    assert (
        PatronStatistics(
            total=0, with_active_loan=0, with_active_loan_or_hold=0, loans=0, holds=0
        )
        == no_patrons
    )

    admin.add_role(AdminRole.SYSTEM_ADMIN)

    default_library = db.default_library()

    # At first, there are no patrons in the database.
    response = session.get_statistics()
    library_stats = response.libraries_by_key.get(default_library.short_name)
    library_patron_stats = library_stats.patron_statistics
    summary_patron_stats = response.patron_summary
    for patron_data in [library_patron_stats, summary_patron_stats]:
        assert no_patrons == patron_data
        assert 0 == patron_data.total
        assert 0 == patron_data.with_active_loan
        assert 0 == patron_data.with_active_loan_or_hold
        assert 0 == patron_data.loans
        assert 0 == patron_data.holds

    edition, pool = db.edition(with_license_pool=True, with_open_access_download=False)
    edition2, open_access_pool = db.edition(with_open_access_download=True)

    # patron1 has a loan.
    patron1 = db.patron()
    pool.loan_to(patron1, end=utc_now() + timedelta(days=5))

    # patron2 has a hold.
    patron2 = db.patron()
    pool.on_hold_to(patron2)

    # patron3 has an open access loan with no end date, but it doesn't count
    # because we don't know if it is still active.
    patron3 = db.patron()
    open_access_pool.loan_to(patron3)

    response = session.get_statistics()
    library_stats = response.libraries_by_key.get(default_library.short_name)
    library_patron_stats = library_stats.patron_statistics
    summary_patron_stats = response.patron_summary
    for patron_data in [library_patron_stats, summary_patron_stats]:
        assert 3 == patron_data.total
        assert 1 == patron_data.with_active_loan
        assert 2 == patron_data.with_active_loan_or_hold
        assert 1 == patron_data.loans
        assert 1 == patron_data.holds

    # These patrons are in a different library..
    l2 = db.library()
    patron4 = db.patron(library=l2)
    pool.loan_to(patron4, end=utc_now() + timedelta(days=5))

    patron5 = db.patron(library=l2)
    pool.on_hold_to(patron5)

    response = session.get_statistics()
    library_stats = response.libraries_by_key.get(default_library.short_name)
    library_patron_stats = library_stats.patron_statistics
    summary_patron_stats = response.patron_summary
    assert 3 == library_patron_stats.total
    assert 1 == library_patron_stats.with_active_loan
    assert 2 == library_patron_stats.with_active_loan_or_hold
    assert 1 == library_patron_stats.loans
    assert 1 == library_patron_stats.holds
    assert 5 == summary_patron_stats.total
    assert 2 == summary_patron_stats.with_active_loan
    assert 4 == summary_patron_stats.with_active_loan_or_hold
    assert 2 == summary_patron_stats.loans
    assert 2 == summary_patron_stats.holds

    # If the admin only has access to some libraries, only those will be counted
    # in the total stats.
    admin.remove_role(AdminRole.SYSTEM_ADMIN)
    admin.add_role(AdminRole.LIBRARIAN, default_library)

    response = session.get_statistics()
    library_stats = response.libraries_by_key.get(default_library.short_name)
    library_patron_stats = library_stats.patron_statistics
    summary_patron_stats = response.patron_summary
    assert 3 == library_patron_stats.total
    assert 1 == library_patron_stats.with_active_loan
    assert 2 == library_patron_stats.with_active_loan_or_hold
    assert 1 == library_patron_stats.loans
    assert 1 == library_patron_stats.holds
    assert 3 == summary_patron_stats.total
    assert 1 == summary_patron_stats.with_active_loan
    assert 2 == summary_patron_stats.with_active_loan_or_hold
    assert 1 == summary_patron_stats.loans
    assert 1 == summary_patron_stats.holds


def test_stats_inventory(admin_statistics_session: AdminStatisticsSessionFixture):
    session = admin_statistics_session
    admin = session.admin
    db = session.db

    admin.add_role(AdminRole.SYSTEM_ADMIN)

    default_library = db.default_library()

    # At first, there are no titles in the database.
    response = session.get_statistics()
    library_stats = response.libraries_by_key.get(default_library.short_name)
    library_inventory = library_stats.inventory_summary
    summary_inventory = response.inventory_summary
    for inventory_data in [library_inventory, summary_inventory]:
        assert 0 == inventory_data.titles
        assert 0 == inventory_data.available_titles
        assert 0 == inventory_data.self_hosted_titles
        assert 0 == inventory_data.open_access_titles
        assert 0 == inventory_data.licensed_titles
        assert 0 == inventory_data.unlimited_license_titles
        assert 0 == inventory_data.metered_license_titles
        assert 0 == inventory_data.metered_licenses_owned
        assert 0 == inventory_data.metered_licenses_available

    # This edition has no licenses owned and isn't counted in the inventory.
    edition1, pool1 = db.edition(
        with_license_pool=True, with_open_access_download=False
    )
    pool1.open_access = False
    pool1.licenses_owned = 0
    pool1.licenses_available = 0

    edition2, pool2 = db.edition(
        with_license_pool=True, with_open_access_download=False
    )
    pool2.open_access = False
    pool2.licenses_owned = 10
    pool2.licenses_available = 0

    edition3, pool3 = db.edition(
        with_license_pool=True, with_open_access_download=False
    )
    pool3.open_access = False
    pool3.licenses_owned = 5
    pool3.licenses_available = 4

    response = session.get_statistics()
    library_stats = response.libraries_by_key.get(default_library.short_name)
    library_inventory = library_stats.inventory_summary
    summary_inventory = response.inventory_summary
    for inventory_data in [library_inventory, summary_inventory]:
        assert 2 == inventory_data.titles
        assert 1 == inventory_data.available_titles
        assert 0 == inventory_data.self_hosted_titles
        assert 0 == inventory_data.open_access_titles
        assert 2 == inventory_data.licensed_titles
        assert 0 == inventory_data.unlimited_license_titles
        assert 2 == inventory_data.metered_license_titles
        assert 15 == inventory_data.metered_licenses_owned
        assert 4 == inventory_data.metered_licenses_available

    # This edition is in a different collection.
    c2 = db.collection()
    edition4, pool4 = db.edition(
        with_license_pool=True, with_open_access_download=False, collection=c2
    )
    pool4.licenses_owned = 2
    pool4.licenses_available = 2

    response = session.get_statistics()
    library_stats = response.libraries_by_key.get(default_library.short_name)
    library_inventory = library_stats.inventory_summary
    summary_inventory = response.inventory_summary
    assert 2 == library_inventory.titles
    assert 1 == library_inventory.available_titles
    assert 0 == library_inventory.self_hosted_titles
    assert 0 == library_inventory.open_access_titles
    assert 2 == library_inventory.licensed_titles
    assert 0 == library_inventory.unlimited_license_titles
    assert 2 == library_inventory.metered_license_titles
    assert 15 == library_inventory.metered_licenses_owned
    assert 4 == library_inventory.metered_licenses_available

    assert 3 == summary_inventory.titles
    assert 2 == summary_inventory.available_titles
    assert 0 == summary_inventory.self_hosted_titles
    assert 0 == summary_inventory.open_access_titles
    assert 3 == summary_inventory.licensed_titles
    assert 0 == summary_inventory.unlimited_license_titles
    assert 3 == summary_inventory.metered_license_titles
    assert 17 == summary_inventory.metered_licenses_owned
    assert 6 == summary_inventory.metered_licenses_available

    admin.remove_role(AdminRole.SYSTEM_ADMIN)
    admin.add_role(AdminRole.LIBRARIAN, default_library)

    # The admin can no longer see the other collection, so it's not
    # counted in the totals.
    response = session.get_statistics()
    library_stats = response.libraries_by_key.get(default_library.short_name)
    library_inventory = library_stats.inventory_summary
    summary_inventory = response.inventory_summary
    for inventory_data in [library_inventory, summary_inventory]:
        assert 2 == inventory_data.titles
        assert 1 == inventory_data.available_titles
        assert 0 == inventory_data.self_hosted_titles
        assert 0 == inventory_data.open_access_titles
        assert 2 == inventory_data.licensed_titles
        assert 0 == inventory_data.unlimited_license_titles
        assert 2 == inventory_data.titles
        assert 15 == inventory_data.metered_licenses_owned
        assert 4 == inventory_data.metered_licenses_available


def test_stats_collections(admin_statistics_session: AdminStatisticsSessionFixture):
    session = admin_statistics_session
    admin = session.admin
    db = session.db

    # A `zeroed` InventoryStatistics object is the same as one whose
    # properties are individually specified as zero.
    empty_inventory = InventoryStatistics.zeroed()
    assert empty_inventory == InventoryStatistics(
        titles=0,
        available_titles=0,
        self_hosted_titles=0,
        open_access_titles=0,
        licensed_titles=0,
        unlimited_license_titles=0,
        metered_license_titles=0,
        metered_licenses_owned=0,
        metered_licenses_available=0,
    )
    # We can update individual properties on the object while copying.
    new_metered_inventory = empty_inventory.copy(
        update={
            "titles": 2,
            "available_titles": 2,
            "licensed_titles": 2,
            "metered_license_titles": 2,
            "metered_licenses_owned": 4,
            "metered_licenses_available": 4,
        }
    )
    assert new_metered_inventory == InventoryStatistics(
        titles=2,
        available_titles=2,
        self_hosted_titles=0,
        open_access_titles=0,
        licensed_titles=2,
        unlimited_license_titles=0,
        metered_license_titles=2,
        metered_licenses_owned=4,
        metered_licenses_available=4,
    )

    admin.add_role(AdminRole.SYSTEM_ADMIN)

    # Initially, there is no inventory.
    response = session.get_statistics()
    assert response.inventory_summary == empty_inventory

    default_library = db.library("Default Library", "default")
    default_collection = db.collection(name="Default Collection")
    default_collection.libraries += [default_library]

    # default collection adds an OA title.
    _, _ = db.edition(
        with_open_access_download=True,
        data_source_name=DataSource.GUTENBERG,
        collection=default_collection,
    )

    # Now there is 1 open access title in the database,
    # created in CirculationControllerTest.setup.
    expected_library_inventory = empty_inventory.copy(
        update={
            "titles": 1,
            "available_titles": 1,
            "open_access_titles": 1,
        }
    )
    expected_summary_inventory = expected_library_inventory.copy()

    response = session.get_statistics()
    assert (
        expected_library_inventory
        == response.libraries_by_key.get(default_library.short_name).inventory_summary
    )
    assert expected_summary_inventory == response.inventory_summary

    c2 = db.collection()
    c3 = db.collection()
    c3.libraries += [default_library]

    # c2 adds a 5/10 metered license title.
    _, pool = db.edition(
        with_license_pool=True,
        with_open_access_download=False,
        data_source_name=DataSource.OVERDRIVE,
        collection=c2,
    )
    pool.open_access = False
    pool.licenses_owned = 10
    pool.licenses_available = 5

    # c3 does not add a title, since no licenses owned.
    _, pool = db.edition(
        with_license_pool=True,
        with_open_access_download=False,
        data_source_name=DataSource.OVERDRIVE,
        collection=c3,
    )
    pool.open_access = False
    pool.licenses_owned = 0
    pool.licenses_available = 0

    # default collection adds a 0/3 metered license title.
    _, pool = db.edition(
        with_license_pool=True,
        with_open_access_download=False,
        data_source_name=DataSource.BIBLIOTHECA,
        collection=default_collection,
    )
    pool.open_access = False
    pool.licenses_owned = 3
    pool.licenses_available = 0

    # c2 adds a 5/5 metered license title.
    _, pool = db.edition(
        with_license_pool=True,
        with_open_access_download=False,
        data_source_name=DataSource.AXIS_360,
        collection=c2,
    )
    pool.open_access = False
    pool.licenses_owned = 5
    pool.licenses_available = 5

    added_library_inventory = empty_inventory.copy(
        update={
            "titles": 1,
            "available_titles": 0,
            "licensed_titles": 1,
            "metered_license_titles": 1,
            "metered_licenses_owned": 3,
            "metered_licenses_available": 0,
        }
    )
    added_summary_inventory = empty_inventory.copy(
        update={
            "titles": 3,
            "available_titles": 2,
            "licensed_titles": 3,
            "metered_license_titles": 3,
            "metered_licenses_owned": 18,
            "metered_licenses_available": 10,
        }
    )
    expected_library_inventory += added_library_inventory
    expected_summary_inventory += added_summary_inventory

    response = session.get_statistics()
    library_stats_data = response.libraries_by_key.get(default_library.short_name)
    all_collections_by_id = {c.id: c for c in response.collections}
    library_collections_by_id = {
        id_: all_collections_by_id[id_] for id_ in library_stats_data.collection_ids
    }
    assert expected_library_inventory == library_stats_data.inventory_summary
    assert expected_summary_inventory == response.inventory_summary
    assert 2 == len(library_stats_data.collection_ids)
    assert 3 == len(response.collections)

    for collections in [library_collections_by_id, all_collections_by_id]:
        default_inventory = collections[default_collection.id].inventory
        c3_inventory = collections[c3.id].inventory
        assert 1 == default_inventory.licensed_titles
        assert 1 == default_inventory.open_access_titles
        assert 3 == default_inventory.metered_licenses_owned
        assert 0 == default_inventory.metered_licenses_available

        assert 0 == c3_inventory.licensed_titles
        assert 0 == c3_inventory.open_access_titles
        assert 0 == c3_inventory.metered_licenses_owned
        assert 0 == c3_inventory.metered_licenses_available

    # assert None == library_collections_data.get(c2.name)
    # c2_data = total_collections_data.get(c2.name)
    assert library_collections_by_id.get(c2.id) is None
    c2_inventory = all_collections_by_id[c2.id].inventory
    assert 2 == c2_inventory.licensed_titles
    assert 0 == c2_inventory.open_access_titles
    assert 15 == c2_inventory.metered_licenses_owned
    assert 10 == c2_inventory.metered_licenses_available

    admin.remove_role(AdminRole.SYSTEM_ADMIN)
    admin.add_role(AdminRole.LIBRARY_MANAGER, default_library)

    # c2 is no longer included in the totals since the admin user's
    # library is not associated with it.
    response = session.get_statistics()
    library_stats_data = response.libraries_by_key.get(default_library.short_name)
    all_collections_by_id = {c.id: c for c in response.collections}
    library_collections_by_id = {
        id: all_collections_by_id[id] for id in library_stats_data.collection_ids
    }
    for collections in [library_collections_by_id, all_collections_by_id]:
        assert 2 == len(collections)
        assert collections.get(c2.id) is None

        default_inventory = collections[default_collection.id].inventory
        assert 1 == default_inventory.licensed_titles
        assert 1 == default_inventory.open_access_titles
        assert 3 == default_inventory.metered_licenses_owned
        assert 0 == default_inventory.metered_licenses_available

        c3_inventory = collections[c3.id].inventory
        assert 0 == c3_inventory.licensed_titles
        assert 0 == c3_inventory.open_access_titles
        assert 0 == c3_inventory.metered_licenses_owned
        assert 0 == c3_inventory.metered_licenses_available


def test_stats_parent_collection_permissions(
    admin_statistics_session: AdminStatisticsSessionFixture,
):
    """A parent collection may be dissociated from a library"""

    session = admin_statistics_session
    admin = session.admin
    db = session.db

    parent: Collection = db.collection()
    child: Collection = db.collection()
    child.parent = parent
    library = db.library()
    child.libraries.append(library)
    admin.add_role(AdminRole.LIBRARIAN, library)

    response = session.get_statistics()
    collection_ids = [c.id for c in response.collections]

    # Child is in stats, but parent is not
    # No exceptions were thrown
    assert child.id in collection_ids
    assert parent.name not in collection_ids
