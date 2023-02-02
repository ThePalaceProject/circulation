from datetime import timedelta

import pytest

from api.admin.dashboard_stats import generate_statistics
from core.model import Admin, AdminRole, Collection, DataSource, create
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture
def admin(db: DatabaseTransactionFixture) -> Admin:
    admin, ignore = create(db.session, Admin, email="example@nypl.org")
    admin.password = "password"
    return admin


def test_stats_patrons(admin: Admin, db: DatabaseTransactionFixture):
    db_session = db.session

    admin.add_role(AdminRole.SYSTEM_ADMIN)

    default_library = db.library("Default Library", "default")

    # At first, there are no patrons in the database.
    response = generate_statistics(admin, db_session)
    library_data = response.get(default_library.short_name)
    total_data = response.get("total")
    for data in [library_data, total_data]:
        patron_data = data.get("patrons")
        assert 0 == patron_data.get("total")
        assert 0 == patron_data.get("with_active_loans")
        assert 0 == patron_data.get("with_active_loans_or_holds")
        assert 0 == patron_data.get("loans")
        assert 0 == patron_data.get("holds")

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

    response = generate_statistics(admin, db_session)
    library_data = response.get(default_library.short_name)
    total_data = response.get("total")
    for data in [library_data, total_data]:
        patron_data = data.get("patrons")
        assert 3 == patron_data.get("total")
        assert 1 == patron_data.get("with_active_loans")
        assert 2 == patron_data.get("with_active_loans_or_holds")
        assert 1 == patron_data.get("loans")
        assert 1 == patron_data.get("holds")

    # These patrons are in a different library..
    l2 = db.library()
    patron4 = db.patron(library=l2)
    pool.loan_to(patron4, end=utc_now() + timedelta(days=5))

    patron5 = db.patron(library=l2)
    pool.on_hold_to(patron5)

    response = generate_statistics(admin, db_session)
    library_data = response.get(default_library.short_name)
    total_data = response.get("total")
    assert 3 == library_data.get("patrons").get("total")
    assert 1 == library_data.get("patrons").get("with_active_loans")
    assert 2 == library_data.get("patrons").get("with_active_loans_or_holds")
    assert 1 == library_data.get("patrons").get("loans")
    assert 1 == library_data.get("patrons").get("holds")
    assert 5 == total_data.get("patrons").get("total")
    assert 2 == total_data.get("patrons").get("with_active_loans")
    assert 4 == total_data.get("patrons").get("with_active_loans_or_holds")
    assert 2 == total_data.get("patrons").get("loans")
    assert 2 == total_data.get("patrons").get("holds")

    # If the admin only has access to some libraries, only those will be counted
    # in the total stats.
    admin.remove_role(AdminRole.SYSTEM_ADMIN)
    admin.add_role(AdminRole.LIBRARIAN, default_library)

    response = generate_statistics(admin, db_session)
    library_data = response.get(default_library.short_name)
    total_data = response.get("total")
    assert 3 == library_data.get("patrons").get("total")
    assert 1 == library_data.get("patrons").get("with_active_loans")
    assert 2 == library_data.get("patrons").get("with_active_loans_or_holds")
    assert 1 == library_data.get("patrons").get("loans")
    assert 1 == library_data.get("patrons").get("holds")
    assert 3 == total_data.get("patrons").get("total")
    assert 1 == total_data.get("patrons").get("with_active_loans")
    assert 2 == total_data.get("patrons").get("with_active_loans_or_holds")
    assert 1 == total_data.get("patrons").get("loans")
    assert 1 == total_data.get("patrons").get("holds")


def test_stats_inventory(admin: Admin, db: DatabaseTransactionFixture):
    db_session = db.session

    admin.add_role(AdminRole.SYSTEM_ADMIN)

    default_library = db.library("Default Library", "default")

    # At first, there are no titles in the database.
    response = generate_statistics(admin, db_session)
    library_data = response.get(default_library.short_name)
    total_data = response.get("total")
    for data in [library_data, total_data]:
        inventory_data = data.get("inventory")
        assert 0 == inventory_data.get("titles")
        assert 0 == inventory_data.get("licenses")
        assert 0 == inventory_data.get("available_licenses")

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

    response = generate_statistics(admin, db_session)
    library_data = response.get(default_library.short_name)
    total_data = response.get("total")
    for data in [library_data, total_data]:
        inventory_data = data.get("inventory")
        assert 2 == inventory_data.get("titles")
        assert 15 == inventory_data.get("licenses")
        assert 4 == inventory_data.get("available_licenses")

    # This edition is in a different collection.
    c2 = db.collection()
    edition4, pool4 = db.edition(
        with_license_pool=True, with_open_access_download=False, collection=c2
    )
    pool4.licenses_owned = 2
    pool4.licenses_available = 2

    response = generate_statistics(admin, db_session)
    library_data = response.get(default_library.short_name)
    total_data = response.get("total")
    assert 2 == library_data.get("inventory").get("titles")
    assert 15 == library_data.get("inventory").get("licenses")
    assert 4 == library_data.get("inventory").get("available_licenses")
    assert 3 == total_data.get("inventory").get("titles")
    assert 17 == total_data.get("inventory").get("licenses")
    assert 6 == total_data.get("inventory").get("available_licenses")

    admin.remove_role(AdminRole.SYSTEM_ADMIN)
    admin.add_role(AdminRole.LIBRARIAN, default_library)

    # The admin can no longer see the other collection, so it's not
    # counted in the totals.
    response = generate_statistics(admin, db_session)
    library_data = response.get(default_library.short_name)
    total_data = response.get("total")
    for data in [library_data, total_data]:
        inventory_data = data.get("inventory")
        assert 2 == inventory_data.get("titles")
        assert 15 == inventory_data.get("licenses")
        assert 4 == inventory_data.get("available_licenses")


def test_stats_collections(admin: Admin, db: DatabaseTransactionFixture):
    db_session = db.session

    admin.add_role(AdminRole.SYSTEM_ADMIN)

    default_library = db.library("Default Library", "default")
    default_collection = db.collection(name="Default Collection")
    default_collection.libraries += [default_library]
    edition0, _ = db.edition(
        with_open_access_download=True,
        data_source_name=DataSource.GUTENBERG,
        collection=default_collection,
    )

    # At first, there is 1 open access title in the database,
    # created in CirculationControllerTest.setup.
    response = generate_statistics(admin, db_session)
    library_data = response.get(default_library.short_name)
    total_data = response.get("total")
    for data in [library_data, total_data]:
        collections_data = data.get("collections")
        assert 1 == len(collections_data)
        collection_data = collections_data.get(default_collection.name)
        assert 0 == collection_data.get("licensed_titles")
        assert 1 == collection_data.get("open_access_titles")
        assert 0 == collection_data.get("licenses")
        assert 0 == collection_data.get("available_licenses")

    c2 = db.collection()
    c3 = db.collection()
    c3.libraries += [default_library]

    edition1, pool1 = db.edition(
        with_license_pool=True,
        with_open_access_download=False,
        data_source_name=DataSource.OVERDRIVE,
        collection=c2,
    )
    pool1.open_access = False
    pool1.licenses_owned = 10
    pool1.licenses_available = 5

    edition2, pool2 = db.edition(
        with_license_pool=True,
        with_open_access_download=False,
        data_source_name=DataSource.OVERDRIVE,
        collection=c3,
    )
    pool2.open_access = False
    pool2.licenses_owned = 0
    pool2.licenses_available = 0

    edition3, pool3 = db.edition(
        with_license_pool=True,
        with_open_access_download=False,
        data_source_name=DataSource.BIBLIOTHECA,
    )
    pool3.open_access = False
    pool3.licenses_owned = 3
    pool3.licenses_available = 0

    edition4, pool4 = db.edition(
        with_license_pool=True,
        with_open_access_download=False,
        data_source_name=DataSource.AXIS_360,
        collection=c2,
    )
    pool4.open_access = False
    pool4.licenses_owned = 5
    pool4.licenses_available = 5

    response = generate_statistics(admin, db_session)
    library_data = response.get(default_library.short_name)
    total_data = response.get("total")
    library_collections_data = library_data.get("collections")
    total_collections_data = total_data.get("collections")
    assert 2 == len(library_collections_data)
    assert 3 == len(total_collections_data)
    for data in [library_collections_data, total_collections_data]:
        c1_data = data.get(default_collection.name)
        assert 1 == c1_data.get("licensed_titles")
        assert 1 == c1_data.get("open_access_titles")
        assert 3 == c1_data.get("licenses")
        assert 0 == c1_data.get("available_licenses")

        c3_data = data.get(c3.name)
        assert 0 == c3_data.get("licensed_titles")
        assert 0 == c3_data.get("open_access_titles")
        assert 0 == c3_data.get("licenses")
        assert 0 == c3_data.get("available_licenses")

    assert None == library_collections_data.get(c2.name)
    c2_data = total_collections_data.get(c2.name)
    assert 2 == c2_data.get("licensed_titles")
    assert 0 == c2_data.get("open_access_titles")
    assert 15 == c2_data.get("licenses")
    assert 10 == c2_data.get("available_licenses")

    admin.remove_role(AdminRole.SYSTEM_ADMIN)
    admin.add_role(AdminRole.LIBRARY_MANAGER, default_library)

    # c2 is no longer included in the totals since the admin's library does
    # not use it.
    response = generate_statistics(admin, db_session)
    library_data = response.get(default_library.short_name)
    total_data = response.get("total")
    for data in [library_data, total_data]:
        collections_data = data.get("collections")
        assert 2 == len(collections_data)
        assert None == collections_data.get(c2.name)

        c1_data = collections_data.get(default_collection.name)
        assert 1 == c1_data.get("licensed_titles")
        assert 1 == c1_data.get("open_access_titles")
        assert 3 == c1_data.get("licenses")
        assert 0 == c1_data.get("available_licenses")

        c3_data = collections_data.get(c3.name)
        assert 0 == c3_data.get("licensed_titles")
        assert 0 == c3_data.get("open_access_titles")
        assert 0 == c3_data.get("licenses")
        assert 0 == c3_data.get("available_licenses")


def test_stats_parent_collection_permissions(
    admin: Admin, db: DatabaseTransactionFixture
):
    """A parent collection may be dissociated from a library"""
    parent: Collection = db.collection()
    child: Collection = db.collection()
    child.parent = parent
    library = db.library()
    child.libraries.append(library)
    admin.add_role(AdminRole.LIBRARIAN, library)

    response = generate_statistics(admin, db.session)
    stats = response["total"]["collections"]

    # Child is in stats, but parent is not
    # No exceptions were thrown
    assert child.name in stats
    assert parent.name not in stats
