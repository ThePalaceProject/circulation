"""Empty coveragerecords before dropping it

This release removes the ``coverage_records`` relationships on Identifier, DataSource
and Collection, which is what finally stops the application reading this table. Those
relationships were also the only thing cleaning up children on a parent delete --
``coveragerecords`` has plain foreign keys with no ``ON DELETE`` clause -- so with them
gone any surviving row would make deleting its parent fail on a foreign key. The rows
are dead data, so we empty the table rather than add ``ON DELETE`` clauses to a table
that is dropped in the next release.

TRUNCATE rather than DELETE: the table can hold tens of millions of rows, and TRUNCATE
reclaims them in constant time without row-level WAL. Nothing references
``coveragerecords``, so no CASCADE is needed.

``lock_timeout`` bounds the wait for TRUNCATE's ACCESS EXCLUSIVE lock. That lock is not
guaranteed to be uncontended: N-1 servers still map the relationships this release
removes, so they read the table when deleting an Identifier, DataSource or Collection.
Without the timeout a TRUNCATE waiting on such a delete would queue ahead of every
later reader and stall them all indefinitely. With it, the statement gives up after 5s
and the migration fails -- nothing retries it, so the deploy has to be re-run, which is
the better failure: loud and bounded rather than a spreading stall.

The timeout is reset immediately afterwards because ``SET LOCAL`` lasts to the end of
the *transaction*, not the end of this revision, and ``alembic/env.py`` runs every
pending revision in a single ``context.begin_transaction()``. Without the reset, a
later revision in the same upgrade would inherit a 5s timeout it never asked for.

``equivalentscoveragerecords`` needs no such treatment: it has no ORM relationships
pointing at it, and its one foreign key already declares ``ON DELETE CASCADE``.

Revision ID: 58ebd34c5092
Revises: 5b1f4f3c7979
Create Date: 2026-09-16 20:32:09.926471+00:00

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "58ebd34c5092"
down_revision = "5b1f4f3c7979"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("TRUNCATE TABLE coveragerecords")
    # Scope the timeout to the statement above; see the module docstring.
    op.execute("SET LOCAL lock_timeout = DEFAULT")


def downgrade() -> None:
    # The rows are gone for good; emptying a dead table is not reversible. The
    # downgrade is a no-op so the migration can still be stepped back over.
    pass
