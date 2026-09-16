"""Empty coveragerecords before dropping it

The ``coverage_records`` relationships on Identifier, DataSource and Collection are
removed in this release, which is what finally stops the application reading the
``coveragerecords`` table. Those relationships were also the only thing keeping parent
deletes working: ``coveragerecords`` has plain foreign keys to ``identifiers``,
``datasources`` and ``collections`` with no ``ON DELETE`` clause, so SQLAlchemy had to
load the children and cascade (or null their FK) by hand. With the relationships gone
nothing does that any more, and any surviving row would make deleting its parent fail
with a foreign-key violation.

The rows are dead data -- the CoverageProvider machinery that wrote them was retired a
release ago -- so we empty the table rather than add ``ON DELETE`` clauses to a table
that is dropped in the next release.

TRUNCATE rather than DELETE: the table can hold tens of millions of rows, and TRUNCATE
reclaims them in constant time without generating row-level WAL. It takes an ACCESS
EXCLUSIVE lock, which is uncontended here because no running code (current or N-1)
reads or writes this table. Nothing references ``coveragerecords``, so no CASCADE is
needed.

``equivalentscoveragerecords`` deliberately gets no such treatment: it has no ORM
relationships pointing at it, and its one foreign key already declares
``ON DELETE CASCADE``, so the database cleans it up on its own.

Revision ID: 58ebd34c5092
Revises: 912c566f3383
Create Date: 2026-09-16 20:32:09.926471+00:00

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "58ebd34c5092"
down_revision = "912c566f3383"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("TRUNCATE TABLE coveragerecords")


def downgrade() -> None:
    # The rows are gone for good; emptying a dead table is not reversible. The
    # downgrade is a no-op so the migration can still be stepped back over.
    pass
