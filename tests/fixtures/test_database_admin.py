"""Tests for the admin fixture method in DatabaseTransactionFixture."""

from palace.manager.sqlalchemy.model.admin import Admin
from tests.fixtures.database import DatabaseTransactionFixture


class TestDatabaseTransactionFixtureAdmin:
    """Tests for the admin() method on DatabaseTransactionFixture."""

    def test_admin_with_email_and_password(self, db: DatabaseTransactionFixture):
        """Test creating an admin with email and password."""
        admin = db.admin(email="test@example.com", password="secure123")

        assert isinstance(admin, Admin)
        assert admin.email == "test@example.com"
        assert admin.password_hashed is not None
        # Password should be hashed with bcrypt
        assert admin.password_hashed.startswith("$2")
        # Verify the password works
        assert admin.has_password("secure123")
        assert not admin.has_password("wrongpassword")

    def test_admin_with_email_only(self, db: DatabaseTransactionFixture):
        """Test creating an admin with email but no password."""
        admin = db.admin(email="nopw@example.com")

        assert isinstance(admin, Admin)
        assert admin.email == "nopw@example.com"
        assert admin.password_hashed is None

    def test_admin_with_password_only(self, db: DatabaseTransactionFixture):
        """Test creating an admin with auto-generated email."""
        admin = db.admin(password="mypassword")

        assert isinstance(admin, Admin)
        assert admin.email.endswith("@example.com")
        assert admin.password_hashed is not None
        assert admin.has_password("mypassword")

    def test_admin_with_no_args(self, db: DatabaseTransactionFixture):
        """Test creating an admin with auto-generated email and no password."""
        admin = db.admin()

        assert isinstance(admin, Admin)
        assert admin.email.endswith("@example.com")
        assert admin.password_hashed is None

    def test_admin_idempotent(self, db: DatabaseTransactionFixture):
        """Test that calling admin() with same email returns same admin."""
        admin1 = db.admin(email="same@example.com", password="password1")
        admin2 = db.admin(email="same@example.com")

        # Should be the same admin object
        assert admin1.id == admin2.id
        assert admin1.email == admin2.email
        # Password should still be set from first call
        assert admin1.has_password("password1")
        assert admin2.has_password("password1")

    def test_admin_password_hashed_properly(self, db: DatabaseTransactionFixture):
        """Test that password is properly hashed using bcrypt."""
        password = "testpassword123"
        admin = db.admin(email="hash@example.com", password=password)

        # Password hash should be a bcrypt hash
        assert admin.password_hashed is not None
        assert isinstance(admin.password_hashed, str)
        # Bcrypt hashes start with $2a$ or $2b$ or $2y$
        assert admin.password_hashed.startswith("$2")
        # Hash should be different from the plain password
        assert admin.password_hashed != password
        # Should be able to authenticate with correct password
        assert admin.has_password(password)
        # Should NOT authenticate with wrong password
        assert not admin.has_password("wrong" + password)

    def test_admin_with_complex_password(self, db: DatabaseTransactionFixture):
        """Test that complex passwords with special characters work."""
        complex_password = "P@ssw0rd!#$%&*()_+-=[]{}|;:',.<>?/~`"
        admin = db.admin(email="complex@example.com", password=complex_password)

        assert admin.has_password(complex_password)
        assert not admin.has_password("simple")

    def test_admin_unique_emails(self, db: DatabaseTransactionFixture):
        """Test that auto-generated emails are unique."""
        admin1 = db.admin()
        admin2 = db.admin()
        admin3 = db.admin()

        # All should have different emails
        assert admin1.email != admin2.email
        assert admin1.email != admin3.email
        assert admin2.email != admin3.email
