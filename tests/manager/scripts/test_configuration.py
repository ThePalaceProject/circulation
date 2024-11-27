from __future__ import annotations

from io import StringIO

import pytest

from palace.manager.api.bibliotheca import BibliothecaAPI
from palace.manager.api.overdrive import OverdriveAPI
from palace.manager.scripts.configuration import (
    ConfigureCollectionScript,
    ConfigureLaneScript,
    ConfigureLibraryScript,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.lane import Lane
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import get_one
from tests.fixtures.database import DatabaseTransactionFixture


class TestConfigureLibraryScript:
    def test_bad_arguments(self, db: DatabaseTransactionFixture):
        script = ConfigureLibraryScript()
        library = db.library(
            name="Library 1",
            short_name="L1",
        )
        library.library_registry_shared_secret = "secret"
        db.session.commit()
        with pytest.raises(ValueError) as excinfo:
            script.do_run(db.session, [])
        assert "You must identify the library by its short name." in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            script.do_run(db.session, ["--short-name=foo"])
        assert "Could not locate library 'foo'" in str(excinfo.value)

    def test_create_library(self, db: DatabaseTransactionFixture):
        # There is no library.
        assert [] == db.session.query(Library).all()

        script = ConfigureLibraryScript()
        output = StringIO()
        script.do_run(
            db.session,
            [
                "--short-name=L1",
                "--name=Library 1",
                "--setting=customkey=value",
                "--setting=website=http://library.org",
                "--setting=help_email=support@library.org",
            ],
            output,
        )

        # Now there is one library.
        [library] = db.session.query(Library).all()
        assert "Library 1" == library.name
        assert "L1" == library.short_name
        assert "http://library.org" == library.settings.website
        assert "support@library.org" == library.settings.help_email
        assert "value" == library.settings_dict.get("customkey")
        expect_output = (
            "Configuration settings stored.\n" + "\n".join(library.explain()) + "\n"
        )
        assert expect_output == output.getvalue()

    def test_reconfigure_library(self, db: DatabaseTransactionFixture):
        # The library exists.
        library = db.library(
            name="Library 1",
            short_name="L1",
        )
        script = ConfigureLibraryScript()
        output = StringIO()

        # We're going to change one value and add a setting.
        script.do_run(
            db.session,
            [
                "--short-name=L1",
                "--name=Library 1 New Name",
                "--setting=customkey=value",
            ],
            output,
        )

        assert "Library 1 New Name" == library.name
        assert "value" == library.settings_dict.get("customkey")

        expect_output = (
            "Configuration settings stored.\n" + "\n".join(library.explain()) + "\n"
        )
        assert expect_output == output.getvalue()


class TestConfigureCollectionScript:
    def test_bad_arguments(self, db: DatabaseTransactionFixture):
        script = ConfigureCollectionScript()
        db.library(
            name="Library 1",
            short_name="L1",
        )
        db.session.commit()

        # Reference to a nonexistent collection without the information
        # necessary to create it.
        with pytest.raises(ValueError) as excinfo:
            script.do_run(db.session, ["--name=collection"])
        assert (
            'No collection called "collection". You can create it, but you must specify a protocol.'
            in str(excinfo.value)
        )

        # Incorrect format for the 'setting' argument.
        with pytest.raises(ValueError) as excinfo:
            script.do_run(
                db.session,
                ["--name=collection", "--protocol=Overdrive", "--setting=key"],
            )
        assert 'Incorrect format for setting: "key". Should be "key=value"' in str(
            excinfo.value
        )

        # Try to add the collection to a nonexistent library.
        with pytest.raises(ValueError) as excinfo:
            script.do_run(
                db.session,
                [
                    "--name=collection",
                    "--protocol=Overdrive",
                    "--library=nosuchlibrary",
                ],
            )
        assert 'No such library: "nosuchlibrary". I only know about: "L1"' in str(
            excinfo.value
        )

    def test_success(self, db: DatabaseTransactionFixture):
        script = ConfigureCollectionScript()
        l1 = db.library(name="Library 1", short_name="L1")
        l2 = db.library(name="Library 2", short_name="L2")
        l3 = db.library(name="Library 3", short_name="L3")

        # Create a collection, set all its attributes, set a custom
        # setting, and associate it with two libraries.
        output = StringIO()
        script.do_run(
            db.session,
            [
                "--name=New Collection",
                "--protocol=Overdrive",
                "--library=L2",
                "--library=L1",
                "--setting=library_id=1234",
                "--external-account-id=acctid",
                "--url=url",
                "--username=username",
                "--password=password",
            ],
            output,
        )

        db.session.commit()

        # The collection was created and configured properly.
        collection = get_one(db.session, Collection)
        assert collection is not None
        assert "New Collection" == collection.name
        assert "url" == collection.integration_configuration.settings_dict["url"]
        assert (
            "acctid"
            == collection.integration_configuration.settings_dict["external_account_id"]
        )
        assert (
            "username" == collection.integration_configuration.settings_dict["username"]
        )
        assert (
            "password" == collection.integration_configuration.settings_dict["password"]
        )

        # Two libraries now have access to the collection.
        assert [collection] == l1.associated_collections
        assert [collection] == l2.associated_collections
        assert [] == l3.associated_collections

        # One CollectionSetting was set on the collection, in addition
        # to url, username, and password.
        setting = collection.integration_configuration.settings_dict.get("library_id")
        assert "1234" == setting

        # The output explains the collection settings.
        expect = (
            "Configuration settings stored.\n" + "\n".join(collection.explain()) + "\n"
        )
        assert expect == output.getvalue()

    def test_reconfigure_collection(self, db: DatabaseTransactionFixture):
        # The collection exists.
        collection = db.collection(name="Collection 1", protocol=OverdriveAPI)
        script = ConfigureCollectionScript()
        output = StringIO()

        # We're going to change one value and add a new one.
        script.do_run(
            db.session,
            [
                "--name=Collection 1",
                "--url=foo",
                "--protocol=%s" % BibliothecaAPI.label(),
            ],
            output,
        )

        # The collection has been changed.
        db.session.refresh(collection.integration_configuration)
        assert "foo" == collection.integration_configuration.settings_dict.get("url")
        assert BibliothecaAPI.label() == collection.protocol

        expect = (
            "Configuration settings stored.\n" + "\n".join(collection.explain()) + "\n"
        )

        assert expect == output.getvalue()


class TestConfigureLaneScript:
    def test_bad_arguments(self, db: DatabaseTransactionFixture):
        script = ConfigureLaneScript()

        # No lane id but no library short name for creating it either.
        with pytest.raises(
            ValueError,
            match="Library short name and lane display name are required to create a new lane",
        ):
            script.do_run(db.session, [])

        # Try to create a lane for a nonexistent library.
        with pytest.raises(ValueError) as excinfo:
            script.do_run(db.session, ["--library-short-name=nosuchlibrary"])
        assert 'No such library: "nosuchlibrary".' in str(excinfo.value)

    def test_create_lane(self, db: DatabaseTransactionFixture):
        script = ConfigureLaneScript()
        parent = db.lane()

        # Create a lane and set its attributes.
        output = StringIO()
        script.do_run(
            db.session,
            [
                "--library-short-name=%s" % db.default_library().short_name,
                "--parent-id=%s" % parent.id,
                "--priority=3",
                "--display-name=NewLane",
            ],
            output,
        )

        # The lane was created and configured properly.
        lane = get_one(db.session, Lane, display_name="NewLane")
        assert lane is not None
        assert db.default_library() == lane.library
        assert parent == lane.parent
        assert 3 == lane.priority

        # The output explains the lane settings.
        expect = "Lane settings stored.\n" + "\n".join(lane.explain()) + "\n"
        assert expect == output.getvalue()

    def test_reconfigure_lane(self, db: DatabaseTransactionFixture):
        # The lane exists.
        lane = db.lane(display_name="Name")
        lane.priority = 3

        parent = db.lane()

        script = ConfigureLaneScript()
        output = StringIO()

        script.do_run(
            db.session,
            [
                "--id=%s" % lane.id,
                "--priority=1",
                "--parent-id=%s" % parent.id,
            ],
            output,
        )

        # The lane has been changed.
        assert 1 == lane.priority
        assert parent == lane.parent
        expect = "Lane settings stored.\n" + "\n".join(lane.explain()) + "\n"

        assert expect == output.getvalue()
