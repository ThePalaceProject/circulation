from unittest.mock import MagicMock, PropertyMock

import pytest

from core.model import ConfigurationSetting
from core.model.hassessioncache import CacheTuple, HasSessionCache
from tests.fixtures.database import DatabaseTransactionFixture


class TestHasSessionCache:
    @pytest.fixture()
    def mock_db(self):
        def mock():
            mock_db = MagicMock()
            mock_db.info = {}
            mock_db.__contains__ = MagicMock(return_value=True)
            mock_db.deleted.__contains__ = MagicMock(return_value=False)
            return mock_db

        return mock

    @pytest.fixture()
    def mock(self):
        mock = MagicMock()
        mock.id = "the only ID"
        mock.cache_key = MagicMock(return_value="the only cache key")
        return mock

    @pytest.fixture()
    def mock_class(self):
        return HasSessionCache

    def test_cache_from_session(self, mock_db, mock_class):
        mock_db1 = mock_db()
        mock_db2 = mock_db()

        # Calling _cache_from_session with two different database
        # sessions should return two different caches
        cache1 = mock_class._cache_from_session(mock_db1)
        cache2 = mock_class._cache_from_session(mock_db2)
        assert cache1 is not cache2

        # Each one is a CacheTuple instance
        assert isinstance(cache1, CacheTuple)
        assert isinstance(cache2, CacheTuple)

    def test_cache_insert(self, mock_db, mock_class, mock):
        db = mock_db()
        cache = mock_class._cache_from_session(db)
        mock_class._cache_insert(mock, cache)

        # Items are inserted in both the key and id cache
        assert cache.id[mock.id] == mock
        assert cache.key[mock.cache_key()] == mock

    def test_by_id(self, mock_db, mock_class, mock):
        db = mock_db()

        # Look up item using by_id
        item = mock_class.by_id(db, mock.id)
        cache = mock_class._cache_from_session(db)

        # Make sure statistics are kept
        assert cache.stats.misses == 1
        assert cache.stats.hits == 0
        assert len(cache.id) == 1
        assert len(cache.key) == 1
        # Item was queried from DB
        db.query.assert_called_once()

        # Lookup item again
        cached_item = mock_class.by_id(db, item.id)

        # Stats are updated
        assert cache.stats.misses == 1
        assert cache.stats.hits == 1
        assert len(cache.id) == 1
        assert len(cache.key) == 1
        # Item comes from cache
        assert item == cached_item
        db.query.assert_called_once()

    def test_by_cache_key_miss_triggers_cache_miss_hook(
        self, mock_db, mock_class, mock
    ):
        db = mock_db()
        cache_miss_hook = MagicMock(side_effect=lambda: (mock, True))
        created, is_new = mock_class.by_cache_key(db, mock.cache_key(), cache_miss_hook)
        cache = mock_class._cache_from_session(db)

        # Item from create_func
        assert is_new is True
        assert created is mock
        cache_miss_hook.assert_called_once()

        # Make sure statistics are kept
        assert cache.stats.misses == 1
        assert cache.stats.hits == 0
        assert len(cache.id) == 1
        assert len(cache.key) == 1

        # Item from cache
        cached_item, cached_is_new = mock_class.by_cache_key(
            db, mock.cache_key(), cache_miss_hook
        )
        assert cached_is_new is False
        assert cached_item is created
        cache_miss_hook.assert_called_once()

        # Make sure statistics are kept
        assert cache.stats.misses == 1
        assert cache.stats.hits == 1
        assert len(cache.id) == 1
        assert len(cache.key) == 1

    def test_warm_cache(self, mock_db, mock_class):
        item1 = MagicMock()
        type(item1).id = PropertyMock(return_value=1)
        item1.cache_key = MagicMock(return_value="key1")
        item2 = MagicMock()
        type(item2).id = PropertyMock(return_value=2)
        item2.cache_key = MagicMock(return_value="key2")

        def populate():
            return [item1, item2]

        db = mock_db()
        # Throw exception if we query database
        db.query.side_effect = Exception

        # Warm cache with items from populate
        mock_class.cache_warm(db, populate)
        cache = mock_class._cache_from_session(db)

        assert cache.stats.misses == 0
        assert cache.stats.hits == 0
        assert len(cache.id) == 2
        assert len(cache.key) == 2

        # Get item1 by key and id
        item1_by_id = mock_class.by_id(db, 1)
        assert item1_by_id is item1
        item1_by_key, item1_new = mock_class.by_cache_key(db, "key1", db.query)
        assert item1_by_key is item1
        assert item1_new is False

        assert cache.stats.misses == 0
        assert cache.stats.hits == 2

        # Get item2 by key and id
        item2_by_id = mock_class.by_id(db, 2)
        assert item2_by_id is item2
        item2_by_key, item2_new = mock_class.by_cache_key(db, "key2", db.query)
        assert item2_by_key is item2
        assert item2_new is False

        assert cache.stats.misses == 0
        assert cache.stats.hits == 4

    def test_cache_remove(self, mock_db, mock_class):
        db = mock_db()

        # put items into cache
        item1, _ = mock_class.by_cache_key(db, "key1", lambda: (MagicMock(), False))
        item2, _ = mock_class.by_cache_key(db, "key2", lambda: (MagicMock(), False))
        cache = mock_class._cache_from_session(db)
        assert len(cache.id) == 2
        assert len(cache.key) == 2

        # Remove item1 from cache
        mock_class._cache_remove(item1, cache)

        # item2 is left in cache
        assert len(cache.id) == 1
        assert len(cache.key) == 1
        assert cache.id.get(item2.id) is item2

    def test_cache_remove_fail(self, mock_db, mock_class):
        db = mock_db()

        # put items into cache
        mock_class.by_cache_key(db, "key1", lambda: (MagicMock(), False))
        mock_class.by_cache_key(db, "key2", lambda: (MagicMock(), False))
        cache = mock_class._cache_from_session(db)
        assert len(cache.id) == 2
        assert len(cache.key) == 2

        # Try to remove an item that cannot be found in cache
        mock_class._cache_remove(MagicMock(), cache)

        # Cache clears itself to make sure we are not returning expired items
        assert len(cache.id) == 0
        assert len(cache.key) == 0

    def test_cache_remove_exception(self, mock_db, mock_class):
        db = mock_db()

        # put items into cache
        mock_class.by_cache_key(db, "key1", lambda: (MagicMock(), False))
        mock_class.by_cache_key(db, "key2", lambda: (MagicMock(), False))
        cache = mock_class._cache_from_session(db)
        assert len(cache.id) == 2
        assert len(cache.key) == 2

        # Try to remove an item that causes an exception
        mock_class._cache_remove(MagicMock(side_effect=KeyError), cache)

        # Cache clears itself to make sure we are not returning expired items
        assert len(cache.id) == 0
        assert len(cache.key) == 0


class TestHasFullTableCacheDatabase:
    def test_cached_values_are_properly_updated(self, db: DatabaseTransactionFixture):
        setting_key = "key"
        setting_old_value = "old value"
        setting_new_value = "new value"

        # First, let's create a ConfigurationSetting instance and save it in the database.
        setting = ConfigurationSetting(key=setting_key, _value=setting_old_value)
        db.session.add(setting)
        db.session.commit()

        # Let's save ConfigurationSetting's ID to find it later.
        setting_id = setting.id

        # Now let's fetch the configuration setting from the database and add it to the cache.
        db_setting1 = (
            db.session.query(ConfigurationSetting)
            .filter(ConfigurationSetting.key == setting_key)
            .one()
        )
        ConfigurationSetting.cache_warm(db.session, lambda: [db_setting1])

        # After, let's fetch it again and change its value.
        db_setting2 = (
            db.session.query(ConfigurationSetting)
            .filter(ConfigurationSetting.key == setting_key)
            .one()
        )
        db_setting2.value = setting_new_value

        # Now let's make sure that the cached value has also been updated.
        assert isinstance(setting_id, int)
        config_setting_by_id = ConfigurationSetting.by_id(db.session, setting_id)
        assert isinstance(config_setting_by_id, HasSessionCache)
        assert config_setting_by_id._value == setting_new_value

    def test_cached_value_deleted(self, db: DatabaseTransactionFixture):
        # Get setting
        setting = ConfigurationSetting.sitewide(db.session, "test")
        setting.value = "testing"

        # Delete setting
        db.session.delete(setting)

        # we should no longer be able to get setting from cache
        cached = ConfigurationSetting.by_id(db.session, setting.id)
        cache = ConfigurationSetting._cache_from_session(db.session)
        assert cached is None
        assert len(cache.id) == 0
        assert len(cache.key) == 0

    def test_cached_value_deleted_flushed(self, db: DatabaseTransactionFixture):
        # Get setting
        setting = ConfigurationSetting.sitewide(db.session, "test")
        setting.value = "testing"

        # Delete setting and flush
        db.session.delete(setting)
        db.session.flush()

        # we should no longer be able to get setting from cache
        cached = ConfigurationSetting.by_id(db.session, setting.id)
        cache = ConfigurationSetting._cache_from_session(db.session)
        assert cached is None
        assert len(cache.id) == 0
        assert len(cache.key) == 0

    def test_cached_value_deleted_committed(self, db: DatabaseTransactionFixture):
        # Get setting
        setting = ConfigurationSetting.sitewide(db.session, "test")
        setting.value = "testing"
        db.session.commit()

        # Delete setting and commit
        db.session.delete(setting)
        db.session.commit()

        # We should no longer be able to get setting from cache
        cached = ConfigurationSetting.by_id(db.session, setting.id)
        cache = ConfigurationSetting._cache_from_session(db.session)
        assert cached is None
        assert len(cache.id) == 0
        assert len(cache.key) == 0
