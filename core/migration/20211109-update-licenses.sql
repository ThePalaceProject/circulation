DO $$
 BEGIN
  ALTER TABLE licenses RENAME COLUMN remaining_checkouts TO checkouts_left;
  ALTER TABLE licenses RENAME COLUMN concurrent_checkouts TO checkouts_available;

  BEGIN
   CREATE TYPE licensestatus AS ENUM ('preorder', 'available', 'unavailable');
  EXCEPTION
   WHEN duplicate_object THEN RAISE NOTICE 'type "licensestatus" already exists, not creating it.';
  END;

  ALTER TABLE licenses ADD COLUMN status licensestatus;
  UPDATE licenses SET status = 'available';
  ALTER TABLE licenses ADD COLUMN terms_concurrency INTEGER;
 END;
$$;
