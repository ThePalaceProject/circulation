DO $$
 BEGIN
  BEGIN
   CREATE TYPE auto_update_status AS ENUM ('init', 'updated', 'populated');
  EXCEPTION
   WHEN duplicate_object THEN RAISE NOTICE 'type auto_update_status already exists.';
  END;
  ALTER TABLE customlists ADD COLUMN IF NOT EXISTS auto_update_status auto_update_status DEFAULT 'init';
 END;
$$;
