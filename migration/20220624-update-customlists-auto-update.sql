DO $$
 BEGIN
  ALTER TABLE customlists ADD COLUMN IF NOT EXISTS auto_update_enabled BOOLEAN DEFAULT false;
  ALTER TABLE customlists ADD COLUMN IF NOT EXISTS auto_update_query TEXT DEFAULT NULL;
  ALTER TABLE customlists ADD COLUMN IF NOT EXISTS auto_update_last_update TIMESTAMP DEFAULT NULL;
 END;
$$;
