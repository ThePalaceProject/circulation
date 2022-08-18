DO $$
 BEGIN
  ALTER TABLE customlists ADD COLUMN IF NOT EXISTS auto_update_facets TEXT DEFAULT NULL;
 END;
$$;
