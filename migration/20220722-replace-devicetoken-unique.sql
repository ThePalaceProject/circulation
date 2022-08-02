-- Drop the basic unique device_token index
-- And add a device_token and patron unique index
DO $$
BEGIN
    DROP INDEX IF EXISTS ix_devicetokens_device_token;
    CREATE UNIQUE INDEX IF NOT EXISTS ix_devicetokens_device_token_patron on devicetokens using btree (device_token ASC, patron_id ASC);
END;
$$;
