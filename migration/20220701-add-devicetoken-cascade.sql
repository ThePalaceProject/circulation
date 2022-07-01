-- Add a CASCADE on the patron_id fkey
DO $$
BEGIN
    ALTER TABLE IF EXISTS devicetokens
        add constraint devicetokens_patron_fkey
        foreign key (patron_id)
        references patrons(id)
        on delete cascade;
EXCEPTION when DUPLICATE_OBJECT THEN
    RAISE NOTICE 'devicetokens_patron_fkey already exists, no need to create';
END;
$$;
