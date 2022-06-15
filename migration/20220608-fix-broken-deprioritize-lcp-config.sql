-- The purpose of this migration is to fix any weird values that were inserted
-- into the configurationsettings table by the Admin API. Previous versions of
-- the code represented the LCP priority setting as an enum, and the Admin API
-- inserted the configuration labels rather than the enum values into the database.

UPDATE configurationsettings SET value = 'false' WHERE value = 'Do not de-prioritize';
UPDATE configurationsettings SET value = 'true' WHERE value = 'De-prioritize';
