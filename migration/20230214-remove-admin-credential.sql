--
-- Remove admin credential field since we are not using any external admin auth service.
--

-- Remove the admin.credential
ALTER TABLE admins DROP COLUMN if exists credential;
