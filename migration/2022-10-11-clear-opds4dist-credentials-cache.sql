-- Clear cached OPDS for Distributors bearer tokens with no collection association.

DELETE FROM credentials
WHERE collection_id IS NULL
  AND type = 'OPDS For Distributors Bearer Token'
;
