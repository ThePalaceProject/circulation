update externalintegrations set protocol = REPLACE(protocol, 'api.', 'palace.api.') where protocol like 'api.%';
update externalintegrations set protocol = REPLACE(protocol, 'core.', 'palace.core.') where protocol like 'core.%';
