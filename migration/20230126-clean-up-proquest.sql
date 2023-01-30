--
-- Delete any collections that are still configured to use ProQuest.
--

create temporary table delete_ids (
  id integer not null
);

insert into delete_ids (id)
  select e.id from externalintegrations as e
  	where e.protocol = 'ProQuest'
  	  and e.goal = 'licenses';

-- Delete all collection library references that refer to a
-- collection that refers to ProQuest.
delete from collections_libraries as cl
  where cl.collection_id in
    (select c.id from collections as c
      where c.external_integration_id in
        (select d.id from delete_ids as d));

-- Delete all collections that refer to ProQuest.
delete from collections as c
  where c.external_integration_id in
    (select d.id from delete_ids as d);

-- Delete all external integration <-> library links that refer
-- to ProQuest.
delete from externalintegrations_libraries as el
  where el.externalintegration_id in
    (select d.id from delete_ids as d);

-- Delete all external integration links that refer to ProQuest.
delete from externalintegrationslinks as el
  where el.external_integration_id in
    (select d.id from delete_ids as d);

-- Delete all configuration settings that refer to ProQuest.
delete from configurationsettings as cs
  where cs.external_integration_id in
    (select d.id from delete_ids as d);

-- Delete the external integrations that refer to ProQuest.
delete from externalintegrations as e
  where e.id in
    (select d.id from delete_ids as d);

-- Not strictly necessary, but good practice to drop temporary tables.
drop table delete_ids;
