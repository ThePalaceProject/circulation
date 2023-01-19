-- Explicity set the target_age of between 0 and 13 on lanes that are "Children" only and have not target age already
-- set. This update is necessary to complete this ticket:
-- https://www.notion.so/lyrasis/Adult-titles-showing-in-Children-and-Middle-Grades-lane-in-Palace-app-St-Mary-s-County-Library-f2914ecfd97c42a4a7554cf08f995d5e
update lanes set target_age = '[0,13)' where cast(audiences as text) = '{Children}' and target_age is null;
