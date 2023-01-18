-- Change the name of the "Easy Readers" lane to "Early Readers" in accordance with
-- https://www.notion.so/lyrasis/Rename-the-default-lane-Easy-Readers-to-Early-Readers-e2df6515cb644064b4cd2b65556f186d
UPDATE lanes SET display_name = 'Early Readers' WHERE display_name= 'Easy Readers';
