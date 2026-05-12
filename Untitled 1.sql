create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED            --- task creation
	warehouse=COMPUTE_WH
	schedule='5 minute'
	as INSERT INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
SELECT logs.ip_address 
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
, DAYNAME(game_event_ltz) as DOW_NAME
, TOD_NAME
from ags_game_audience.raw.LOGS logs
JOIN ipinfo_geoloc.demo.location loc 
ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
ON HOUR(game_event_ltz) = tod.hour;


create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	warehouse=COMPUTE_WH
	schedule='5 minute'
	as
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED AS e
USING (
    SELECT logs.ip_address
    , logs.user_login AS GAMER_NAME
    , logs.user_event AS GAME_EVENT_NAME
    , logs.datetime_iso8601 AS GAME_EVENT_UTC
    , city
    , region
    , country
    , timezone AS GAMER_LTZ_NAME
    , CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz
    , DAYNAME(game_event_ltz) AS DOW_NAME
    , TOD_NAME
    FROM AGS_GAME_AUDIENCE.RAW.LOGS logs
    JOIN IPINFO_GEOLOC.DEMO.LOCATION loc
        ON IPINFO_GEOLOC.PUBLIC.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND IPINFO_GEOLOC.PUBLIC.TO_INT(logs.ip_address)
        BETWEEN start_ip_int AND end_ip_int
    JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
) AS r
ON r.gamer_name = e.gamer_name
AND r.game_event_utc = e.game_event_utc
AND r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME)
VALUES (r.ip_address, r.gamer_name, r.game_event_name, r.game_event_utc, r.city, r.region, r.country, r.gamer_ltz_name, r.game_event_ltz, r.dow_name, r.tod_name);

execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;


select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


----------------------------------------------------------------------

--Testing cycle for MERGE. Use these commands to make sure the Merge works as expected

--Write down the number of records in your table 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the Merge a few times. No new rows should be added at this time 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if your row count changed 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Insert a test record into your Raw Table 
--You can change the user_event field each time to create "new" records 
--editing the ip_address or datetime_iso8601 can complicate things more than they need to 
--editing the user_login will make it harder to remove the fake records after you finish testing 
INSERT INTO ags_game_audience.raw.game_logs (RAW_LOG, IP_ADDRESS)
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}'),
'196.197.196.255';

--After inserting a new row, run the Merge again 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if any rows were added 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--When you are confident your merge is working, you can delete the raw records 
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

--You should also delete the fake rows from the enhanced table
delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

--Row count should be back to what it was in the beginning
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; 
