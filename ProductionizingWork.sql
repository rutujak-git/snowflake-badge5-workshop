--first we dump all the rows out of the table
truncate table ags_game_audience.enhanced.LOGS_ENHANCED;

--then we put them all back in
INSERT INTO ags_game_audience.enhanced.LOGS_ENHANCED (
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
ON HOUR(game_event_ltz) = tod.hour);


--clone the table to save this version as a backup (BU stands for Back Up)
create table ags_game_audience.enhanced.LOGS_ENHANCED_BU 
clone ags_game_audience.enhanced.LOGS_ENHANCED;

merge into ags_game_audience.enhanced.logs_enhanced as e 
using ags_game_audience.raw.logs as r 
on r.user_login = e.gamer_name
and r.datetime_iso8601 = e.game_event_utc
and r.user_event = e.game_event_name
when matched then
update set ip_address = 'Hey I updated matching rows!';

select * from ags_game_audience.enhanced.logs_enhanced;

-- Step 1: Rename the messed up table
ALTER TABLE ags_game_audience.enhanced.LOGS_ENHANCED 
RENAME TO ags_game_audience.enhanced.LOGS_ENHANCED_MESSED_UP;

-- Step 2: Rename the clean BU clone to original name
ALTER TABLE ags_game_audience.enhanced.LOGS_ENHANCED_BU 
RENAME TO ags_game_audience.enhanced.LOGS_ENHANCED;

-- Step 3: Verify it's back to normal
SELECT * FROM ags_game_audience.enhanced.LOGS_ENHANCED LIMIT 10;

-- Step 4: Confirm IP addresses look real again
SELECT ip_address, COUNT(*) 
FROM ags_game_audience.enhanced.LOGS_ENHANCED
GROUP BY ip_address
LIMIT 10;

-- Should show real IP addresses like 108.169.166.55
-- NOT 'Hey I updated matching rows!'
SELECT DISTINCT ip_address 
FROM ags_game_audience.enhanced.LOGS_ENHANCED 
LIMIT 5;

merge into ags_game_audience.enhanced.LOGS_ENHANCED as e 
using (SELECT 
            logs.ip_address,
            logs.user_login as GAMER_NAME,
            logs.user_event as GAME_EVENT_NAME,
            logs.datetime_iso8601 as GAME_EVENT_UTC,
            city,
            region,
            country,
            timezone as GAMER_LTZ_NAME,
            CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
            DAYNAME(game_event_ltz) AS dow_name,
            tod.tod_name
        FROM AGS_GAME_AUDIENCE.RAW.LOGS logs
        JOIN IPINFO_GEOLOC.demo.location loc 
            ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
            AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
            BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
            ON HOUR(game_event_ltz) = tod.hour) as r
on r.gamer_name = e.gamer_name
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name;

