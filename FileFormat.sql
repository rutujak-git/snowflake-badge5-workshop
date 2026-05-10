create file format AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS
    type = JSON
    strip_outer_array = true;

select $1
    from @uni_kishore/kickoff
    (file_format => ff_json_logs);

copy into AGS_GAME_AUDIENCE.RAW.GAME_LOGS
from @uni_kishore/kickoff
file_format = (format_name = ff_json_logs);

select
    raw_log:agent::text as agent,
    raw_log:user_event::text as user_event,
    *
    from game_logs;

select
    raw_log:agent::text as agent,
    raw_log:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601,
    raw_log:user_event::text as user_event,
    raw_log:user_login::text as user_login,
    *
    from game_logs;

create or replace view ags_game_audience.raw.logs
as
    select
    raw_log:agent::text as agent,
    raw_log:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601,
    raw_log:user_event::text as user_event,
    raw_log:user_login::text as user_login,
    *
    from game_logs;

select * from logs;

select current_timestamp();

alter session set timezone = 'UTC';
select current_timestamp();

alter session set timezone = 'Africa/Nairobi';
    select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';

select * from logs;

-- Or specifically check the updated_feed folder
LIST @uni_kishore/updated_feed;

-- View records pre-load using $1 notation
SELECT $1
FROM @uni_kishore/updated_feed
(FILE_FORMAT => ff_json_logs);

-- Check current GAME_LOGS structure
DESC TABLE GAME_LOGS;

-- Add IP_ADDRESS column to GAME_LOGS
ALTER TABLE GAME_LOGS 
ADD COLUMN ip_address VARCHAR;

-- Copy into from the NEW folder (updated_feed, not kickoff)
COPY INTO GAME_LOGS
FROM @uni_kishore/updated_feed
FILE_FORMAT = ff_json_logs
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

-- View all records including new ones
SELECT * FROM GAME_LOGS;

CREATE OR REPLACE VIEW ags_game_audience.raw.logs AS
SELECT
    raw_log:agent::text AS agent,
    raw_log:datetime_iso8601::TIMESTAMP_NTZ AS datetime_iso8601,
    raw_log:user_event::text AS user_event,
    raw_log:user_login::text AS user_login,
    raw_log:ip_address::text AS ip_address,
    * EXCLUDE (ip_address)
FROM game_logs;

select * from logs;

-- Check if ip_address exists in the raw JSON for new rows
SELECT 
    raw_log:ip_address::text AS ip_from_json,
    ip_address AS ip_from_column,
    raw_log
FROM game_logs
LIMIT 20;

-- How many rows total?
SELECT COUNT(*) FROM game_logs;

-- Check last 20 rows (new load)
SELECT 
    raw_log:ip_address::text AS ip_from_json,
    ip_address AS ip_from_column,
    raw_log
FROM game_logs
ORDER BY raw_log:datetime_iso8601::timestamp_ntz DESC
LIMIT 20;

SELECT * FROM logs
WHERE agent IS NULL;

--looking for non-empty IP_ADDRESS column
select 
RAW_LOG:ip_address::text as IP_ADDRESS
,*
from ags_game_audience.raw.LOGS
where RAW_LOG:ip_address::text is not null;

CREATE OR REPLACE VIEW ags_game_audience.raw.logs AS
SELECT
    raw_log:datetime_iso8601::TIMESTAMP_NTZ AS datetime_iso8601,
    raw_log:user_event::text AS user_event,
    raw_log:user_login::text AS user_login,
    ip_address,
    raw_log
FROM game_logs
WHERE ip_address IS NOT NULL;

CREATE OR REPLACE VIEW ags_game_audience.raw.logs AS
SELECT
    raw_log:datetime_iso8601::TIMESTAMP_NTZ AS datetime_iso8601,
    raw_log:user_event::text AS user_event,
    raw_log:user_login::text AS user_login,
    COALESCE(raw_log:ip_address::text, ip_address) AS ip_address,
    raw_log
FROM game_logs
WHERE raw_log IS NOT NULL;

SELECT * FROM logs;

SELECT * FROM logs
WHERE user_login ILIKE '%prajina%';

COPY INTO AGS_GAME_AUDIENCE.RAW.GAME_LOGS(raw_log)
FROM (SELECT $1 FROM @uni_kishore/updated_feed)
FILE_FORMAT = (FORMAT_NAME = ff_json_logs)
FORCE = TRUE;

-- Truncate and reload correctly
TRUNCATE TABLE AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

-- Load kickoff (no ip_address in JSON)
COPY INTO AGS_GAME_AUDIENCE.RAW.GAME_LOGS(raw_log)
FROM (SELECT $1 FROM @uni_kishore/kickoff)
FILE_FORMAT = (FORMAT_NAME = ff_json_logs);

-- Load updated_feed (has ip_address in JSON)
COPY INTO AGS_GAME_AUDIENCE.RAW.GAME_LOGS(raw_log)
FROM (SELECT $1 FROM @uni_kishore/updated_feed)
FILE_FORMAT = (FORMAT_NAME = ff_json_logs);

CREATE OR REPLACE VIEW AGS_GAME_AUDIENCE.RAW.LOGS AS
SELECT
    raw_log:datetime_iso8601::TIMESTAMP_NTZ AS datetime_iso8601,
    raw_log:user_event::text AS user_event,
    raw_log:user_login::text AS user_login,
    raw_log:ip_address::text AS ip_address,
    raw_log
FROM AGS_GAME_AUDIENCE.RAW.GAME_LOGS
WHERE raw_log:ip_address::text IS NOT NULL;


SELECT PARSE_IP('100.41.16.160','inet');

select parse_ip('107.217.231.17','inet'):host;

select parse_ip('107.217.231.17','inet'):family;

--Look up Kishore and Prajina's Time Zone in the IPInfo share using his headset's IP Address with the PARSE_IP function.
select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4 --Kishore's Headset's IP Address
BETWEEN start_ip_int AND end_ip_int;

--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;

--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;