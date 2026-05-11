use role sysadmin;
use warehouse compute_wh;
use database AGS_GAME_AUDIENCE;
use schema AGS_GAME_AUDIENCE.RAW;

create or replace table AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS (
    RAW_LOG VARIANT
);

select * from pl_game_logs;

copy into AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
 from @ags_game_audience.raw.uni_kishore_pipeline
 file_format = ( format_name = ff_json_logs);

 select count(*) from pl_game_logs;

 copy into AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
 from @ags_game_audience.raw.uni_kishore_pipeline
 file_format = ( format_name = ff_json_logs)
 force = true;

  select count(*) from pl_game_logs;

  truncate table AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

 copy into AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
 from @ags_game_audience.raw.uni_kishore_pipeline
 file_format = ( format_name = ff_json_logs)
 force = false;

 select count(*) from pl_game_logs;

------------------- step 2 : Create Task to run copy into ---------------
create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    --warehouse = compute_wh
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    schedule = '10 minute'
as
    copy into AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
    from @ags_game_audience.raw.uni_kishore_pipeline
    file_format = ( format_name = ff_json_logs)
    force = false;

select count(*) from pl_game_logs;

select * from pl_game_logs;

create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS(
	DATETIME_ISO8601,
	USER_EVENT,
	USER_LOGIN,
	IP_ADDRESS,
	RAW_LOG
) as
SELECT
    raw_log:datetime_iso8601::TIMESTAMP_NTZ AS datetime_iso8601,
    raw_log:user_event::text AS user_event,
    raw_log:user_login::text AS user_login,
    raw_log:ip_address::text AS ip_address,
    raw_log
FROM AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
WHERE raw_log:ip_address::text IS NOT NULL;


------------------- step 4 : Create MERGE task using pipeline sources ---------------
create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    -- warehouse = COMPUTE_WH
    --schedule = '5 minute'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
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
    FROM AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
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

EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

USE ROLE ACCOUNTADMIN;
GRANT ALL ON TABLE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

--Turning on a task is done with a RESUME command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Turning OFF a task is done with a SUSPEND command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step 3 - number of rows in raw view (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay because not all IP addresses are available from the IPInfo share)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


SHOW STAGES LIKE 'UNI_KISHORE_PIPELINE' IN SCHEMA AGS_GAME_AUDIENCE.RAW;

USE ROLE ACCOUNTADMIN;
GRANT OWNERSHIP ON STAGE AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE TO ROLE SYSADMIN COPY CURRENT GRANTS;


list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    warehouse = COMPUTE_WH
    schedule = '10 minute'
AS
    COPY INTO AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
    FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
    file_format = (format_name = ff_json_logs)
    force = false;

ALTER TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES RESUME;

GRANT OWNERSHIP ON TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES TO ROLE SYSADMIN COPY CURRENT GRANTS;

USE ROLE ACCOUNTADMIN;
GRANT OWNERSHIP ON TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES TO ROLE SYSADMIN COPY CURRENT GRANTS;



USE ROLE SYSADMIN;

CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    schedule = '10 minute'
AS
    COPY INTO AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
    FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
    file_format = (format_name = ff_json_logs)
    force = false;

USE ROLE ACCOUNTADMIN;
GRANT OWNERSHIP ON TABLE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED TO ROLE SYSADMIN COPY CURRENT GRANTS;

