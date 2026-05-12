create task load_logs_enhanced        -- task creation
    warehouse = 'compute_wh'
    schedule = '5 minute'
  as
    select 'hello';

use role accountadmin;
grant execute task on account to role sysadmin;
use role sysadmin;

execute task ags_game_audience.raw.load_logs_enhanced;

show tasks in account;

describe task ags_game_audience.raw.load_logs_enhanced;

EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Run the task a few times to see changes in the RUN HISTORY
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

