# Snowflake Badge 5 Workshop — Data Engineering with Snowflake

This project contains the SQL and notebook files for the **Snowflake Badge 5: Data Engineering Workshop** (DNGW). It covers building an end-to-end data pipeline for game audience analytics.

## Project Overview

The workshop ingests raw game log events, enriches them with geolocation (IP-to-city via IPInfo), converts timestamps to local time zones, and loads the results into a curated analytics table — all orchestrated with Snowflake Tasks and Snowpipe.

## Database & Schema Layout

| Database | Schema | Purpose |
|---|---|---|
| `AGS_GAME_AUDIENCE` | `RAW` | Raw ingestion layer — stages, file formats, raw tables, views, tasks, and pipes |
| `AGS_GAME_AUDIENCE` | `ENHANCED` | Curated layer — enriched `LOGS_ENHANCED` table |
| `UTIL_DB` | `PUBLIC` | DORA grader function for badge validation |
| `IPINFO_GEOLOC` | `DEMO` | Shared IPInfo geolocation data (Marketplace) |

## Files

| File | Description |
|---|---|
| `GameLogsTable.sql` | Creates the `GAME_LOGS` raw table and lists stage files |
| `FileFormat.sql` | Creates the JSON file format, loads raw data, creates the `LOGS` view, builds the `TIME_OF_DAY_LU` lookup table, joins with IPInfo geolocation, and creates the `LOGS_ENHANCED` table |
| `insert.sql` | Populates `LOGS_ENHANCED` via truncate-and-reload, creates a backup clone, tests MERGE logic, and recovers from a bad merge |
| `lesson6.sql` | Creates the initial `LOAD_LOGS_ENHANCED` task (INSERT-based), then upgrades it to a MERGE task with deduplication testing |
| `lesson7.sql` | Builds the pipeline: `PL_GAME_LOGS` table, `GET_NEW_FILES` COPY task, `PL_LOGS` view, `LOAD_LOGS_ENHANCED` child MERGE task, Snowpipe setup, and task orchestration |
| `ProductionizingWork.sql` | Truncates and reloads `LOGS_ENHANCED`, creates backup clones, and refines the production MERGE |
| `Dashboards.ipynb` | Notebook with dashboard queries — distinct gamers by city, time-of-day distribution, and session length analysis |
| `DORASetup.sql` | DORA grader checks (DNGW01–DNGW07) for badge validation |

## Pipeline Architecture

```
@UNI_KISHORE stage (S3)
        │
        ▼
  GET_NEW_FILES task (COPY INTO PL_GAME_LOGS, every 10 min)
        │
        ▼
  PL_LOGS view (parse JSON, filter nulls)
        │
        ▼
  LOAD_LOGS_ENHANCED task (MERGE into LOGS_ENHANCED)
        │
        ├── JOIN IPINFO_GEOLOC.DEMO.LOCATION  (IP → city/region/country/timezone)
        └── JOIN TIME_OF_DAY_LU               (hour → time-of-day name)
```

## Key Objects

- **Stage:** `AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE`
- **File Format:** `AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS` (JSON, strip outer array)
- **Tasks:** `GET_NEW_FILES` → `LOAD_LOGS_ENHANCED` (parent-child)
- **Pipe:** `AGS_GAME_AUDIENCE.RAW.PIPE_GET_NEW_FILES`
- **Final Table:** `AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED`

## Badge Validation

Run the queries in `DORASetup.sql` to validate all workshop steps (DNGW01–DNGW07) using the `UTIL_DB.PUBLIC.GRADER` function.