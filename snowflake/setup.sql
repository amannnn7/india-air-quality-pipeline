-- ============================================================================================
--  ONE-TIME SNOWFLAKE SETUP for the India Air Quality pipeline
--  Run this once in Snowsight (Projects -> Worksheets) as ACCOUNTADMIN.
--
--  What it creates, and why:
--    * AQI_WH             an X-Small warehouse that suspends after 60 s idle (you pay only while it runs)
--    * AQI_MONITOR        a resource monitor: hard spending cap of 5 credits per month
--    * AQI database       with schemas RAW (loaded data), REF (seeds) and ANALYTICS (dbt models),
--                         created by the pipeline role itself on first run
--    * AQI_PIPELINE_ROLE  least-privilege role that owns only this project's objects
--    * AQI_PIPELINE_USER  a SERVICE user that can log in ONLY with an RSA key (no password)
--
--  Before running: generate a key pair with `aqi snowflake-keygen` and paste the printed public key
--  below where it says <PASTE_PUBLIC_KEY_HERE>.
-- ============================================================================================

use role accountadmin;

-- 1. Compute: smallest size, auto-suspend after 60 seconds, auto-resume when a query arrives
create warehouse if not exists AQI_WH
    warehouse_size = 'XSMALL'
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true
    comment = 'India AQI pipeline: dbt builds and loads';

-- 2. Cost control: notify at 80%, suspend the warehouse at 100% of 5 credits/month
create resource monitor if not exists AQI_MONITOR
    with credit_quota = 5
    frequency = monthly
    start_timestamp = immediately
    triggers
        on 80 percent do notify
        on 100 percent do suspend;

alter warehouse AQI_WH set resource_monitor = AQI_MONITOR;

-- 3. Storage
create database if not exists AQI comment = 'India air-quality data (raw + modelled)';

-- 4. Role with only what the pipeline needs (role-based access control)
create role if not exists AQI_PIPELINE_ROLE comment = 'Loads RAW data and runs dbt for the AQI project';
grant usage on warehouse AQI_WH to role AQI_PIPELINE_ROLE;
grant usage, create schema on database AQI to role AQI_PIPELINE_ROLE;
-- let SYSADMIN (and you) see everything the pipeline creates
grant role AQI_PIPELINE_ROLE to role SYSADMIN;

-- 5. Service user: key-pair authentication only, no password, no interactive login
create user if not exists AQI_PIPELINE_USER
    type = service
    default_role = AQI_PIPELINE_ROLE
    default_warehouse = AQI_WH
    default_namespace = AQI.RAW
    rsa_public_key = '<PASTE_PUBLIC_KEY_HERE>'
    comment = 'Used by GitHub Actions / Airflow for the AQI pipeline';

grant role AQI_PIPELINE_ROLE to user AQI_PIPELINE_USER;

-- 6. Find your account identifier for the SNOWFLAKE_ACCOUNT secret (format: ORGNAME-ACCOUNTNAME)
select current_organization_name() || '-' || current_account_name() as snowflake_account;

-- ============================================================================================
--  Handy queries once the pipeline has run (use role AQI_PIPELINE_ROLE, warehouse AQI_WH)
-- ============================================================================================
-- select * from AQI.ANALYTICS.MART_CITY_AQI_SUMMARY order by pollution_rank_30d;
-- select * from AQI.ANALYTICS.FCT_CITY_DAILY_AQI where city_id = 'patna' order by observed_date desc;
-- list @AQI.RAW.AQI_LANDING;
-- select query_tag, count(*), sum(total_elapsed_time)/1000 as seconds
--   from table(AQI.information_schema.query_history()) group by 1;
