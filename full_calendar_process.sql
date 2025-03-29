-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 1: Setup network rules to allow access to data.gov.au
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE
OR REPLACE NETWORK RULE allow_data_gov_au MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('data.gov.au:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION apis_access_integration
  ALLOWED_NETWORK_RULES = (allow_data_gov_au) -- Specifies the network rules that this integration uses to control network access.
  ENABLED = true; -- Enables the external access integration.  Must be true for it to function.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 2: Setup LOAD_AU_HOLIDAYS(DATABASE_NAME, SCHEMA_NAME) stored procedure
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create procedure LOAD_AU_HOLIDAYS(DATABASE_NAME VARCHAR, SCHEMA_NAME VARCHAR)
returns String -- Returns a string indicating the success or failure of the procedure.
language python -- Specifies that the procedure is written in Python.
runtime_version=3.11 -- Specifies the Python runtime version to use.
packages=('pandas==2.2.3','requests==2.32.3','snowflake-snowpark-python==*') -- Specifies the Python packages required by the procedure.  Explicit versioning is recommended for stability.
handler='main' -- Specifies the entry point function in the Python code.
comment='Load Australian holidays from data.gov.au' -- Adds a comment to the stored procedure for documentation.
EXTERNAL_ACCESS_INTEGRATIONS = (apis_access_integration)  -- Links the stored procedure to the external access integration, granting it permission to access external resources.
as
'
import requests
import pandas as pd
from datetime import datetime
from snowflake.snowpark import Session

STAGE_NAME = "AU_PUBLIC_HOLIDAYS_STAGE" # Name of the Snowflake stage used for temporary data storage.
TABLE_NAME = "AU_PUBLIC_HOLIDAYS" # Name of the Snowflake table where the holiday data is loaded.
API_URL = "https://data.gov.au/data/api/action/datastore_search?resource_id=4d4d744b-50ed-45b9-ae77-760bc478ad75" # URL of the data.gov.au API endpoint for public holiday data.

def fetch_api_data(api_url):
    """
    Fetch data from the data.gov.au API endpoint
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()

        if data[''success'']:
            return data[''result''][''records'']
        else:
            raise Exception("API request was not successful")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        raise # Re-raise the exception to halt execution and report the error.

def create_stage(session, stage_name, database_name, schema_name):
    """
    Create an internal stage if it doesn\'t exist
    """
    try:
        fully_qualified_stage = f"{database_name}.{schema_name}.{stage_name}"
        session.sql(f"CREATE STAGE IF NOT EXISTS {fully_qualified_stage}").collect() # Executes SQL to create the stage if it doesn\'t exist.
    except Exception as e:
        print(f"Error creating stage: {e}")
        raise # Re-raise the exception.

def load_data_to_stage(session, df, stage_name, table_name, database_name, schema_name):
    """
    Load DataFrame to Snowflake stage and create table
    """
    try:
        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
            HOLIDAY_ID NUMBER,
            HOLIDAY_DATE DATE,
            HOLIDAY_NAME VARCHAR(255),
            INFORMATION VARCHAR(1000),
            MORE_INFORMATION VARCHAR(255),
            JURISDICTION VARCHAR(10),
            LOADED_AT TIMESTAMP_NTZ -- TIMESTAMP_NTZ: Timestamp without time zone.  Suitable for recording when the data was loaded.
        )
        """

        session.sql(create_table_sql).collect() # Executes SQL to create the table if it doesn\'t exist.

        # Process the date column
        df[''HOLIDAY_DATE''] = pd.to_datetime(df[''Date''], format=''%Y%m%d'').dt.date # Convert the "Date" column to a date format (%Y%m%d).

        # Rename columns to match table schema
        df = df.rename(columns={
            ''_id'': ''HOLIDAY_ID'',
            ''Holiday Name'': ''HOLIDAY_NAME'',
            ''Information'': ''INFORMATION'',
            ''More Information'': ''MORE_INFORMATION'',
            ''Jurisdiction'': ''JURISDICTION''
        })

        # Add load timestamp
        df[''LOADED_AT''] = datetime.now() # Adds a column with the current timestamp to track when the data was loaded.

        # Select and reorder columns to match table schema
        df = df[[
            ''HOLIDAY_ID'',
            ''HOLIDAY_DATE'',
            ''HOLIDAY_NAME'',
            ''INFORMATION'',
            ''MORE_INFORMATION'',
            ''JURISDICTION'',
            ''LOADED_AT''
        ]]

        # Convert pandas DataFrame to Snowpark DataFrame
        snowdf = session.create_dataframe(df) # Converts the Pandas DataFrame to a Snowpark DataFrame for efficient interaction with Snowflake.

        # Write DataFrame to Snowflake table
        snowdf.write \\
            .mode("append") \\
            .save_as_table(f"{database_name}.{schema_name}.{table_name}")  # Append to the table if it exists.  Use "overwrite" to replace existing data. Saves the Snowpark DataFrame to the specified Snowflake table.

        return f"Successfully loaded {len(df)} rows into {database_name}.{schema_name}.{table_name}"

    except Exception as e:
        error_msg = f"Error loading data to stage: {e}"
        print(error_msg)
        return error_msg

def main(session, DATABASE_NAME, SCHEMA_NAME):
    try:
        # Fetch data from API
        data = fetch_api_data(API_URL)

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Create stage
        create_stage(session, STAGE_NAME, DATABASE_NAME, SCHEMA_NAME)

        # Load data to stage and create table
        return load_data_to_stage(session, df, STAGE_NAME, TABLE_NAME, DATABASE_NAME, SCHEMA_NAME)

    except Exception as e:
        error_msg = f"Error in main process: {e}"
        print(error_msg)
        return error_msg
'
;

-- LOAD PROCEDURE EXAMPLE
-- Example of how to call the stored procedure.  Replace with your database and schema names.
-- call load_au_holidays('DBT_SPG_DW','STAGING');

-- DROP PROCEDURE EXAMPLE
-- Example of how to drop the stored procedure.  Use with caution!
-- drop procedure if exists load_au_holidays(VARCHAR, VARCHAR);

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 3: Call LOAD_AU_HOLIDAYS(DATABASE, SCHEMA)
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
call load_au_holidays('DBT_SPG_DW','STAGING');

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 4: Create a view to store the public holidays and allow modification for business requirements
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SPG_DAP01.PBI.AU_PUBLIC_HOLIDAYS_VW AS
SELECT
    HOLIDAY_DATE as date,
    HOLIDAY_NAME as holiday_name,
    JURISDICTION as state
FROM SPG_DAP01.PBI.AU_PUBLIC_HOLIDAYS;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 5: Establish local timezones for the business calendar
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ALTER SESSION SET TIMEZONE = 'Australia/Adelaide';

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- STEP 6: Create the stored procedure to build the business calendar
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- TODO: make the date to end a variable to pass through
-- TODO: Qualify the F445 calendar components

CREATE OR REPLACE PROCEDURE SPG_DAP01.PBI.SP_BUILD_BUSINESS_CALENDAR()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    status_message VARCHAR DEFAULT 'SUCCESS';
    current_run_ts TIMESTAMP_LTZ(9) := CURRENT_TIMESTAMP();
BEGIN
    CREATE OR REPLACE TABLE SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE
    AS
    WITH date_generator AS (
        SELECT '2015-01-01'::DATE AS calendar_date
        UNION ALL
        SELECT DATEADD(DAY, 1, calendar_date)
        FROM date_generator
        WHERE calendar_date < '2035-12-31'
    ),
    -- Modified to include holiday state information
    date_generator_with_holidays AS (
        SELECT
            dg.calendar_date,
            h.DATE IS NOT NULL AS is_holiday_flag,
            h.state AS holiday_state  -- Include the state information
        FROM date_generator dg
        LEFT JOIN SPG_DAP01.PBI.AU_PUBLIC_HOLIDAYS_VW h ON dg.calendar_date = h.DATE
    ),
    base_calendar AS (
        SELECT
            dgh.calendar_date                                      AS date,
            TO_NUMBER(TO_CHAR(dgh.calendar_date, 'YYYYMMDD'))     AS date_key,
            YEAR(dgh.calendar_date)                                AS year_num,
            'CY' || MOD(YEAR(dgh.calendar_date), 100)::STRING     AS year_desc,
            QUARTER(dgh.calendar_date)                             AS quarter_num,
            'Q' || QUARTER(dgh.calendar_date)                      AS quarter_desc,
            YEAR(dgh.calendar_date) * 10 + QUARTER(dgh.calendar_date) AS year_quarter_key,
            MONTH(dgh.calendar_date)                               AS month_num,
            MONTHNAME(dgh.calendar_date)                           AS month_short_name,
            CASE MONTH(dgh.calendar_date)
                WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
                WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
                WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
                WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
            END                                                    AS month_long_name,
            YEAR(dgh.calendar_date) * 100 + MONTH(dgh.calendar_date) AS year_month_key,
            CONCAT(MONTHNAME(dgh.calendar_date), ' ', YEAR(dgh.calendar_date)) AS month_year_desc,
            WEEKOFYEAR(dgh.calendar_date)                          AS week_of_year_num,
            YEAROFWEEK(dgh.calendar_date)                          AS year_of_week_num,
            CONCAT(YEAROFWEEK(dgh.calendar_date), '-W', LPAD(WEEKOFYEAR(dgh.calendar_date), 2, '0')) AS year_week_desc,
            WEEKISO(dgh.calendar_date)                             AS iso_week_num,
            YEAROFWEEKISO(dgh.calendar_date)                       AS iso_year_of_week_num,
            CONCAT(YEAROFWEEKISO(dgh.calendar_date), '-W', LPAD(WEEKISO(dgh.calendar_date), 2, '0')) AS iso_year_week_desc,
            DAY(dgh.calendar_date)                                 AS day_of_month_num,
            DAYOFWEEK(dgh.calendar_date)                           AS day_of_week_num, -- Sunday = 0
            DAYOFWEEKISO(dgh.calendar_date)                        AS iso_day_of_week_num, -- Monday = 1
            DAYOFYEAR(dgh.calendar_date)                           AS day_of_year_num,
            DAYNAME(dgh.calendar_date)                             AS day_short_name,
            CASE DAYOFWEEK(dgh.calendar_date)
                WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END                                                    AS day_long_name,
            TO_CHAR(dgh.calendar_date, 'DD Mon YYYY')              AS date_full_desc,
            TO_CHAR(dgh.calendar_date, 'DD/MM/YYYY')               AS date_formatted,
            DATE_TRUNC('MONTH', dgh.calendar_date)                 AS month_start_date,
            LAST_DAY(dgh.calendar_date)                            AS month_end_date,
            DATE_TRUNC('QUARTER', dgh.calendar_date)               AS quarter_start_date,
            LAST_DAY(dgh.calendar_date, QUARTER)                   AS quarter_end_date,
            DATE_TRUNC('YEAR', dgh.calendar_date)                  AS year_start_date,
            LAST_DAY(dgh.calendar_date, YEAR)                      AS year_end_date,
            DATE_TRUNC('WEEK', dgh.calendar_date)                  AS week_start_date,
            LAST_DAY(dgh.calendar_date, WEEK)                      AS week_end_date,
            DATEDIFF(DAY, DATE_TRUNC('MONTH', dgh.calendar_date), dgh.calendar_date) + 1 AS day_of_month_count,
            DAY(LAST_DAY(dgh.calendar_date))                       AS days_in_month_count,
            DATEDIFF(DAY, DATE_TRUNC('QUARTER', dgh.calendar_date), dgh.calendar_date) + 1 AS day_of_quarter_count,
            DATEDIFF(DAY, DATE_TRUNC('QUARTER', dgh.calendar_date), LAST_DAY(dgh.calendar_date, QUARTER)) + 1 AS days_in_quarter_count,
            CEIL(DAY(dgh.calendar_date) / 7.0)                     AS week_of_month_num,
            CEIL((DATEDIFF(DAY, DATE_TRUNC('QUARTER', dgh.calendar_date), dgh.calendar_date) + 1) / 7.0) AS week_of_quarter_num,
            CASE WHEN DAYOFWEEKISO(dgh.calendar_date) IN (6, 7) THEN 0 ELSE 1 END AS is_weekday, -- ISO Weekday 1-5
            CASE WHEN DAYOFWEEKISO(dgh.calendar_date) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS weekday_indicator, -- ISO Weekend 6,7
            CASE WHEN dgh.is_holiday_flag THEN 1 ELSE 0 END        AS is_holiday,
            CASE WHEN dgh.is_holiday_flag THEN 'Holiday' ELSE 'Non-Holiday' END AS holiday_indicator,
            dgh.holiday_state,                                     -- Include the holiday state
            DATEADD(YEAR, -1, dgh.calendar_date)                   AS same_date_last_year
        FROM date_generator_with_holidays dgh
    ),
    /* F445 Calendar - Start is the first Monday of July */
    f445_year_markers AS (
        SELECT
            date, year_num, month_num, day_of_month_num, iso_day_of_week_num, -- Use ISO day
            -- F445 year starts on the first Monday in July
            CASE WHEN month_num = 7 AND iso_day_of_week_num = 1 AND day_of_month_num <= 7
                 THEN 1 ELSE 0
            END AS is_f445_soy_marker -- Corrected Logic
        FROM base_calendar
    ),
    f445_years AS (
        SELECT
            date AS f445_soy_date,
            year_num AS marker_year,
            LAST_VALUE(CASE WHEN is_f445_soy_marker = 1 THEN date ELSE NULL END IGNORE NULLS) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as f445_start_of_year
        FROM f445_year_markers
    ),
    f445_years_processed AS (
        SELECT DISTINCT
            f445_start_of_year,
            COALESCE(
                DATEADD(DAY, -1, LEAD(f445_start_of_year) OVER (ORDER BY f445_start_of_year)),
                (SELECT MAX(DATEADD(DAY, 363, f445_start_of_year)) FROM f445_years fy_inner WHERE fy_inner.f445_start_of_year = f445_years.f445_start_of_year)
            ) AS f445_end_of_year,
            YEAR(DATEADD(DAY, 363, f445_start_of_year)) AS f445_year_num,
            'F' || MOD(YEAR(DATEADD(DAY, 363, f445_start_of_year)), 100)::STRING AS f445_year_desc
        FROM f445_years
        WHERE f445_start_of_year IS NOT NULL
    ),
    /* AU Fiscal Calendar */
    au_fiscal_base AS (
        SELECT date, year_num, month_num,
            CASE WHEN month_num >= 7 THEN DATE_FROM_PARTS(year_num, 7, 1) ELSE DATE_FROM_PARTS(year_num - 1, 7, 1) END AS au_fiscal_start_date_for_year,
            DATEADD(YEAR, 1, au_fiscal_start_date_for_year) - 1 AS au_fiscal_end_date_for_year
        FROM base_calendar
    ),
    au_fiscal_years AS (
       SELECT
            afb.date AS au_fiscal_date,
            afb.au_fiscal_start_date_for_year, afb.au_fiscal_end_date_for_year,
            YEAR(afb.au_fiscal_start_date_for_year) + 1 AS au_fiscal_year_num,
            'FY' || MOD(YEAR(afb.au_fiscal_start_date_for_year) + 1, 100)::STRING AS au_fiscal_year_desc
       FROM au_fiscal_base afb
    ),
    /* Combine Base, F445, AU Fiscal */
    combined_calendar AS (
         SELECT
            b.*,
            fy.f445_start_of_year, fy.f445_end_of_year, fy.f445_year_num, fy.f445_year_desc,
            afy.au_fiscal_start_date_for_year, afy.au_fiscal_end_date_for_year, afy.au_fiscal_year_num, afy.au_fiscal_year_desc,
            CASE WHEN b.date >= fy.f445_start_of_year THEN FLOOR(DATEDIFF(DAY, fy.f445_start_of_year, b.date) / 7) + 1 ELSE NULL END AS f445_week_num,
            DATEDIFF(QUARTER, afy.au_fiscal_start_date_for_year, b.date) + 1 AS au_fiscal_quarter_num,
            afy.au_fiscal_year_num * 10 + (DATEDIFF(QUARTER, afy.au_fiscal_start_date_for_year, b.date) + 1) AS au_fiscal_quarter_year_key,
            'QTR ' || (DATEDIFF(QUARTER, afy.au_fiscal_start_date_for_year, b.date) + 1)::VARCHAR AS au_fiscal_quarter_desc,
            DATEADD(QUARTER, DATEDIFF(QUARTER, afy.au_fiscal_start_date_for_year, b.date), afy.au_fiscal_start_date_for_year) AS au_fiscal_quarter_start_date,
            DATEADD(DAY, -1, DATEADD(QUARTER, 1, au_fiscal_quarter_start_date)) AS au_fiscal_quarter_end_date,
            MOD(DATEDIFF(MONTH, afy.au_fiscal_start_date_for_year, b.date), 12) + 1 AS au_fiscal_month_num,
            afy.au_fiscal_year_num * 100 + (MOD(DATEDIFF(MONTH, afy.au_fiscal_start_date_for_year, b.date), 12) + 1) AS au_fiscal_month_year_key,
            'Month ' || LPAD((MOD(DATEDIFF(MONTH, afy.au_fiscal_start_date_for_year, b.date), 12) + 1)::VARCHAR, 2, '0') AS au_fiscal_month_desc,
            DATEADD(MONTH, DATEDIFF(MONTH, afy.au_fiscal_start_date_for_year, b.date), afy.au_fiscal_start_date_for_year) AS au_fiscal_month_start_date,
            LAST_DAY(au_fiscal_month_start_date) AS au_fiscal_month_end_date,
            FLOOR(DATEDIFF(DAY, afy.au_fiscal_start_date_for_year, b.date) / 7) + 1 AS au_fiscal_week_num
        FROM base_calendar b
        LEFT JOIN f445_years_processed fy ON b.date >= fy.f445_start_of_year AND b.date <= fy.f445_end_of_year
        LEFT JOIN au_fiscal_years afy ON b.date = afy.au_fiscal_date
    ),
    /* F445 Details */
    f445_detail AS (
        SELECT
            cc.*,
            CASE cc.f445_week_num
                WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 3 THEN 1 WHEN 4 THEN 1 -- P1
                WHEN 5 THEN 2 WHEN 6 THEN 2 WHEN 7 THEN 2 WHEN 8 THEN 2 -- P2
                WHEN 9 THEN 3 WHEN 10 THEN 3 WHEN 11 THEN 3 WHEN 12 THEN 3 WHEN 13 THEN 3 -- P3
                WHEN 14 THEN 4 WHEN 15 THEN 4 WHEN 16 THEN 4 WHEN 17 THEN 4 -- P4
                WHEN 18 THEN 5 WHEN 19 THEN 5 WHEN 20 THEN 5 WHEN 21 THEN 5 -- P5
                WHEN 22 THEN 6 WHEN 23 THEN 6 WHEN 24 THEN 6 WHEN 25 THEN 6 WHEN 26 THEN 6 -- P6
                WHEN 27 THEN 7 WHEN 28 THEN 7 WHEN 29 THEN 7 WHEN 30 THEN 7 -- P7
                WHEN 31 THEN 8 WHEN 32 THEN 8 WHEN 33 THEN 8 WHEN 34 THEN 8 -- P8
                WHEN 35 THEN 9 WHEN 36 THEN 9 WHEN 37 THEN 9 WHEN 38 THEN 9 WHEN 39 THEN 9 -- P9
                WHEN 40 THEN 10 WHEN 41 THEN 10 WHEN 42 THEN 10 WHEN 43 THEN 10 -- P10
                WHEN 44 THEN 11 WHEN 45 THEN 11 WHEN 46 THEN 11 WHEN 47 THEN 11 -- P11
                WHEN 48 THEN 12 WHEN 49 THEN 12 WHEN 50 THEN 12 WHEN 51 THEN 12 WHEN 52 THEN 12 -- P12
                WHEN 53 THEN 12
                ELSE NULL
            END AS f445_period_num,
            CASE WHEN (f445_period_num) IN (1, 2, 3, 4, 5, 6) THEN 1 WHEN (f445_period_num) IN (7, 8, 9, 10, 11, 12) THEN 2 ELSE NULL END AS f445_half_num,
            CASE WHEN (f445_period_num) IN (1, 2, 3) THEN 1 WHEN (f445_period_num) IN (4, 5, 6) THEN 2 WHEN (f445_period_num) IN (7, 8, 9) THEN 3 WHEN (f445_period_num) IN (10, 11, 12) THEN 4 ELSE NULL END AS f445_quarter_num,
            cc.f445_year_num * 10 + f445_quarter_num AS f445_quarter_year_key,
            cc.f445_year_num * 100 + f445_period_num AS f445_year_month_key,
            'HALF ' || f445_half_num::VARCHAR AS f445_half_desc,
            'QTR ' || f445_quarter_num::VARCHAR AS f445_quarter_desc,
            'MONTH ' || f445_period_num::VARCHAR AS f445_period_desc,
            CASE f445_period_num
                    WHEN 1 THEN 'Jul' WHEN 2 THEN 'Aug' WHEN 3 THEN 'Sep' WHEN 4 THEN 'Oct'
                    WHEN 5 THEN 'Nov' WHEN 6 THEN 'Dec' WHEN 7 THEN 'Jan' WHEN 8 THEN 'Feb'
                    WHEN 9 THEN 'Mar' WHEN 10 THEN 'Apr' WHEN 11 THEN 'May' WHEN 12 THEN 'Jun'
                END AS f445_month_short_name,
            CASE f445_period_num
                    WHEN 1 THEN 'July' WHEN 2 THEN 'August' WHEN 3 THEN 'September' WHEN 4 THEN 'October'
                    WHEN 5 THEN 'November' WHEN 6 THEN 'December' WHEN 7 THEN 'January' WHEN 8 THEN 'February'
                    WHEN 9 THEN 'March' WHEN 10 THEN 'April' WHEN 11 THEN 'May' WHEN 12 THEN 'June'
                END AS f445_month_long_name,
            CONCAT(f445_month_short_name, ' ', cc.f445_year_num::STRING) AS f445_month_year_desc,
            CONCAT(f445_month_long_name, ' ', cc.f445_year_num::STRING) AS f445_month_full_year_desc
        FROM combined_calendar cc
    ),
    /* Trading Day Calendar */
    trading_day_calendar AS (
        SELECT
            f445d.*,
            CASE WHEN f445d.is_weekday = 1 AND f445d.is_holiday = 0 THEN 1 ELSE 0 END AS is_trading_day,
            CASE WHEN f445d.is_weekday = 0 THEN 'Weekend' WHEN f445d.is_holiday = 1 THEN 'Holiday' ELSE 'Trading Day' END AS trading_day_desc,
            ROW_NUMBER() OVER (PARTITION BY f445d.year_month_key ORDER BY f445d.date) AS day_of_month_seq,
            SUM(is_trading_day) OVER (PARTITION BY f445d.year_month_key ORDER BY f445d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS trading_day_of_month_seq,
            CASE WHEN f445d.date = f445d.month_start_date THEN 1 ELSE 0 END AS is_first_day_of_month,
            CASE WHEN f445d.date = f445d.month_end_date THEN 1 ELSE 0 END AS is_last_day_of_month,
            CASE WHEN f445d.date = f445d.quarter_start_date THEN 1 ELSE 0 END AS is_first_day_of_quarter,
            CASE WHEN f445d.date = f445d.quarter_end_date THEN 1 ELSE 0 END AS is_last_day_of_quarter,
            LEAD(f445d.date) OVER (ORDER BY f445d.date) AS next_date,
            LAG(f445d.date) OVER (ORDER BY f445d.date) AS previous_date,
            LAG(CASE WHEN is_trading_day = 1 THEN f445d.date END) OVER (ORDER BY f445d.date) AS previous_trading_date,
            LEAD(CASE WHEN is_trading_day = 1 THEN f445d.date END) OVER (ORDER BY f445d.date) AS next_trading_date,
            SUM(is_trading_day) OVER (PARTITION BY f445d.year_month_key) AS trading_days_in_month,
            SUM(is_trading_day) OVER (PARTITION BY f445d.year_quarter_key) AS trading_days_in_quarter,
            SUM(is_trading_day) OVER (PARTITION BY f445d.au_fiscal_month_year_key) AS trading_days_in_au_fiscal_month,
            SUM(is_trading_day) OVER (PARTITION BY f445d.au_fiscal_quarter_year_key) AS trading_days_in_au_fiscal_quarter,
            SUM(is_trading_day) OVER (PARTITION BY f445d.f445_year_month_key) AS trading_days_in_f445_period,
            SUM(is_trading_day) OVER (PARTITION BY f445d.f445_quarter_year_key) AS trading_days_in_f445_quarter
        FROM f445_detail f445d
    ),
    /* Good Fridays */
    good_fridays AS (
         SELECT year_num, MIN(date) as good_friday_date
         FROM base_calendar WHERE is_holiday = 1 AND month_num IN (3, 4) AND day_long_name = 'Friday'
         GROUP BY year_num
    ),
    /* Retail Seasons */
    retail_seasons AS (
        SELECT
            f445d.date, gf.good_friday_date,
            CASE
                WHEN (f445d.month_num = 11) OR (f445d.month_num = 12 AND f445d.day_of_month_num <= 24) THEN 'Christmas Season'
                WHEN (f445d.month_num = 1 AND f445d.day_of_month_num >= 15) OR (f445d.month_num = 2 AND f445d.day_of_month_num <= 15) THEN 'Back to School'
                WHEN gf.good_friday_date IS NOT NULL AND f445d.date BETWEEN DATEADD(DAY, -21, gf.good_friday_date) AND DATEADD(DAY, 1, gf.good_friday_date) THEN 'Easter Season'
                WHEN f445d.month_num = 6 THEN 'EOFY Sales'
                ELSE 'Regular Season'
            END AS retail_season,
            CASE
                WHEN f445d.month_num = 12 AND f445d.day_of_month_num BETWEEN 20 AND 24 THEN 'Christmas Eve Period'
                WHEN f445d.month_num = 12 AND f445d.day_of_month_num = 26 THEN 'Boxing Day'
                WHEN f445d.month_num = 12 AND f445d.day_of_month_num BETWEEN 27 AND 31 THEN 'Post-Christmas Sale'
                ELSE NULL
            END AS holiday_proximity
        FROM f445_detail f445d
        LEFT JOIN good_fridays gf ON f445d.year_num = gf.year_num
    ),
    /* Final Calendar Build - WITHOUT RELATIVE TIME FLAGS */
       final_calendar_build AS (
        SELECT
            f445d.*,
            ly_map.date AS same_business_day_last_year,
            tdc.is_trading_day, tdc.trading_day_desc, tdc.day_of_month_seq, tdc.trading_day_of_month_seq,
            tdc.is_first_day_of_month, tdc.is_last_day_of_month, tdc.is_first_day_of_quarter, tdc.is_last_day_of_quarter,
            tdc.next_date, tdc.previous_date, tdc.next_trading_date, tdc.previous_trading_date,
            tdc.trading_days_in_month, tdc.trading_days_in_quarter,
            tdc.trading_days_in_au_fiscal_month, tdc.trading_days_in_au_fiscal_quarter,
            tdc.trading_days_in_f445_period, tdc.trading_days_in_f445_quarter,
            rs.retail_season, rs.holiday_proximity,
            :current_run_ts AS dw_created_ts,
            'SP_BUILD_BUSINESS_CALENDAR' AS dw_source_system,
            'v1.4 - Added holiday jurisdiction' AS dw_version_desc
        FROM f445_detail f445d
        LEFT JOIN f445_detail ly_map
            ON ly_map.au_fiscal_year_num = f445d.au_fiscal_year_num - 1
           AND ly_map.au_fiscal_week_num = f445d.au_fiscal_week_num
           AND ly_map.iso_day_of_week_num = f445d.iso_day_of_week_num
        LEFT JOIN trading_day_calendar tdc ON f445d.date = tdc.date
        LEFT JOIN retail_seasons rs ON f445d.date = rs.date
    )
    SELECT * FROM final_calendar_build ORDER BY date;

    -- Clustering on the base table
    ALTER TABLE SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE CLUSTER BY (date);

    -- Add comments
    COMMENT ON TABLE SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE IS
    'Base calendar dimension table generated by BUILD_BUSINESS_CALENDAR_SP procedure.
Contains static attributes for Gregorian, AU Fiscal (July-June), and Retail 4-4-5 (starting first Monday of July) calendars.
Relative time period flags (e.g., is_current_month) are calculated dynamically in the BUSINESS_CALENDAR view.

Key features:
1. Standard calendar attributes.
2. Australian fiscal year calendar (FY ends June 30).
3. Retail 4-4-5 calendar system (F-Year ends ~June/July).
4. Year-over-Year mapping using two methods:
   - same_date_last_year: Exact calendar date from previous year.
   - same_business_day_last_year: Same fiscal week and weekday from previous year.
5. Trading day indicators and counts.
6. Retail seasons.

Update Frequency: Infrequent (e.g., yearly) or when holiday/fiscal structure changes.
Query Interface: Use the BUSINESS_CALENDAR view for analysis.
Version: v1.3';

    RETURN status_message;

EXCEPTION
    WHEN OTHER THEN
        status_message := 'ERROR: ' || SQLERRM;
        RETURN status_message;
END
$$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 7: Call stored procedure to build the Business Calendar
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CALL SPG_DAP01.PBI.SP_BUILD_BUSINESS_CALENDAR();

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------    
-- Step 8: Create the View with Dynamic Relative Time Flags and inline column comments
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
BEGIN
CREATE OR REPLACE VIEW SPG_DAP01.PBI.BUSINESS_CALENDAR (
    -- Basic columns
    date COMMENT 'Primary key. The specific calendar date (YYYY-MM-DD).',
    date_key COMMENT 'Integer representation of the date (YYYYMMDD). Useful for joining or partitioning.',
    year_num COMMENT 'Calendar year number (e.g., 2024).',
    year_desc COMMENT 'Calendar year description (e.g., CY24).',
    quarter_num COMMENT 'Calendar quarter number (1-4).',
    quarter_desc COMMENT 'Calendar quarter description (e.g., Q1).',
    year_quarter_key COMMENT 'Integer key for calendar year and quarter (YYYYQ).',
    month_num COMMENT 'Calendar month number (1-12).',
    month_short_name COMMENT 'Abbreviated calendar month name (e.g., Jan).',
    month_long_name COMMENT 'Full calendar month name (e.g., January).',
    year_month_key COMMENT 'Integer key for calendar year and month (YYYYMM).',
    month_year_desc COMMENT 'Calendar month and year description (e.g., Jan 2024).',
    week_of_year_num COMMENT 'Week number within the calendar year (behavior depends on WEEK_START session parameter).',
    year_of_week_num COMMENT 'Year number associated with week_of_year_num (depends on WEEK_START).',
    year_week_desc COMMENT 'Year and week description (depends on WEEK_START).',
    iso_week_num COMMENT 'ISO 8601 week number within the ISO year (Week starts Monday).',
    iso_year_of_week_num COMMENT 'ISO 8601 year number associated with the ISO week.',
    iso_year_week_desc COMMENT 'ISO 8601 year and week description.',
    day_of_month_num COMMENT 'Day number within the calendar month (1-31).',
    day_of_week_num COMMENT 'Day number within the week (0=Sunday, 1=Monday, ..., 6=Saturday). Behavior depends on WEEK_START session parameter.',
    iso_day_of_week_num COMMENT 'ISO 8601 day number within the week (1=Monday, ..., 7=Sunday).',
    day_of_year_num COMMENT 'Day number within the calendar year (1-366).',
    day_short_name COMMENT 'Abbreviated day name (e.g., Mon).',
    day_long_name COMMENT 'Full day name (e.g., Monday).',
    date_full_desc COMMENT 'Full date description (e.g., 27 Mar 2024).',
    date_formatted COMMENT 'Date formatted as DD/MM/YYYY.',
    month_start_date COMMENT 'First day of the calendar month.',
    month_end_date COMMENT 'Last day of the calendar month.',
    quarter_start_date COMMENT 'First day of the calendar quarter.',
    quarter_end_date COMMENT 'Last day of the calendar quarter.',
    year_start_date COMMENT 'First day of the calendar year.',
    year_end_date COMMENT 'Last day of the calendar year.',
    week_start_date COMMENT 'First day of the week (behavior depends on WEEK_START session parameter).',
    week_end_date COMMENT 'Last day of the week (behavior depends on WEEK_START session parameter).',
    day_of_month_count COMMENT 'Sequential day number within the calendar month (1 to N).',
    days_in_month_count COMMENT 'Total number of days in the calendar month.',
    day_of_quarter_count COMMENT 'Sequential day number within the calendar quarter (1 to N).',
    days_in_quarter_count COMMENT 'Total number of days in the calendar quarter.',
    week_of_month_num COMMENT 'Week number within the calendar month (1-6). Calculated as CEIL(day_of_month_num / 7.0).',
    week_of_quarter_num COMMENT 'Week number within the calendar quarter (1-14). Calculated as CEIL(day_of_quarter_count / 7.0).',

    -- Business indicator columns
    is_weekday COMMENT 'Indicator (1/0) if the day is a weekday (Monday-Friday based on day_of_week_num).',
    weekday_indicator COMMENT 'Description (Weekday/Weekend) based on is_weekday.',
    is_holiday COMMENT 'Indicator (1/0) if the day exists in the AU_PUBLIC_HOLIDAYS table.',
    holiday_state COMMENT 'The state/jurisdiction for which this holiday applies (e.g., ''National'', ''VIC'', ''NSW''). NULL for non-holiday dates.',
    holiday_indicator COMMENT 'Description (Holiday/Non-Holiday) based on is_holiday.',
    is_trading_day COMMENT 'Indicator (1/0) if the day is a weekday AND not a holiday. Useful for business day calculations.',
    trading_day_desc COMMENT 'Description (Trading Day/Weekend/Holiday) based on is_trading_day logic.',

    -- YoY comparison columns
    same_date_last_year COMMENT 'The exact same calendar date from the previous year (e.g., 2023-03-15 for 2024-03-15). Properly handles leap years. Use for strict date-based comparisons (e.g., financial month-end).',
    same_business_day_last_year COMMENT 'The equivalent business day from the previous AU fiscal year, matching AU fiscal week number and ISO day of week number. Use for business performance comparisons where week alignment matters (e.g., retail sales). May not be the same calendar date.',

    -- Australian fiscal calendar columns
    au_fiscal_year_num COMMENT 'Australian Financial Year number (July 1 - June 30), designated by the calendar year it ends in (e.g., FY24 = Jul 2023 - Jun 2024).',
    au_fiscal_year_desc COMMENT 'Australian Financial Year description (e.g., FY24).',
    au_fiscal_quarter_num COMMENT 'Quarter number within the Australian Fiscal Year (1-4, where Q1 = Jul-Sep).',
    au_fiscal_quarter_desc COMMENT 'Australian Fiscal Quarter description (e.g., QTR 1).',
    au_fiscal_quarter_year_key COMMENT 'Integer key for AU fiscal year and quarter (YYYYQ, where YYYY is fiscal year number).',
    au_fiscal_month_num COMMENT 'Month number within the Australian Fiscal Year (1-12, where 1 = July).',
    au_fiscal_month_desc COMMENT 'Australian Fiscal Month description (e.g., Month 01 for July).',
    au_fiscal_month_year_key COMMENT 'Integer key for AU fiscal year and month number (YYYYMM, where YYYY is fiscal year number, MM is fiscal month 1-12).',
    au_fiscal_week_num COMMENT 'Sequential week number within the Australian Fiscal Year (starting from 1). Simple calculation based on days since FY start.',
    au_fiscal_start_date_for_year COMMENT 'Start date (July 1) of the AU Fiscal Year this date belongs to.',
    au_fiscal_end_date_for_year COMMENT 'End date (June 30) of the AU Fiscal Year this date belongs to.',
    au_fiscal_quarter_start_date COMMENT 'Start date of the AU Fiscal Quarter this date belongs to.',
    au_fiscal_quarter_end_date COMMENT 'End date of the AU Fiscal Quarter this date belongs to.',
    au_fiscal_month_start_date COMMENT 'Start date of the AU Fiscal Month (within the fiscal year) this date belongs to.',
    au_fiscal_month_end_date COMMENT 'End date of the AU Fiscal Month (within the fiscal year) this date belongs to.',

    -- Retail 4-4-5 calendar columns
    f445_year_num COMMENT 'Retail 4-4-5 Year number, designated by the calendar year it ends in (assumption: starts first Sunday of July).',
    f445_year_desc COMMENT 'Retail 4-4-5 Year description (e.g., F24).',
    f445_half_num COMMENT 'Half number within the Retail 4-4-5 Year (1 or 2).',
    f445_half_desc COMMENT 'Retail 4-4-5 Half description (e.g., HALF 1).',
    f445_quarter_num COMMENT 'Quarter number within the Retail 4-4-5 Year (1-4), based on 4-4-5 period groupings.',
    f445_quarter_desc COMMENT 'Retail 4-4-5 Quarter description (e.g., QTR 1).',
    f445_quarter_year_key COMMENT 'Integer key for F445 year and quarter (YYYYQ, where YYYY is F445 year number).',
    f445_period_num COMMENT 'Period number (month equivalent) within the Retail 4-4-5 Year (1-12), following a 4-4-5 week pattern.',
    f445_period_desc COMMENT 'Retail 4-4-5 Period description (e.g., MONTH 1).',
    f445_year_month_key COMMENT 'Integer key for F445 year and period number (YYYYPP, where YYYY is F445 year number, PP is period 1-12).',
    f445_month_short_name COMMENT 'Equivalent calendar month name (approx) for the F445 period (e.g., Jul for Period 1).',
    f445_month_long_name COMMENT 'Full equivalent calendar month name (approx) for the F445 period (e.g., July for Period 1).',
    f445_month_year_desc COMMENT 'F445 equivalent month and year description (e.g., Jul F24).',
    f445_month_full_year_desc COMMENT 'F445 equivalent full month and year description (e.g., July F24).',
    f445_week_num COMMENT 'Sequential week number within the Retail 4-4-5 Year (starting from 1).',
    f445_start_of_year COMMENT 'Start date of the F445 Year this date belongs to (assumed first Sunday of July).',
    f445_end_of_year COMMENT 'End date of the F445 Year this date belongs to (calculated).',

    -- Trading day sequence & count columns
    day_of_month_seq COMMENT 'Sequential number for all days within the calendar month (1 to N).',
    trading_day_of_month_seq COMMENT 'Sequential number for trading days only within the calendar month (1 to M, where M <= N).',
    is_first_day_of_month COMMENT 'Indicator (1/0) if this is the first calendar day of the month.',
    is_last_day_of_month COMMENT 'Indicator (1/0) if this is the last calendar day of the month.',
    is_first_day_of_quarter COMMENT 'Indicator (1/0) if this is the first calendar day of the quarter.',
    is_last_day_of_quarter COMMENT 'Indicator (1/0) if this is the last calendar day of the quarter.',
    next_date COMMENT 'The next calendar date.',
    previous_date COMMENT 'The previous calendar date.',
    next_trading_date COMMENT 'The next trading date (skips weekends/holidays).',
    previous_trading_date COMMENT 'The previous trading date (skips weekends/holidays).',
    trading_days_in_month COMMENT 'Total count of trading days in the calendar month.',
    trading_days_in_quarter COMMENT 'Total count of trading days in the calendar quarter.',
    trading_days_in_au_fiscal_month COMMENT 'Total count of trading days in the AU Fiscal Month.',
    trading_days_in_au_fiscal_quarter COMMENT 'Total count of trading days in the AU Fiscal Quarter.',
    trading_days_in_f445_period COMMENT 'Total count of trading days in the F445 Period.',
    trading_days_in_f445_quarter COMMENT 'Total count of trading days in the F445 Quarter.',

    -- Retail season columns
    retail_season COMMENT 'Categorizes dates into Australian retail seasons: ''Christmas Season'' (Nov 1 - Dec 24), ''Back to School'' (Jan 15 - Feb 15), ''Easter Season'' (3 weeks prior to Good Friday incl. Easter Monday), ''EOFY Sales'' (June), ''Regular Season'' (other).',
    holiday_proximity COMMENT 'Identifies specific periods near major holidays: ''Christmas Eve Period'' (Dec 20-24), ''Boxing Day'' (Dec 26), ''Post-Christmas Sale'' (Dec 27-31). NULL otherwise.',

    -- Metadata columns
    dw_created_ts COMMENT 'Timestamp (LTZ) when the row/table was created or last updated by the procedure.',
    dw_source_system COMMENT 'The name of the procedure that generated this data (BUILD_BUSINESS_CALENDAR_SP).',
    dw_version_desc COMMENT 'Version description of the logic used to generate the data.',

    -- Dynamic relative time flags
    is_current_date COMMENT 'Indicator (1/0) if this date IS CURRENT_DATE(). Calculated dynamically.',
    is_current_month COMMENT 'Indicator (1/0) if this date is in the same calendar month as CURRENT_DATE(). Calculated dynamically.',
    is_current_quarter COMMENT 'Indicator (1/0) if this date is in the same calendar quarter as CURRENT_DATE(). Calculated dynamically.',
    is_current_year COMMENT 'Indicator (1/0) if this date is in the same calendar year as CURRENT_DATE(). Calculated dynamically.',
    is_current_fiscal_year COMMENT 'Indicator (1/0) if this date is in the same AU fiscal year as CURRENT_DATE(). Calculated dynamically.',
    is_last_7_days COMMENT 'Indicator (1/0) if this date is within the 6 days prior to CURRENT_DATE() or is CURRENT_DATE(). Calculated dynamically.',
    is_last_30_days COMMENT 'Indicator (1/0) if this date is within the 29 days prior to CURRENT_DATE() or is CURRENT_DATE(). Calculated dynamically.',
    is_last_90_days COMMENT 'Indicator (1/0) if this date is within the 89 days prior to CURRENT_DATE() or is CURRENT_DATE(). Calculated dynamically.',
    is_previous_month COMMENT 'Indicator (1/0) if this date is in the calendar month immediately preceding the month of CURRENT_DATE(). Calculated dynamically.',
    is_previous_quarter COMMENT 'Indicator (1/0) if this date is in the calendar quarter immediately preceding the quarter of CURRENT_DATE(). Calculated dynamically.',
    is_rolling_12_months COMMENT 'Indicator (1/0) if this date is within the 12 months preceding CURRENT_DATE() (inclusive). Calculated dynamically.',
    is_rolling_quarter COMMENT 'Indicator (1/0) if this date is within the 3 months preceding CURRENT_DATE() (inclusive). Calculated dynamically.',
    is_year_to_date COMMENT 'Indicator (1/0) if this date is within the current calendar year, up to and including CURRENT_DATE(). Calculated dynamically.',
    is_fiscal_year_to_date COMMENT 'Indicator (1/0) if this date is within the current AU fiscal year, up to and including CURRENT_DATE(). Calculated dynamically.',
    is_quarter_to_date COMMENT 'Indicator (1/0) if this date is within the current calendar quarter, up to and including CURRENT_DATE(). Calculated dynamically.',
    is_month_to_date COMMENT 'Indicator (1/0) if this date is within the current calendar month, up to and including CURRENT_DATE(). Calculated dynamically.'
)
AS
WITH current_values AS (
    -- Pre-calculate values based on CURRENT_DATE() once for efficiency in the view
    SELECT
        CURRENT_DATE() AS current_date_val,
        DATE_TRUNC('MONTH', CURRENT_DATE()) AS current_month_start,
        DATE_TRUNC('QUARTER', CURRENT_DATE()) AS current_quarter_start,
        DATE_TRUNC('YEAR', CURRENT_DATE()) AS current_year_start,
        (SELECT MAX(b.year_month_key) FROM SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE b WHERE b.date < DATE_TRUNC('MONTH', CURRENT_DATE())) AS prev_month_key,
        (SELECT MAX(b.year_quarter_key) FROM SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE b WHERE b.date < DATE_TRUNC('QUARTER', CURRENT_DATE())) AS prev_quarter_key,
        (SELECT b.au_fiscal_year_num FROM SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE b WHERE b.date = CURRENT_DATE()) AS current_au_fiscal_year_num,
        (SELECT b.au_fiscal_start_date_for_year FROM SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE b WHERE b.date = CURRENT_DATE()) AS current_au_fiscal_start_date
)
SELECT
    -- Select all columns from the base table
    base.*,

    -- Calculate Relative Time Period Flags Dynamically
    CASE WHEN base.date = cv.current_date_val THEN 1 ELSE 0 END AS is_current_date,
    CASE WHEN base.year_month_key = TO_NUMBER(TO_CHAR(cv.current_date_val, 'YYYYMM')) THEN 1 ELSE 0 END AS is_current_month,
    CASE WHEN base.year_quarter_key = (YEAR(cv.current_date_val) * 10 + QUARTER(cv.current_date_val)) THEN 1 ELSE 0 END AS is_current_quarter,
    CASE WHEN base.year_num = YEAR(cv.current_date_val) THEN 1 ELSE 0 END AS is_current_year,
    CASE WHEN base.au_fiscal_year_num = cv.current_au_fiscal_year_num THEN 1 ELSE 0 END AS is_current_fiscal_year,
    CASE WHEN base.date BETWEEN DATEADD(DAY, -6, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_last_7_days,
    CASE WHEN base.date BETWEEN DATEADD(DAY, -29, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_last_30_days,
    CASE WHEN base.date BETWEEN DATEADD(DAY, -89, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_last_90_days,
    CASE WHEN base.year_month_key = cv.prev_month_key THEN 1 ELSE 0 END AS is_previous_month,
    CASE WHEN base.year_quarter_key = cv.prev_quarter_key THEN 1 ELSE 0 END AS is_previous_quarter,
    CASE WHEN base.date BETWEEN DATEADD(MONTH, -12, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_rolling_12_months,
    CASE WHEN base.date BETWEEN DATEADD(MONTH, -3, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_rolling_quarter,
    CASE WHEN base.year_num = YEAR(cv.current_date_val) AND base.date BETWEEN cv.current_year_start AND cv.current_date_val THEN 1 ELSE 0 END AS is_year_to_date,
    CASE WHEN base.au_fiscal_year_num = cv.current_au_fiscal_year_num AND base.date BETWEEN cv.current_au_fiscal_start_date AND cv.current_date_val THEN 1 ELSE 0 END AS is_fiscal_year_to_date,
    CASE WHEN base.year_quarter_key = (YEAR(cv.current_date_val) * 10 + QUARTER(cv.current_date_val)) AND base.date BETWEEN cv.current_quarter_start AND cv.current_date_val THEN 1 ELSE 0 END AS is_quarter_to_date,
    CASE WHEN base.year_month_key = TO_NUMBER(TO_CHAR(cv.current_date_val, 'YYYYMM')) AND base.date BETWEEN cv.current_month_start AND cv.current_date_val THEN 1 ELSE 0 END AS is_month_to_date

FROM SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE base
CROSS JOIN current_values cv;

-- Add comment on the view itself
COMMENT ON VIEW SPG_DAP01.PBI.BUSINESS_CALENDAR IS 'View providing a comprehensive calendar dimension by combining the static BUSINESS_CALENDAR_BASE table with dynamically calculated relative time period flags (e.g., is_current_month, is_last_7_days). Use this view for all reporting and analysis. Relative flags are always up-to-date based on CURRENT_DATE() at query time.';

END;

