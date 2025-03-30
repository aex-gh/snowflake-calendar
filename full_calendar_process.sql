-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 1: Setup network rules to allow access to data.gov.au
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE NETWORK RULE allow_data_gov_au MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('data.gov.au:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION apis_access_integration
  ALLOWED_NETWORK_RULES = (allow_data_gov_au) -- Specifies the network rules that this integration uses to control network access.
  ENABLED = true; -- Enables the external access integration.  Must be true for it to function.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 2: Setup LOAD_AU_HOLIDAYS(DATABASE_NAME, SCHEMA_NAME) stored procedure
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_LOAD_AU_HOLIDAYS(DATABASE_NAME STRING, SCHEMA_NAME STRING)
RETURNS String
LANGUAGE PYTHON
RUNTIME_VERSION=3.11
PACKAGES=('pandas==2.2.3','requests==2.32.3','snowflake-snowpark-python==*')
HANDLER='main'
COMMENT='Load Australian holidays from data.gov.au with retry logic and CSV fallback'
EXTERNAL_ACCESS_INTEGRATIONS = (apis_access_integration)
AS
'
import requests
import pandas as pd
import time
import io
from datetime import datetime
from snowflake.snowpark import Session

STAGE_NAME = "AU_PUBLIC_HOLIDAYS_STAGE"
TABLE_NAME = "AU_PUBLIC_HOLIDAYS"
BASE_API_URL = "https://data.gov.au/data/api/action/datastore_search"
SQL_API_URL = "https://data.gov.au/data/api/action/datastore_search_sql"
RESOURCE_ID = "33673aca-0857-42e5-b8f0-9981b4755686"  # Public holidays resource ID
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
FALLBACK_FILE = "australian_holidays_fallback.csv"  # Fallback data filename (CSV instead of JSON)

def create_table_if_not_exists(session, database_name, schema_name, table_name):
    """
    Create the holidays table if it doesn\'t exist
    """
    try:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
            HOLIDAY_ID NUMBER,
            HOLIDAY_DATE DATE,
            HOLIDAY_NAME STRING,
            INFORMATION STRING),
            MORE_INFORMATION STRING,
            JURISDICTION STRING),
            LOADED_AT TIMESTAMP_NTZ,
            DATA_SOURCE STRING  -- New column to track data source
        )
        """
        session.sql(create_table_sql).collect()
        return True
    except Exception as e:
        print(f"Error creating table: {e}")
        return False

def check_table_exists(session, database_name, schema_name, table_name):
    """
    Check if the holidays table exists and has data
    """
    try:
        result = session.sql(f"SELECT COUNT(*) AS count FROM {database_name}.{schema_name}.{table_name}").collect()
        return result[0]["COUNT"] > 0
    except Exception:
        return False

def get_latest_holiday_date(session, database_name, schema_name, table_name):
    """
    Get the latest holiday date from the existing table
    """
    try:
        result = session.sql(f"""
            SELECT MAX(HOLIDAY_DATE) AS max_date
            FROM {database_name}.{schema_name}.{table_name}
        """).collect()

        max_date = result[0]["MAX_DATE"]
        if max_date:
            return max_date.strftime("%Y%m%d")
        return None
    except Exception as e:
        print(f"Error getting latest holiday date: {e}")
        return None

def fetch_api_data_with_retry(api_url, params=None):
    """
    Fetch data from API with retry logic
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(api_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data["success"]:
                return data
            else:
                print(f"API request unsuccessful: {data.get(\'error\', \'Unknown error\')}")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")

        # Don\'t sleep on the last attempt
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)

    # All attempts failed
    return None

def build_sql_query(latest_date=None):
    """
    Build the SQL query for the API, using the latest date if available
    """
    if latest_date:
        # Incremental load - fetch only newer records
        return f"SELECT * from \\"{RESOURCE_ID}\\" WHERE \\"Date\\" > \'{latest_date}\'"
    else:
        # Initial load - fetch all records
        return f"SELECT * from \\"{RESOURCE_ID}\\""

def fetch_holidays_via_sql_api(latest_date=None):
    """
    Fetch holidays using the SQL API endpoint
    """
    sql_query = build_sql_query(latest_date)
    params = {"sql": sql_query}

    print(f"Fetching holidays with query: {sql_query}")
    data = fetch_api_data_with_retry(SQL_API_URL, params)

    if data and "result" in data and "records" in data["result"]:
        return data["result"]["records"]
    return None

def fetch_holidays_via_standard_api():
    """
    Fallback to standard API if SQL API fails
    """
    params = {"resource_id": RESOURCE_ID, "limit": 1000}  # Adjust limit as needed
    data = fetch_api_data_with_retry(BASE_API_URL, params)

    if data and "result" in data and "records" in data["result"]:
        return data["result"]["records"]
    return None

def check_fallback_data_exists(session, database_name, schema_name, stage_name):
    """
    Check if fallback CSV data file exists in the stage
    """
    try:
        result = session.sql(f"""
            SELECT COUNT(*) AS count
            FROM TABLE(INFORMATION_SCHEMA.FILES(
                PATTERN=>\'.*{FALLBACK_FILE}\',
                STAGE_NAME=>\'@{database_name}.{schema_name}.{stage_name}\'
            ))
        """).collect()
        return result[0]["COUNT"] > 0
    except Exception as e:
        print(f"Error checking fallback data: {e}")
        return False

def load_fallback_data(session, database_name, schema_name, stage_name):
    """
    Load fallback data from CSV in stage if available
    """
    try:
        # Create a file format for CSV
        session.sql(f"""
            CREATE FILE FORMAT IF NOT EXISTS {database_name}.{schema_name}.csv_format
            TYPE = CSV
            FIELD_DELIMITER = \',\'
            SKIP_HEADER = 1
            NULL_IF = (\'NULL\', \'\')
            FIELD_OPTIONALLY_ENCLOSED_BY = \'\"\'
        """).collect()

        # Load the CSV data into a temporary table
        temp_table = f"{database_name}.{schema_name}.TEMP_HOLIDAYS"
        session.sql(f"""
            CREATE OR REPLACE TEMPORARY TABLE {temp_table} AS
            SELECT
                $1::NUMBER AS HOLIDAY_ID,
                TO_DATE($2, \'YYYYMMDD\') AS HOLIDAY_DATE,
                $3::STRING AS HOLIDAY_NAME,
                $4::STRING AS INFORMATION,
                $5::STRING AS MORE_INFORMATION,
                $6::STRING AS JURISDICTION
            FROM @{database_name}.{schema_name}.{stage_name}/{FALLBACK_FILE}
            (FILE_FORMAT => \'{database_name}.{schema_name}.csv_format\')
        """).collect()

        # Convert to records format (similar to API response)
        df = session.table(temp_table).to_pandas()

        # Clean up temporary table
        session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()

        if not df.empty:
            # Convert df back to records format (list of dicts)
            # Convert HOLIDAY_DATE back to string format for consistency with API
            df[\'Date\'] = df[\'HOLIDAY_DATE\'].dt.strftime(\'%Y%m%d\')

            # Map back to original API column names for consistency
            records = df.rename(columns={
                \'HOLIDAY_ID\': \'_id\',
                \'HOLIDAY_NAME\': \'Holiday Name\',
                \'INFORMATION\': \'Information\',
                \'MORE_INFORMATION\': \'More Information\',
                \'JURISDICTION\': \'Jurisdiction\'
            }).to_dict(\'records\')

            return records

        return None
    except Exception as e:
        print(f"Error loading fallback data: {e}")
        return None

def save_fallback_data(session, database_name, schema_name, stage_name, data):
    """
    Save the latest API data as CSV fallback for future use
    """
    try:
        # Convert data to DataFrame
        df = pd.DataFrame(data)

        # Ensure we have all the right columns in the right order
        columns_order = [
            \'_id\',
            \'Date\',
            \'Holiday Name\',
            \'Information\',
            \'More Information\',
            \'Jurisdiction\'
        ]

        # Filter to only include the columns we need
        df = df[columns_order]

        # Convert DataFrame to CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_str = csv_buffer.getvalue()

        # Write to a temporary file
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=\'.csv\', delete=False) as temp_file:
            temp_file_path = temp_file.name
            temp_file.write(csv_str.encode())

        # Upload to stage
        session.file.put(
            temp_file_path,
            f"@{database_name}.{schema_name}.{stage_name}/{FALLBACK_FILE}",
            auto_compress=False,
            overwrite=True
        )

        # Clean up temporary file
        import os
        os.unlink(temp_file_path)

        return True
    except Exception as e:
        print(f"Error saving fallback data: {e}")
        return False

def create_stage(session, stage_name, database_name, schema_name):
    """
    Create an internal stage if it doesn\'t exist
    """
    try:
        fully_qualified_stage = f"{database_name}.{schema_name}.{stage_name}"
        session.sql(f"CREATE STAGE IF NOT EXISTS {fully_qualified_stage}").collect()
        return True
    except Exception as e:
        print(f"Error creating stage: {e}")
        return False

def process_and_load_data(session, data, database_name, schema_name, table_name, data_source):
    """
    Process the holiday data and load it into Snowflake
    """
    try:
        if not data or len(data) == 0:
            return "No new records to load"

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Check if the expected columns exist
        required_columns = [\'_id\', \'Date\', \'Holiday Name\', \'Information\', \'More Information\', \'Jurisdiction\']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            return f"API structure has changed. Missing columns: {missing_columns}"

        # Process the date column
        df[\'HOLIDAY_DATE\'] = pd.to_datetime(df[\'Date\'], format=\'%Y%m%d\').dt.date

        # Rename columns to match table schema
        df = df.rename(columns={
            \'_id\': \'HOLIDAY_ID\',
            \'Holiday Name\': \'HOLIDAY_NAME\',
            \'Information\': \'INFORMATION\',
            \'More Information\': \'MORE_INFORMATION\',
            \'Jurisdiction\': \'JURISDICTION\'
        })

        # Add load timestamp and data source
        df[\'LOADED_AT\'] = datetime.now()
        df[\'DATA_SOURCE\'] = data_source  # Add the data source

        # Select and reorder columns to match table schema
        df = df[[
            \'HOLIDAY_ID\',
            \'HOLIDAY_DATE\',
            \'HOLIDAY_NAME\',
            \'INFORMATION\',
            \'MORE_INFORMATION\',
            \'JURISDICTION\',
            \'LOADED_AT\',
            \'DATA_SOURCE\'  # Include the data source in the output
        ]]

        # Convert pandas DataFrame to Snowpark DataFrame
        snowdf = session.create_dataframe(df)

        # Write DataFrame to Snowflake table
        snowdf.write.mode("append").save_as_table(f"{database_name}.{schema_name}.{table_name}")

        return f"Successfully loaded {len(df)} rows into {database_name}.{schema_name}.{table_name}"
    except Exception as e:
        error_msg = f"Error processing and loading data: {e}"
        print(error_msg)
        return error_msg

def main(session, DATABASE_NAME, SCHEMA_NAME):
    """
    Main execution function
    """
    try:
        # Create stage
        if not create_stage(session, STAGE_NAME, DATABASE_NAME, SCHEMA_NAME):
            return "Failed to create stage"

        # Create table if not exists
        if not create_table_if_not_exists(session, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME):
            return "Failed to create table"

        # Check if table has data
        has_data = check_table_exists(session, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME)

        # Get the latest holiday date if we have data
        latest_date = None
        if has_data:
            latest_date = get_latest_holiday_date(session, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME)
            print(f"Latest holiday date in database: {latest_date}")

        # Try SQL API first
        holiday_data = fetch_holidays_via_sql_api(latest_date)
        data_source = "data.gov.au SQL API"

        # If SQL API fails, try standard API
        if not holiday_data:
            print("SQL API failed, trying standard API...")
            holiday_data = fetch_holidays_via_standard_api()
            data_source = "data.gov.au Standard API"

        # If both APIs fail, try fallback data
        if not holiday_data:
            print("Both APIs failed, checking for fallback data...")
            if check_fallback_data_exists(session, DATABASE_NAME, SCHEMA_NAME, STAGE_NAME):
                holiday_data = load_fallback_data(session, DATABASE_NAME, SCHEMA_NAME, STAGE_NAME)
                if holiday_data:
                    print("Using fallback data from CSV")
                    data_source = "Local CSV Fallback"
                    # Filter fallback data to only include records after latest_date if incremental
                    if latest_date:
                        holiday_data = [record for record in holiday_data
                                       if int(record.get("Date", "0")) > int(latest_date)]

        # If we have data by any method, process and load it
        if holiday_data:
            # Save current data as fallback for future use - only save API data, not already fallback data
            if "API" in data_source:
                save_fallback_data(session, DATABASE_NAME, SCHEMA_NAME, STAGE_NAME, holiday_data)

            # Add timestamp to data source for audit trail
            timestamped_data_source = f"{data_source} ({datetime.now().strftime(\'%Y-%m-%d %H:%M:%S\')})"

            # Process and load the data
            result = process_and_load_data(session, holiday_data, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, timestamped_data_source)
            return f"Source: {data_source}. {result}"
        else:
            return "Failed to retrieve holiday data from API and no fallback data available"

    except Exception as e:
        error_msg = f"Error in main process: {e}"
        print(error_msg)
        return error_msg
'
;

-- LOAD PROCEDURE EXAMPLE
-- Example of how to call the stored procedure.  Replace with your database and schema names.
-- call sp_load_au_holidays('SPG_DAP01','PBI');

-- DROP PROCEDURE EXAMPLE
-- Example of how to drop the stored procedure.  Use with caution!
-- drop procedure if exists sp_load_au_holidays(STRING, STRING);

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 3: Create the AU_PUBLIC_HOLIDAYS table
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
call sp_load_au_holidays('DATABASE', 'SCHEMA');

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 4: Create a view to store the public holidays and allow semantic modification for business requirements
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SPG_DAP01.PBI.AU_PUBLIC_HOLIDAYS_VW AS
SELECT
    HOLIDAY_DATE as date,
    HOLIDAY_NAME as holiday_name,
    JURISDICTION as state
FROM SPG_DAP01.PBI.AU_PUBLIC_HOLIDAYS;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- STEP 5A: Create function for date spine
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SPG_DAP01.PBI.FN_GENERATE_DATE_SPINE(
    start_date DATE,  -- Changed to explicitly accept DATE
    end_date DATE,    -- Changed to explicitly accept DATE
    date_grain STRING,
    timezone STRING DEFAULT 'UTC'
)
RETURNS TABLE (
    calendar_date TIMESTAMP_NTZ,
    calendar_date_tz TIMESTAMP_NTZ,
    timezone STRING
)
AS
$$
    WITH row_count_calc AS (
        SELECT
            GREATEST(
                DATEDIFF('MINUTE', start_date::TIMESTAMP_NTZ, end_date::TIMESTAMP_NTZ) * CASE WHEN UPPER(date_grain) = 'MINUTE' THEN 1 ELSE 0 END,
                DATEDIFF('HOUR', start_date::TIMESTAMP_NTZ, end_date::TIMESTAMP_NTZ) * CASE WHEN UPPER(date_grain) = 'HOUR' THEN 1 ELSE 0 END,
                DATEDIFF('DAY', start_date, end_date) * CASE WHEN UPPER(date_grain) IN ('DAY', '') OR date_grain IS NULL THEN 1 ELSE 0 END,
                DATEDIFF('WEEK', start_date, end_date) * CASE WHEN UPPER(date_grain) = 'WEEK' THEN 1 ELSE 0 END,
                DATEDIFF('MONTH', start_date, end_date) * CASE WHEN UPPER(date_grain) = 'MONTH' THEN 1 ELSE 0 END,
                DATEDIFF('QUARTER', start_date, end_date) * CASE WHEN UPPER(date_grain) = 'QUARTER' THEN 1 ELSE 0 END,
                DATEDIFF('YEAR', start_date, end_date) * CASE WHEN UPPER(date_grain) = 'YEAR' THEN 1 ELSE 0 END,
                1  -- Ensure at least 1 row
            ) + 1 AS row_count_needed
    ),
    date_series AS (
        SELECT
            CASE
                WHEN UPPER(date_grain) = 'MINUTE'  THEN DATEADD('MINUTE', seq4(), start_date::TIMESTAMP_NTZ)
                WHEN UPPER(date_grain) = 'HOUR'    THEN DATEADD('HOUR', seq4(), start_date::TIMESTAMP_NTZ)
                WHEN UPPER(date_grain) = 'WEEK'    THEN DATEADD('WEEK', seq4(), start_date)
                WHEN UPPER(date_grain) = 'MONTH'   THEN DATEADD('MONTH', seq4(), start_date)
                WHEN UPPER(date_grain) = 'QUARTER' THEN DATEADD('QUARTER', seq4(), start_date)
                WHEN UPPER(date_grain) = 'YEAR'    THEN DATEADD('YEAR', seq4(), start_date)
                ELSE DATEADD('DAY', seq4(), start_date)  -- Default to DAY
            END AS calendar_date
        FROM row_count_calc,
             TABLE(GENERATOR(ROWCOUNT => row_count_needed))
    )

    SELECT
        calendar_date,
        CONVERT_TIMEZONE('UTC', timezone, calendar_date) AS calendar_date_tz,
        timezone
    FROM date_series
    WHERE calendar_date <= end_date::TIMESTAMP_NTZ
    ORDER BY calendar_date
$$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- STEP 5B: Create stored procedure for holidays
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SPG_DAP01.PBI.SP_PROCESS_HOLIDAYS(
    calendar_table STRING,
    output_table STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    query STRING;
BEGIN
    -- Drop the output table if it exists
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || output_table;

    -- Construct the dynamic query
    query := 'CREATE TABLE ' || output_table || ' AS
        SELECT
            cal.calendar_date AS date,
            COALESCE(
                OBJECT_CONSTRUCT(
                    ''is_holiday_nsw'', MAX(CASE WHEN UPPER(h.STATE) = ''NSW'' THEN 1 ELSE 0 END),
                    ''is_holiday_vic'', MAX(CASE WHEN UPPER(h.STATE) = ''VIC'' THEN 1 ELSE 0 END),
                    ''is_holiday_qld'', MAX(CASE WHEN UPPER(h.STATE) = ''QLD'' THEN 1 ELSE 0 END),
                    ''is_holiday_sa'', MAX(CASE WHEN UPPER(h.STATE) = ''SA'' THEN 1 ELSE 0 END),
                    ''is_holiday_wa'', MAX(CASE WHEN UPPER(h.STATE) = ''WA'' THEN 1 ELSE 0 END),
                    ''is_holiday_tas'', MAX(CASE WHEN UPPER(h.STATE) = ''TAS'' THEN 1 ELSE 0 END),
                    ''is_holiday_act'', MAX(CASE WHEN UPPER(h.STATE) = ''ACT'' THEN 1 ELSE 0 END),
                    ''is_holiday_nt'', MAX(CASE WHEN UPPER(h.STATE) = ''NT'' THEN 1 ELSE 0 END),
                    ''is_holiday_national'', MAX(CASE
                        WHEN UPPER(h.STATE) = ''NATIONAL'' THEN 1
                        WHEN COUNT(DISTINCT CASE WHEN UPPER(h.STATE) IN (''NSW'', ''VIC'', ''QLD'', ''SA'', ''WA'', ''TAS'', ''ACT'', ''NT'') THEN UPPER(h.STATE) END) OVER (PARTITION BY cal.calendar_date) >= 8 THEN 1
                        ELSE 0
                    END)
                ),
                OBJECT_CONSTRUCT()
            ) AS holiday_metadata,
            CASE WHEN MAX(h.DATE) IS NOT NULL THEN 1 ELSE 0 END AS is_holiday,
            CASE WHEN MAX(h.DATE) IS NOT NULL THEN ''Holiday'' ELSE ''Non-Holiday'' END AS holiday_indicator,
            LISTAGG(DISTINCT CASE WHEN h.HOLIDAY_NAME IS NOT NULL THEN h.HOLIDAY_NAME || '' ('' || UPPER(h.STATE) || '')'' ELSE NULL END, ''; '') AS holiday_desc
        FROM ' || calendar_table || ' cal
        LEFT JOIN SPG_DAP01.PBI.AU_PUBLIC_HOLIDAYS_VW h ON cal.calendar_date = h.DATE
        GROUP BY cal.calendar_date';

    -- Execute the query
    EXECUTE IMMEDIATE query;

    RETURN 'Successfully processed holidays into ' || output_table;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error: ' || SQLERRM;
END;
$$;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- STEP 5C: Create stored procedure for Gregorian calendar
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SPG_DAP01.PBI.SP_GENERATE_GREGORIAN_CALENDAR(
    start_date DATE,
    end_date DATE,
    date_grain STRING,
    timezone STRING,
    target_table STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    sql_command STRING;
    status STRING DEFAULT 'SUCCESS';
BEGIN
    -- Build the SQL to create/replace the target table
    sql_command := '
    CREATE OR REPLACE TABLE ' || target_table || ' AS
    WITH date_generator AS (
        SELECT calendar_date::DATE AS calendar_date
        FROM TABLE(SPG_DAP01.PBI.FN_GENERATE_DATE_SPINE(
            ''' || start_date || ''',
            ''' || end_date || ''',
            ''' || date_grain || ''',
            ''' || timezone || '''
        ))
    )
    
    SELECT
        dg.calendar_date                                      AS date,
        TO_NUMBER(TO_CHAR(dg.calendar_date, ''YYYYMMDD''))     AS date_key,
        YEAR(dg.calendar_date)                                AS year_num,
        ''CY'' || MOD(YEAR(dg.calendar_date), 100)::STRING     AS year_desc,
        QUARTER(dg.calendar_date)                             AS quarter_num,
        ''Q'' || QUARTER(dg.calendar_date)                      AS quarter_desc,
        YEAR(dg.calendar_date) * 10 + QUARTER(dg.calendar_date) AS year_quarter_key,
        MONTH(dg.calendar_date)                               AS month_num,
        MONTHNAME(dg.calendar_date)                           AS month_short_name,
        CASE MONTH(dg.calendar_date)
            WHEN 1 THEN ''January'' WHEN 2 THEN ''February'' WHEN 3 THEN ''March''
            WHEN 4 THEN ''April'' WHEN 5 THEN ''May'' WHEN 6 THEN ''June''
            WHEN 7 THEN ''July'' WHEN 8 THEN ''August'' WHEN 9 THEN ''September''
            WHEN 10 THEN ''October'' WHEN 11 THEN ''November'' WHEN 12 THEN ''December''
        END                                                    AS month_long_name,
        YEAR(dg.calendar_date) * 100 + MONTH(dg.calendar_date) AS year_month_key,
        CONCAT(MONTHNAME(dg.calendar_date), '' '', YEAR(dg.calendar_date)) AS month_year_desc,
        WEEKOFYEAR(dg.calendar_date)                          AS week_of_year_num,
        YEAROFWEEK(dg.calendar_date)                          AS year_of_week_num,
        CONCAT(YEAROFWEEK(dg.calendar_date), ''-W'', LPAD(WEEKOFYEAR(dg.calendar_date), 2, ''0'')) AS year_week_desc,
        WEEKISO(dg.calendar_date)                             AS iso_week_num,
        YEAROFWEEKISO(dg.calendar_date)                       AS iso_year_of_week_num,
        CONCAT(YEAROFWEEKISO(dg.calendar_date), ''-W'', LPAD(WEEKISO(dg.calendar_date), 2, ''0'')) AS iso_year_week_desc,
        DAY(dg.calendar_date)                                 AS day_of_month_num,
        DAYOFWEEK(dg.calendar_date)                           AS day_of_week_num, -- Sunday = 0
        DAYOFWEEKISO(dg.calendar_date)                        AS iso_day_of_week_num, -- Monday = 1
        DAYOFYEAR(dg.calendar_date)                           AS day_of_year_num,
        DAYNAME(dg.calendar_date)                             AS day_short_name,
        CASE DAYOFWEEK(dg.calendar_date)
            WHEN 0 THEN ''Sunday'' WHEN 1 THEN ''Monday'' WHEN 2 THEN ''Tuesday''
            WHEN 3 THEN ''Wednesday'' WHEN 4 THEN ''Thursday'' WHEN 5 THEN ''Friday''
            WHEN 6 THEN ''Saturday''
        END                                                    AS day_long_name,
        TO_CHAR(dg.calendar_date, ''DD Mon YYYY'')              AS date_full_desc,
        TO_CHAR(dg.calendar_date, ''DD/MM/YYYY'')               AS date_formatted,
        DATE_TRUNC(''MONTH'', dg.calendar_date)                 AS month_start_date,
        LAST_DAY(dg.calendar_date)                            AS month_end_date,
        DATE_TRUNC(''QUARTER'', dg.calendar_date)               AS quarter_start_date,
        LAST_DAY(dg.calendar_date, QUARTER)                   AS quarter_end_date,
        DATE_TRUNC(''YEAR'', dg.calendar_date)                  AS year_start_date,
        LAST_DAY(dg.calendar_date, YEAR)                      AS year_end_date,
        DATE_TRUNC(''WEEK'', dg.calendar_date)                  AS week_start_date,
        LAST_DAY(dg.calendar_date, WEEK)                      AS week_end_date,
        DATEDIFF(DAY, DATE_TRUNC(''MONTH'', dg.calendar_date), dg.calendar_date) + 1 AS day_of_month_count,
        DAY(LAST_DAY(dg.calendar_date))                       AS days_in_month_count,
        DATEDIFF(DAY, DATE_TRUNC(''QUARTER'', dg.calendar_date), dg.calendar_date) + 1 AS day_of_quarter_count,
        DATEDIFF(DAY, DATE_TRUNC(''QUARTER'', dg.calendar_date), LAST_DAY(dg.calendar_date, QUARTER)) + 1 AS days_in_quarter_count,
        CEIL(DAY(dg.calendar_date) / 7.0)                     AS week_of_month_num,
        CEIL((DATEDIFF(DAY, DATE_TRUNC(''QUARTER'', dg.calendar_date), dg.calendar_date) + 1) / 7.0) AS week_of_quarter_num,
        CASE WHEN DAYOFWEEKISO(dg.calendar_date) IN (6, 7) THEN 0 ELSE 1 END AS is_weekday, -- ISO Weekday 1-5
        CASE WHEN DAYOFWEEKISO(dg.calendar_date) IN (6, 7) THEN ''Weekend'' ELSE ''Weekday'' END AS weekday_indicator, -- ISO Weekend 6,7
        DATEADD(YEAR, -1, dg.calendar_date)                   AS same_date_last_year
    FROM date_generator dg
    ORDER BY date
    ';

    -- Execute the SQL
    BEGIN
        EXECUTE IMMEDIATE sql_command;
    EXCEPTION
        WHEN OTHER THEN
            status := 'ERROR: ' || SQLERRM;
    END;

    RETURN status;
END;
$$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- STEP 5D: Create stored procedure for Fiscal calendar
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SPG_DAP01.PBI.SP_GENERATE_AU_FISCAL_CALENDAR(
    base_calendar_table STRING,
    target_table STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    sql_command STRING;
    status STRING DEFAULT 'SUCCESS';
BEGIN
    -- Build the SQL to create/replace the target table
    sql_command := '
    CREATE OR REPLACE TABLE ' || target_table || ' AS
    WITH au_fiscal_base AS (
        SELECT
            date,
            year_num,
            month_num,
            iso_day_of_week_num,
            CASE WHEN month_num >= 7
                THEN DATE_FROM_PARTS(year_num, 7, 1)
                ELSE DATE_FROM_PARTS(year_num - 1, 7, 1)
            END AS au_fiscal_start_date_for_year,
            DATEADD(YEAR, 1, CASE WHEN month_num >= 7
                THEN DATE_FROM_PARTS(year_num, 7, 1)
                ELSE DATE_FROM_PARTS(year_num - 1, 7, 1)
            END) - 1 AS au_fiscal_end_date_for_year
        FROM ' || base_calendar_table || '
    ),
    au_fiscal_years AS (
        SELECT
            afb.date,
            afb.iso_day_of_week_num,
            afb.au_fiscal_start_date_for_year,
            afb.au_fiscal_end_date_for_year,
            YEAR(afb.au_fiscal_start_date_for_year) + 1 AS au_fiscal_year_num,
            ''FY'' || MOD(YEAR(afb.au_fiscal_start_date_for_year) + 1, 100)::STRING AS au_fiscal_year_desc
        FROM au_fiscal_base afb
    ),
    au_fiscal_details AS (
        SELECT
            afy.date,
            afy.iso_day_of_week_num,
            afy.au_fiscal_start_date_for_year,
            afy.au_fiscal_end_date_for_year,
            afy.au_fiscal_year_num,
            afy.au_fiscal_year_desc,
            DATEDIFF(QUARTER, afy.au_fiscal_start_date_for_year, afy.date) + 1 AS au_fiscal_quarter_num,
            afy.au_fiscal_year_num * 10 + (DATEDIFF(QUARTER, afy.au_fiscal_start_date_for_year, afy.date) + 1) AS au_fiscal_quarter_year_key,
            ''QTR '' || (DATEDIFF(QUARTER, afy.au_fiscal_start_date_for_year, afy.date) + 1)::STRING AS au_fiscal_quarter_desc,
            DATEADD(QUARTER, DATEDIFF(QUARTER, afy.au_fiscal_start_date_for_year, afy.date), afy.au_fiscal_start_date_for_year) AS au_fiscal_quarter_start_date,
            DATEADD(DAY, -1, DATEADD(QUARTER, 1, DATEADD(QUARTER, DATEDIFF(QUARTER, afy.au_fiscal_start_date_for_year, afy.date), afy.au_fiscal_start_date_for_year))) AS au_fiscal_quarter_end_date,
            MOD(DATEDIFF(MONTH, afy.au_fiscal_start_date_for_year, afy.date), 12) + 1 AS au_fiscal_month_num,
            afy.au_fiscal_year_num * 100 + (MOD(DATEDIFF(MONTH, afy.au_fiscal_start_date_for_year, afy.date), 12) + 1) AS au_fiscal_month_year_key,
            ''Month '' || LPAD((MOD(DATEDIFF(MONTH, afy.au_fiscal_start_date_for_year, afy.date), 12) + 1)::STRING, 2, ''0'') AS au_fiscal_month_desc,
            DATEADD(MONTH, DATEDIFF(MONTH, afy.au_fiscal_start_date_for_year, afy.date), afy.au_fiscal_start_date_for_year) AS au_fiscal_month_start_date,
            LAST_DAY(DATEADD(MONTH, DATEDIFF(MONTH, afy.au_fiscal_start_date_for_year, afy.date), afy.au_fiscal_start_date_for_year)) AS au_fiscal_month_end_date,
            FLOOR(DATEDIFF(DAY, afy.au_fiscal_start_date_for_year, afy.date) / 7) + 1 AS au_fiscal_week_num
        FROM au_fiscal_years afy
    ),
    cal_data AS (
        SELECT
            date,
            au_fiscal_year_num,
            au_fiscal_week_num,
            iso_day_of_week_num
        FROM au_fiscal_details
    ),
    business_day_mapping AS (
        SELECT
            curr.date,
            prev.date AS same_business_day_last_year
        FROM cal_data curr
        LEFT JOIN cal_data prev
            ON prev.au_fiscal_year_num = curr.au_fiscal_year_num - 1
            AND prev.au_fiscal_week_num = curr.au_fiscal_week_num
            AND prev.iso_day_of_week_num = curr.iso_day_of_week_num
    )
    SELECT
        afd.date,
        afd.au_fiscal_start_date_for_year,
        afd.au_fiscal_end_date_for_year,
        afd.au_fiscal_year_num,
        afd.au_fiscal_year_desc,
        afd.au_fiscal_quarter_num,
        afd.au_fiscal_quarter_year_key,
        afd.au_fiscal_quarter_desc,
        afd.au_fiscal_quarter_start_date,
        afd.au_fiscal_quarter_end_date,
        afd.au_fiscal_month_num,
        afd.au_fiscal_month_year_key,
        afd.au_fiscal_month_desc,
        afd.au_fiscal_month_start_date,
        afd.au_fiscal_month_end_date,
        afd.au_fiscal_week_num,
        bdm.same_business_day_last_year
    FROM au_fiscal_details afd
    LEFT JOIN business_day_mapping bdm ON afd.date = bdm.date
    ORDER BY date
    ';

    -- Execute the SQL
    BEGIN
        EXECUTE IMMEDIATE sql_command;
    EXCEPTION
        WHEN OTHER THEN
            status := 'ERROR: ' || SQLERRM;
    END;

    RETURN status;
END;
$$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- STEP 5E: Create stored procedure for Retail calendar (445,454,544)
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SPG_DAP01.PBI.SP_GENERATE_RETAIL_CALENDAR(
    base_calendar_table STRING,
    target_table STRING,
    retail_pattern STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    sql_command STRING;
    status STRING DEFAULT 'SUCCESS';
BEGIN
    -- Build the SQL to create/replace the target table
    sql_command := '
    CREATE OR REPLACE TABLE ' || target_table || ' AS
    WITH retail_year_markers AS (
        SELECT
            date,
            year_num,
            month_num,
            day_of_month_num,
            iso_day_of_week_num,
            CASE
                WHEN month_num = 7 AND iso_day_of_week_num = 1 AND day_of_month_num <= 7
                THEN 1
                ELSE 0
            END AS is_retail_soy_marker
        FROM ' || base_calendar_table || '
    ),
    retail_years AS (
        SELECT
            date AS retail_soy_date,
            year_num AS marker_year,
            LAST_VALUE(CASE WHEN is_retail_soy_marker = 1 THEN date ELSE NULL END IGNORE NULLS)
                OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as retail_start_of_year
        FROM retail_year_markers
    ),
    retail_years_processed AS (
        SELECT DISTINCT
            retail_start_of_year,
            COALESCE(
                DATEADD(DAY, -1, LEAD(retail_start_of_year) OVER (ORDER BY retail_start_of_year)),
                (SELECT MAX(DATEADD(DAY, 363, retail_start_of_year))
                 FROM retail_years fy_inner
                 WHERE fy_inner.retail_start_of_year = retail_years.retail_start_of_year)
            ) AS retail_end_of_year,
            YEAR(DATEADD(DAY, 363, retail_start_of_year)) AS retail_year_num,
            ''F'' || MOD(YEAR(DATEADD(DAY, 363, retail_start_of_year)), 100)::STRING AS retail_year_desc
        FROM retail_years
        WHERE retail_start_of_year IS NOT NULL
    ),
    retail_base AS (
        SELECT
            cal.date,
            fy.retail_start_of_year,
            fy.retail_end_of_year,
            fy.retail_year_num,
            fy.retail_year_desc,
            CASE
                WHEN cal.date >= fy.retail_start_of_year
                THEN FLOOR(DATEDIFF(DAY, fy.retail_start_of_year, cal.date) / 7) + 1
                ELSE NULL
            END AS retail_week_num
        FROM ' || base_calendar_table || ' cal
        LEFT JOIN retail_years_processed fy
            ON cal.date >= fy.retail_start_of_year
            AND cal.date <= fy.retail_end_of_year
    ),
    retail_detail AS (
        SELECT
            rb.date,
            rb.retail_start_of_year,
            rb.retail_end_of_year,
            rb.retail_year_num,
            rb.retail_year_desc,
            rb.retail_week_num,
            -- Period calculation varies based on selected pattern
            CASE
                WHEN ''' || retail_pattern || ''' = ''445'' THEN
                    CASE rb.retail_week_num
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
                    END
                WHEN ''' || retail_pattern || ''' = ''454'' THEN
                    CASE rb.retail_week_num
                        WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 3 THEN 1 WHEN 4 THEN 1 -- P1
                        WHEN 5 THEN 2 WHEN 6 THEN 2 WHEN 7 THEN 2 WHEN 8 THEN 2 WHEN 9 THEN 2 -- P2
                        WHEN 10 THEN 3 WHEN 11 THEN 3 WHEN 12 THEN 3 WHEN 13 THEN 3 -- P3
                        WHEN 14 THEN 4 WHEN 15 THEN 4 WHEN 16 THEN 4 WHEN 17 THEN 4 -- P4
                        WHEN 18 THEN 5 WHEN 19 THEN 5 WHEN 20 THEN 5 WHEN 21 THEN 5 WHEN 22 THEN 5 -- P5
                        WHEN 23 THEN 6 WHEN 24 THEN 6 WHEN 25 THEN 6 WHEN 26 THEN 6 -- P6
                        WHEN 27 THEN 7 WHEN 28 THEN 7 WHEN 29 THEN 7 WHEN 30 THEN 7 -- P7
                        WHEN 31 THEN 8 WHEN 32 THEN 8 WHEN 33 THEN 8 WHEN 34 THEN 8 WHEN 35 THEN 8 -- P8
                        WHEN 36 THEN 9 WHEN 37 THEN 9 WHEN 38 THEN 9 WHEN 39 THEN 9 -- P9
                        WHEN 40 THEN 10 WHEN 41 THEN 10 WHEN 42 THEN 10 WHEN 43 THEN 10 -- P10
                        WHEN 44 THEN 11 WHEN 45 THEN 11 WHEN 46 THEN 11 WHEN 47 THEN 11 WHEN 48 THEN 11 -- P11
                        WHEN 49 THEN 12 WHEN 50 THEN 12 WHEN 51 THEN 12 WHEN 52 THEN 12 -- P12
                        WHEN 53 THEN 12
                        ELSE NULL
                    END
                WHEN ''' || retail_pattern || ''' = ''544'' THEN
                    CASE rb.retail_week_num
                        WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 3 THEN 1 WHEN 4 THEN 1 WHEN 5 THEN 1 -- P1
                        WHEN 6 THEN 2 WHEN 7 THEN 2 WHEN 8 THEN 2 WHEN 9 THEN 2 -- P2
                        WHEN 10 THEN 3 WHEN 11 THEN 3 WHEN 12 THEN 3 WHEN 13 THEN 3 -- P3
                        WHEN 14 THEN 4 WHEN 15 THEN 4 WHEN 16 THEN 4 WHEN 17 THEN 4 WHEN 18 THEN 4 -- P4
                        WHEN 19 THEN 5 WHEN 20 THEN 5 WHEN 21 THEN 5 WHEN 22 THEN 5 -- P5
                        WHEN 23 THEN 6 WHEN 24 THEN 6 WHEN 25 THEN 6 WHEN 26 THEN 6 -- P6
                        WHEN 27 THEN 7 WHEN 28 THEN 7 WHEN 29 THEN 7 WHEN 30 THEN 7 WHEN 31 THEN 7 -- P7
                        WHEN 32 THEN 8 WHEN 33 THEN 8 WHEN 34 THEN 8 WHEN 35 THEN 8 -- P8
                        WHEN 36 THEN 9 WHEN 37 THEN 9 WHEN 38 THEN 9 WHEN 39 THEN 9 -- P9
                        WHEN 40 THEN 10 WHEN 41 THEN 10 WHEN 42 THEN 10 WHEN 43 THEN 10 WHEN 44 THEN 10 -- P10
                        WHEN 45 THEN 11 WHEN 46 THEN 11 WHEN 47 THEN 11 WHEN 48 THEN 11 -- P11
                        WHEN 49 THEN 12 WHEN 50 THEN 12 WHEN 51 THEN 12 WHEN 52 THEN 12 -- P12
                        WHEN 53 THEN 12
                        ELSE NULL
                    END
                ELSE -- Default to 445 if pattern not recognized
                    CASE rb.retail_week_num
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
                    END
            END AS retail_period_num
        FROM retail_base rb
        WHERE rb.retail_week_num IS NOT NULL
    ),
    retail_detail_extended AS (
        SELECT
            rd.date,
            rd.retail_start_of_year,
            rd.retail_end_of_year,
            rd.retail_year_num,
            rd.retail_year_desc,
            rd.retail_week_num,
            rd.retail_period_num,
            CASE
                WHEN (rd.retail_period_num) IN (1, 2, 3, 4, 5, 6) THEN 1
                WHEN (rd.retail_period_num) IN (7, 8, 9, 10, 11, 12) THEN 2
                ELSE NULL
            END AS retail_half_num,

            CASE
                WHEN (rd.retail_period_num) IN (1, 2, 3) THEN 1
                WHEN (rd.retail_period_num) IN (4, 5, 6) THEN 2
                WHEN (rd.retail_period_num) IN (7, 8, 9) THEN 3
                WHEN (rd.retail_period_num) IN (10, 11, 12) THEN 4
                ELSE NULL
            END AS retail_quarter_num,

            ''HALF '' || retail_half_num::STRING AS retail_half_desc,
            ''QTR '' || retail_quarter_num::STRING AS retail_quarter_desc,
            rd.retail_year_num * 10 + retail_quarter_num AS retail_quarter_year_key,
            ''MONTH '' || retail_period_num::STRING AS retail_period_desc,
            rd.retail_year_num * 100 + retail_period_num AS retail_year_month_key,

            CASE retail_period_num
                WHEN 1 THEN ''Jul'' WHEN 2 THEN ''Aug'' WHEN 3 THEN ''Sep'' WHEN 4 THEN ''Oct''
                WHEN 5 THEN ''Nov'' WHEN 6 THEN ''Dec'' WHEN 7 THEN ''Jan'' WHEN 8 THEN ''Feb''
                WHEN 9 THEN ''Mar'' WHEN 10 THEN ''Apr'' WHEN 11 THEN ''May'' WHEN 12 THEN ''Jun''
            END AS retail_month_short_name,

            CASE retail_period_num
                WHEN 1 THEN ''July'' WHEN 2 THEN ''August'' WHEN 3 THEN ''September'' WHEN 4 THEN ''October''
                WHEN 5 THEN ''November'' WHEN 6 THEN ''December'' WHEN 7 THEN ''January'' WHEN 8 THEN ''February''
                WHEN 9 THEN ''March'' WHEN 10 THEN ''April'' WHEN 11 THEN ''May'' WHEN 12 THEN ''June''
            END AS retail_month_long_name,

            CONCAT(retail_month_short_name, '' '', rd.retail_year_num::STRING) AS retail_month_year_desc,
            CONCAT(retail_month_long_name, '' '', rd.retail_year_num::STRING) AS retail_month_full_year_desc
        FROM retail_detail rd
    )

    SELECT
        date,
        retail_start_of_year,
        retail_end_of_year,
        retail_year_num,
        retail_year_desc,
        retail_week_num,
        retail_half_num,
        retail_half_desc,
        retail_quarter_num,
        retail_quarter_desc,
        retail_quarter_year_key,
        retail_period_num,
        retail_period_desc,
        retail_year_month_key,
        retail_month_short_name,
        retail_month_long_name,
        retail_month_year_desc,
        retail_month_full_year_desc
    FROM retail_detail_extended
    ORDER BY date
    ';

    -- Execute the SQL
    BEGIN
        EXECUTE IMMEDIATE sql_command;
    EXCEPTION
        WHEN OTHER THEN
            status := 'ERROR: ' || SQLERRM;
    END;

    RETURN status;
END;
$$;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 6: Create helper functions, procedures, and the configuration-driven business calendar system
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 1. Create helper function for managing columns
CREATE OR REPLACE FUNCTION SPG_DAP01.PBI.FN_ENSURE_COLUMNS_EXIST(
    table_name STRING,
    column_definitions ARRAY
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    columns_to_add ARRAY DEFAULT [];
    col_def OBJECT;
    col_exists BOOLEAN;
    alter_sql STRING;
BEGIN
    -- Check which columns need to be added
    FOR col_def IN (SELECT VALUE FROM (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(FLATTEN(input => column_definitions))))
    DO
        col_exists := (SYSTEM$TYPEOF(col_def:name || ' in ' || table_name) != 'UNDEFINED');
        IF NOT col_exists THEN
            columns_to_add := ARRAY_APPEND(columns_to_add, col_def);
        END IF;
    END FOR;

    -- Nothing to add
    IF ARRAY_SIZE(columns_to_add) = 0 THEN
        RETURN 'All columns already exist';
    END IF;

    -- Build ALTER TABLE statement
    alter_sql := 'ALTER TABLE ' || table_name || ' ADD ';
    FOR i IN 0 TO ARRAY_SIZE(columns_to_add)-1 DO
        col_def := columns_to_add[i];
        alter_sql := alter_sql || col_def:name || ' ' || col_def:type;
        IF i < ARRAY_SIZE(columns_to_add)-1 THEN
            alter_sql := alter_sql || ', ';
        END IF;
    END FOR;

    -- Execute ALTER TABLE
    EXECUTE IMMEDIATE alter_sql;

    RETURN 'Added ' || ARRAY_SIZE(columns_to_add) || ' columns';
END;
$$;

-- 2. Create helper procedure for processing calendar modules
CREATE OR REPLACE PROCEDURE SPG_DAP01.PBI.PR_PROCESS_CALENDAR_MODULE(
    module_name STRING,
    temp_table_name STRING,
    function_name STRING,
    function_parameters OBJECT,
    base_table_name STRING,
    column_mapping ARRAY
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    function_call STRING;
    update_sql STRING;
    column_definitions ARRAY;
    result STRING;
BEGIN
    -- Build function call to create temp table
    function_call := 'CREATE OR REPLACE TEMPORARY TABLE ' || temp_table_name || ' AS SELECT * FROM TABLE(' ||
                     function_name || '(';

    -- Add function parameters
    FOR i IN 0 TO ARRAY_SIZE(OBJECT_KEYS(function_parameters))-1 DO
        IF i > 0 THEN
            function_call := function_call || ', ';
        END IF;
        function_call := function_call || function_parameters[OBJECT_KEYS(function_parameters)[i]];
    END FOR;

    function_call := function_call || '))';

    -- Execute function to create temp table
    EXECUTE IMMEDIATE function_call;

    -- Extract column definitions for ensuring columns exist
    column_definitions := [];
    FOR mapping IN (SELECT VALUE FROM (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(FLATTEN(input => column_mapping))))
    DO
        column_definitions := ARRAY_APPEND(column_definitions,
            OBJECT_CONSTRUCT('name', mapping:target_column, 'type', mapping:type));
    END FOR;

    -- Ensure columns exist in base table
    result := SPG_DAP01.PBI.FN_ENSURE_COLUMNS_EXIST(base_table_name, column_definitions);

    -- Build update SQL
    update_sql := 'UPDATE ' || base_table_name || ' b SET ';

    FOR i IN 0 TO ARRAY_SIZE(column_mapping)-1 DO
        IF i > 0 THEN
            update_sql := update_sql || ', ';
        END IF;
        update_sql := update_sql || column_mapping[i]:target_column || ' = t.' || column_mapping[i]:source_column;
    END FOR;

    update_sql := update_sql || ' FROM ' || temp_table_name || ' t WHERE b.date = t.date';

    -- Execute update
    EXECUTE IMMEDIATE update_sql;

    -- Drop temp table
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || temp_table_name;

    RETURN 'Successfully processed ' || module_name || ' calendar module';
END;
$$;

-- 3. Create holidays processing procedure
CREATE OR REPLACE PROCEDURE SPG_DAP01.PBI.PR_PROCESS_HOLIDAYS(
    date_spine_table STRING,
    target_table STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result STRING;
BEGIN
    -- Create temporary table with holiday data
    CREATE TEMPORARY TABLE temp_holidays AS
    SELECT * FROM TABLE(SPG_DAP01.PBI.FN_PROCESS_HOLIDAYS(:date_spine_table));

    -- Define column definitions for holiday flags
    result := SPG_DAP01.PBI.FN_ENSURE_COLUMNS_EXIST(:target_table, ARRAY_CONSTRUCT(
        OBJECT_CONSTRUCT('name', 'is_holiday', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'holiday_indicator', 'type', 'STRING'),
        OBJECT_CONSTRUCT('name', 'holiday_desc', 'type', 'STRING'),
        OBJECT_CONSTRUCT('name', 'is_holiday_nsw', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'is_holiday_vic', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'is_holiday_qld', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'is_holiday_sa', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'is_holiday_wa', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'is_holiday_tas', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'is_holiday_act', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'is_holiday_nt', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'is_holiday_national', 'type', 'NUMBER(1,0)')
    ));

    -- Update the main table with holiday data
    EXECUTE IMMEDIATE 'UPDATE ' || :target_table || ' b
    SET
        is_holiday = h.is_holiday,
        holiday_indicator = h.holiday_indicator,
        holiday_desc = h.holiday_desc,
        is_holiday_nsw = h.holiday_metadata:is_holiday_nsw,
        is_holiday_vic = h.holiday_metadata:is_holiday_vic,
        is_holiday_qld = h.holiday_metadata:is_holiday_qld,
        is_holiday_sa = h.holiday_metadata:is_holiday_sa,
        is_holiday_wa = h.holiday_metadata:is_holiday_wa,
        is_holiday_tas = h.holiday_metadata:is_holiday_tas,
        is_holiday_act = h.holiday_metadata:is_holiday_act,
        is_holiday_nt = h.holiday_metadata:is_holiday_nt,
        is_holiday_national = h.holiday_metadata:is_holiday_national
    FROM temp_holidays h
    WHERE b.date = h.date';

    -- Drop temporary table
    DROP TABLE IF EXISTS temp_holidays;

    RETURN 'Successfully processed holiday data';
END;
$$;

-- 4. Create trading days processing procedure
CREATE OR REPLACE PROCEDURE SPG_DAP01.PBI.PR_PROCESS_TRADING_DAYS(
    date_spine_table STRING,
    target_table STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result STRING;
BEGIN
    -- Create temporary table with trading day calculations
    CREATE TEMPORARY TABLE temp_trading_days AS
    WITH trading_day_calendar AS (
        SELECT
            date,
            year_month_key,
            year_quarter_key,
            CASE WHEN is_weekday = 1 AND is_holiday = 0 THEN 1 ELSE 0 END AS is_trading_day,
            CASE
                WHEN is_weekday = 0 THEN 'Weekend'
                WHEN is_holiday = 1 THEN 'Holiday'
                ELSE 'Trading Day'
            END AS trading_day_desc,
            ROW_NUMBER() OVER (PARTITION BY year_month_key ORDER BY date) AS day_of_month_seq,
            SUM(CASE WHEN is_weekday = 1 AND is_holiday = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY year_month_key ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS trading_day_of_month_seq,
            CASE WHEN date = month_start_date THEN 1 ELSE 0 END AS is_first_day_of_month,
            CASE WHEN date = month_end_date THEN 1 ELSE 0 END AS is_last_day_of_month,
            CASE WHEN date = quarter_start_date THEN 1 ELSE 0 END AS is_first_day_of_quarter,
            CASE WHEN date = quarter_end_date THEN 1 ELSE 0 END AS is_last_day_of_quarter,
            LEAD(date) OVER (ORDER BY date) AS next_date,
            LAG(date) OVER (ORDER BY date) AS previous_date,
            LAG(CASE WHEN is_weekday = 1 AND is_holiday = 0 THEN date END) OVER (ORDER BY date) AS previous_trading_date,
            LEAD(CASE WHEN is_weekday = 1 AND is_holiday = 0 THEN date END) OVER (ORDER BY date) AS next_trading_date,
            SUM(CASE WHEN is_weekday = 1 AND is_holiday = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY year_month_key) AS trading_days_in_month,
            SUM(CASE WHEN is_weekday = 1 AND is_holiday = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY year_quarter_key) AS trading_days_in_quarter
        FROM identifier(:target_table)
    )
    SELECT
        tdc.*,
        -- Add fiscal calendar trading days if fiscal calendar exists
        CASE WHEN EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = UPPER(:target_table) AND column_name = 'AU_FISCAL_MONTH_YEAR_KEY') THEN
            SUM(CASE WHEN tdc.is_weekday = 1 AND tdc.is_holiday = 0 THEN 1 ELSE 0 END)
                OVER (PARTITION BY b.au_fiscal_month_year_key)
        ELSE NULL END AS trading_days_in_au_fiscal_month,

        CASE WHEN EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = UPPER(:target_table) AND column_name = 'AU_FISCAL_QUARTER_YEAR_KEY') THEN
            SUM(CASE WHEN tdc.is_weekday = 1 AND tdc.is_holiday = 0 THEN 1 ELSE 0 END)
                OVER (PARTITION BY b.au_fiscal_quarter_year_key)
        ELSE NULL END AS trading_days_in_au_fiscal_quarter,

        -- Add retail calendar trading days if retail calendar exists
        CASE WHEN EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = UPPER(:target_table) AND column_name = 'RETAIL_YEAR_MONTH_KEY') THEN
            SUM(CASE WHEN tdc.is_weekday = 1 AND tdc.is_holiday = 0 THEN 1 ELSE 0 END)
                OVER (PARTITION BY b.retail_year_month_key)
        ELSE NULL END AS trading_days_in_retail_period,

        CASE WHEN EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = UPPER(:target_table) AND column_name = 'RETAIL_QUARTER_YEAR_KEY') THEN
            SUM(CASE WHEN tdc.is_weekday = 1 AND tdc.is_holiday = 0 THEN 1 ELSE 0 END)
                OVER (PARTITION BY b.retail_quarter_year_key)
        ELSE NULL END AS trading_days_in_retail_quarter
    FROM trading_day_calendar tdc
    JOIN identifier(:target_table) b ON tdc.date = b.date;

    -- Define column definitions for trading days
    result := SPG_DAP01.PBI.FN_ENSURE_COLUMNS_EXIST(:target_table, ARRAY_CONSTRUCT(
        OBJECT_CONSTRUCT('name', 'is_trading_day', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'trading_day_desc', 'type', 'STRING'),
        OBJECT_CONSTRUCT('name', 'day_of_month_seq', 'type', 'NUMBER'),
        OBJECT_CONSTRUCT('name', 'trading_day_of_month_seq', 'type', 'NUMBER'),
        OBJECT_CONSTRUCT('name', 'is_first_day_of_month', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'is_last_day_of_month', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'is_first_day_of_quarter', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'is_last_day_of_quarter', 'type', 'NUMBER(1,0)'),
        OBJECT_CONSTRUCT('name', 'next_date', 'type', 'DATE'),
        OBJECT_CONSTRUCT('name', 'previous_date', 'type', 'DATE'),
        OBJECT_CONSTRUCT('name', 'next_trading_date', 'type', 'DATE'),
        OBJECT_CONSTRUCT('name', 'previous_trading_date', 'type', 'DATE'),
        OBJECT_CONSTRUCT('name', 'trading_days_in_month', 'type', 'NUMBER'),
        OBJECT_CONSTRUCT('name', 'trading_days_in_quarter', 'type', 'NUMBER'),
        OBJECT_CONSTRUCT('name', 'trading_days_in_au_fiscal_month', 'type', 'NUMBER'),
        OBJECT_CONSTRUCT('name', 'trading_days_in_au_fiscal_quarter', 'type', 'NUMBER'),
        OBJECT_CONSTRUCT('name', 'trading_days_in_retail_period', 'type', 'NUMBER'),
        OBJECT_CONSTRUCT('name', 'trading_days_in_retail_quarter', 'type', 'NUMBER')
    ));

    -- Update the main table with trading day data
    EXECUTE IMMEDIATE 'UPDATE ' || :target_table || ' b
    SET
        is_trading_day = t.is_trading_day,
        trading_day_desc = t.trading_day_desc,
        day_of_month_seq = t.day_of_month_seq,
        trading_day_of_month_seq = t.trading_day_of_month_seq,
        is_first_day_of_month = t.is_first_day_of_month,
        is_last_day_of_month = t.is_last_day_of_month,
        is_first_day_of_quarter = t.is_first_day_of_quarter,
        is_last_day_of_quarter = t.is_last_day_of_quarter,
        next_date = t.next_date,
        previous_date = t.previous_date,
        next_trading_date = t.next_trading_date,
        previous_trading_date = t.previous_trading_date,
        trading_days_in_month = t.trading_days_in_month,
        trading_days_in_quarter = t.trading_days_in_quarter,
        trading_days_in_au_fiscal_month = t.trading_days_in_au_fiscal_month,
        trading_days_in_au_fiscal_quarter = t.trading_days_in_au_fiscal_quarter,
        trading_days_in_retail_period = t.trading_days_in_retail_period,
        trading_days_in_retail_quarter = t.trading_days_in_retail_quarter
    FROM temp_trading_days t
    WHERE b.date = t.date';

    -- Drop temporary table
    DROP TABLE IF EXISTS temp_trading_days;

    RETURN 'Successfully processed trading day data';
END;
$$;

-- 5. Create retail seasons processing procedure
CREATE OR REPLACE PROCEDURE SPG_DAP01.PBI.PR_PROCESS_RETAIL_SEASONS(
    target_table STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result STRING;
BEGIN
    -- Create temporary table with retail season calculations
    CREATE TEMPORARY TABLE temp_retail_seasons AS
    WITH good_fridays AS (
         SELECT year_num, MIN(date) as good_friday_date
         FROM identifier(:target_table)
         WHERE is_holiday = 1 AND month_num IN (3, 4) AND day_long_name = 'Friday'
         GROUP BY year_num
    ),
    retail_seasons AS (
        SELECT
            bc.date, gf.good_friday_date,
            CASE
                WHEN (bc.month_num = 11) OR (bc.month_num = 12 AND bc.day_of_month_num <= 24) THEN 'Christmas Season'
                WHEN (bc.month_num = 1 AND bc.day_of_month_num >= 15) OR (bc.month_num = 2 AND bc.day_of_month_num <= 15) THEN 'Back to School'
                WHEN gf.good_friday_date IS NOT NULL AND bc.date BETWEEN DATEADD(DAY, -21, gf.good_friday_date) AND DATEADD(DAY, 1, gf.good_friday_date) THEN 'Easter Season'
                WHEN bc.month_num = 6 THEN 'EOFY Sales'
                ELSE 'Regular Season'
            END AS retail_season,
            CASE
                WHEN bc.month_num = 12 AND bc.day_of_month_num BETWEEN 20 AND 24 THEN 'Christmas Eve Period'
                WHEN bc.month_num = 12 AND bc.day_of_month_num = 26 THEN 'Boxing Day'
                WHEN bc.month_num = 12 AND bc.day_of_month_num BETWEEN 27 AND 31 THEN 'Post-Christmas Sale'
                ELSE NULL
            END AS holiday_proximity
        FROM identifier(:target_table) bc
        LEFT JOIN good_fridays gf ON bc.year_num = gf.year_num
    );

    -- Define column definitions for retail seasons
    result := SPG_DAP01.PBI.FN_ENSURE_COLUMNS_EXIST(:target_table, ARRAY_CONSTRUCT(
        OBJECT_CONSTRUCT('name', 'retail_season', 'type', 'STRING'),
        OBJECT_CONSTRUCT('name', 'holiday_proximity', 'type', 'STRING')
    ));

    -- Update the main table with retail season data
    EXECUTE IMMEDIATE 'UPDATE ' || :target_table || ' b
    SET
        retail_season = rs.retail_season,
        holiday_proximity = rs.holiday_proximity
    FROM temp_retail_seasons rs
    WHERE b.date = rs.date';

    -- Drop temporary table
    DROP TABLE IF EXISTS temp_retail_seasons;

    RETURN 'Successfully processed retail season data';
END;
$$;

-- 6. Create calendar view generation procedure
CREATE OR REPLACE PROCEDURE SPG_DAP01.PBI.PR_CREATE_CALENDAR_VIEW(
    base_table STRING,
    view_name STRING,
    components ARRAY
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    view_sql STRING;
BEGIN
    -- Define the dynamic calendar view SQL
    view_sql := '
    CREATE OR REPLACE VIEW ' || view_name || '
    AS
    WITH current_values AS (
        -- Pre-calculate values based on CURRENT_DATE() once for efficiency in the view
        SELECT
            CURRENT_DATE() AS current_date_val,
            DATE_TRUNC(''MONTH'', CURRENT_DATE()) AS current_month_start,
            DATE_TRUNC(''QUARTER'', CURRENT_DATE()) AS current_quarter_start,
            DATE_TRUNC(''YEAR'', CURRENT_DATE()) AS current_year_start,
            (SELECT MAX(b.year_month_key) FROM ' || base_table || ' b WHERE b.date < DATE_TRUNC(''MONTH'', CURRENT_DATE())) AS prev_month_key,
            (SELECT MAX(b.year_quarter_key) FROM ' || base_table || ' b WHERE b.date < DATE_TRUNC(''QUARTER'', CURRENT_DATE())) AS prev_quarter_key';

    -- Add AU Fiscal calendar current values if included
    IF ARRAY_CONTAINS('AU_FISCAL'::VARIANT, components) THEN
        view_sql := view_sql || ',
            (SELECT b.au_fiscal_year_num FROM ' || base_table || ' b WHERE b.date = CURRENT_DATE()) AS current_au_fiscal_year_num,
            (SELECT b.au_fiscal_start_date_for_year FROM ' || base_table || ' b WHERE b.date = CURRENT_DATE()) AS current_au_fiscal_start_date';
    END IF;

    -- Add Retail calendar current values if included
    IF ARRAY_CONTAINS('RETAIL'::VARIANT, components) THEN
        view_sql := view_sql || ',
            (SELECT b.retail_year_num FROM ' || base_table || ' b WHERE b.date = CURRENT_DATE()) AS current_retail_year_num,
            (SELECT b.retail_start_of_year FROM ' || base_table || ' b WHERE b.date = CURRENT_DATE()) AS current_retail_start_of_year';
    END IF;

    -- Complete the WITH clause
    view_sql := view_sql || '
    )
    SELECT
        -- Basic Gregorian calendar columns (always included)
        base.date,
        base.date_key,
        base.year_num,
        base.year_desc,
        base.quarter_num,
        base.quarter_desc,
        base.year_quarter_key,
        base.quarter_num AS quarter_sort_key,
        base.month_num,
        base.month_short_name,
        base.month_long_name,
        base.year_month_key,
        base.month_year_desc,
        base.month_num AS month_sort_key,
        base.week_of_year_num,
        base.year_of_week_num,
        base.year_week_desc,
        base.year_of_week_num * 100 + base.week_of_year_num AS week_sort_key,
        base.iso_week_num,
        base.iso_year_of_week_num,
        base.iso_year_week_desc,
        base.day_of_month_num,
        base.day_of_week_num,
        base.iso_day_of_week_num,
        base.day_of_year_num,
        base.day_short_name,
        base.day_long_name,
        base.iso_day_of_week_num AS day_of_week_sort_key,
        base.date_full_desc,
        base.date_formatted,
        base.month_start_date,
        base.month_end_date,
        base.quarter_start_date,
        base.quarter_end_date,
        base.year_start_date,
        base.year_end_date,
        base.week_start_date,
        base.week_end_date,
        base.day_of_month_count,
        base.days_in_month_count,
        base.day_of_quarter_count,
        base.days_in_quarter_count,
        base.week_of_month_num,
        base.week_of_quarter_num,
        base.is_weekday,
        base.weekday_indicator,
        base.is_holiday,
        base.holiday_indicator,
        base.holiday_desc,
        base.is_holiday_nsw,
        base.is_holiday_vic,
        base.is_holiday_qld,
        base.is_holiday_sa,
        base.is_holiday_wa,
        base.is_holiday_tas,
        base.is_holiday_act,
        base.is_holiday_nt,
        base.is_holiday_national,
        base.is_trading_day,
        base.trading_day_desc,
        base.same_date_last_year,

        -- Basic trading day columns
        base.day_of_month_seq,
        base.trading_day_of_month_seq,
        base.is_first_day_of_month,
        base.is_last_day_of_month,
        base.is_first_day_of_quarter,
        base.is_last_day_of_quarter,
        base.next_date,
        base.previous_date,
        base.next_trading_date,
        base.previous_trading_date,
        base.trading_days_in_month,
        base.trading_days_in_quarter';

    -- Add AU Fiscal calendar columns if included
    IF ARRAY_CONTAINS('AU_FISCAL'::VARIANT, components) THEN
        view_sql := view_sql || ',
        -- AU Fiscal calendar columns
        base.au_fiscal_year_num,
        base.au_fiscal_year_desc,
        base.au_fiscal_quarter_num,
        base.au_fiscal_quarter_desc,
        base.au_fiscal_quarter_year_key,
        base.au_fiscal_quarter_num AS au_fiscal_quarter_sort_key,
        base.au_fiscal_month_num,
        base.au_fiscal_month_desc,
        base.au_fiscal_month_year_key,
        base.au_fiscal_month_num AS au_fiscal_month_sort_key,
        base.au_fiscal_week_num,
        base.au_fiscal_start_date_for_year,
        base.au_fiscal_end_date_for_year,
        base.au_fiscal_quarter_start_date,
        base.au_fiscal_quarter_end_date,
        base.au_fiscal_month_start_date,
        base.au_fiscal_month_end_date,
        base.same_business_day_last_year,
        base.trading_days_in_au_fiscal_month,
        base.trading_days_in_au_fiscal_quarter';
    END IF;

    -- Add Retail calendar columns if included
    IF ARRAY_CONTAINS('RETAIL'::VARIANT, components) THEN
        view_sql := view_sql || ',
        -- Retail calendar columns
        base.retail_year_num,
        base.retail_year_desc,
        base.retail_half_num,
        base.retail_half_desc,
        base.retail_half_num AS retail_half_sort_key,
        base.retail_quarter_num,
        base.retail_quarter_desc,
        base.retail_quarter_year_key,
        base.retail_quarter_num AS retail_quarter_sort_key,
        base.retail_period_num,
        base.retail_period_desc,
        base.retail_year_month_key,
        base.retail_period_num AS retail_period_sort_key,
        base.retail_month_short_name,
        base.retail_month_long_name,
        base.retail_month_year_desc,
        base.retail_month_full_year_desc,
        base.retail_week_num,
        base.retail_week_num AS retail_week_sort_key,
        base.retail_start_of_year,
        base.retail_end_of_year,
        base.trading_days_in_retail_period,
        base.trading_days_in_retail_quarter';
    END IF;

    -- Add retail season columns
    view_sql := view_sql || ',
        -- Retail season columns
        base.retail_season,
        CASE
            WHEN base.retail_season = ''Christmas Season'' THEN 1
            WHEN base.retail_season = ''Back to School'' THEN 2
            WHEN base.retail_season = ''Easter Season'' THEN 3
            WHEN base.retail_season = ''EOFY Sales'' THEN 4
            WHEN base.retail_season = ''Regular Season'' THEN 5
            ELSE 9
        END AS retail_season_sort_key,
        base.holiday_proximity,
        CASE
            WHEN base.holiday_proximity = ''Christmas Eve Period'' THEN 1
            WHEN base.holiday_proximity = ''Boxing Day'' THEN 2
            WHEN base.holiday_proximity = ''Post-Christmas Sale'' THEN 3
            ELSE 9
        END AS holiday_proximity_sort_key';

    -- Add dynamic relative time flags
    view_sql := view_sql || ',
        -- Dynamic relative time flags
        CASE WHEN base.date = cv.current_date_val THEN 1 ELSE 0 END AS is_current_date,
        CASE WHEN base.year_month_key = TO_NUMBER(TO_CHAR(cv.current_date_val, ''YYYYMM'')) THEN 1 ELSE 0 END AS is_current_month,
        CASE WHEN base.year_quarter_key = (YEAR(cv.current_date_val) * 10 + QUARTER(cv.current_date_val)) THEN 1 ELSE 0 END AS is_current_quarter,
        CASE WHEN base.year_num = YEAR(cv.current_date_val) THEN 1 ELSE 0 END AS is_current_year';

    -- Add fiscal dynamic flags if fiscal calendar exists
    IF ARRAY_CONTAINS('AU_FISCAL'::VARIANT, components) THEN
        view_sql := view_sql || ',
        CASE WHEN base.au_fiscal_year_num = cv.current_au_fiscal_year_num THEN 1 ELSE 0 END AS is_current_fiscal_year';
    END IF;

    -- Add retail dynamic flags if retail calendar exists
    IF ARRAY_CONTAINS('RETAIL'::VARIANT, components) THEN
        view_sql := view_sql || ',
        CASE WHEN base.retail_year_num = cv.current_retail_year_num THEN 1 ELSE 0 END AS is_current_retail_year';
    END IF;

    -- Add other dynamic flags
    view_sql := view_sql || ',
        CASE WHEN base.date BETWEEN DATEADD(DAY, -6, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_last_7_days,
        CASE WHEN base.date BETWEEN DATEADD(DAY, -29, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_last_30_days,
CASE WHEN base.date BETWEEN DATEADD(DAY, -89, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_last_90_days,
        CASE WHEN base.year_month_key = cv.prev_month_key THEN 1 ELSE 0 END AS is_previous_month,
        CASE WHEN base.year_quarter_key = cv.prev_quarter_key THEN 1 ELSE 0 END AS is_previous_quarter,
        CASE WHEN base.date BETWEEN DATEADD(MONTH, -12, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_rolling_12_months,
        CASE WHEN base.date BETWEEN DATEADD(MONTH, -3, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_rolling_quarter,
        CASE WHEN base.year_num = YEAR(cv.current_date_val) AND base.date BETWEEN cv.current_year_start AND cv.current_date_val THEN 1 ELSE 0 END AS is_year_to_date'';

    -- Add fiscal YTD if fiscal calendar exists
    IF ARRAY_CONTAINS(''AU_FISCAL''::VARIANT, components) THEN
        view_sql := view_sql || '',
        CASE WHEN base.au_fiscal_year_num = cv.current_au_fiscal_year_num AND base.date BETWEEN cv.current_au_fiscal_start_date AND cv.current_date_val THEN 1 ELSE 0 END AS is_fiscal_year_to_date'';
    END IF;

    -- Add retail YTD if retail calendar exists
    IF ARRAY_CONTAINS(''RETAIL''::VARIANT, components) THEN
        view_sql := view_sql || '',
        CASE WHEN base.retail_year_num = cv.current_retail_year_num AND base.date BETWEEN cv.current_retail_start_of_year AND cv.current_date_val THEN 1 ELSE 0 END AS is_retail_year_to_date'';
    END IF;

    -- Add standard time-to-date flags
    view_sql := view_sql || '',
        CASE WHEN base.year_quarter_key = (YEAR(cv.current_date_val) * 10 + QUARTER(cv.current_date_val)) AND base.date BETWEEN cv.current_quarter_start AND cv.current_date_val THEN 1 ELSE 0 END AS is_quarter_to_date,
        CASE WHEN base.year_month_key = TO_NUMBER(TO_CHAR(cv.current_date_val, ''''YYYYMM'''')) AND base.date BETWEEN cv.current_month_start AND cv.current_date_val THEN 1 ELSE 0 END AS is_month_to_date

    FROM '' || base_table || '' base
    CROSS JOIN current_values cv';

    -- Execute view creation
    EXECUTE IMMEDIATE view_sql;

    -- Add comment on view (with proper quote escaping)
    EXECUTE IMMEDIATE ''COMMENT ON VIEW '' || view_name || '' IS
    ''''Comprehensive business calendar with dynamically calculated relative time flags.
    Generated by SP_BUILD_BUSINESS_CALENDAR with modular component-based processing.
    Supports Gregorian, AU Fiscal, and Retail calendars with proper sort keys for BI tools.'''''';

    RETURN ''Successfully created view '' || view_name;
END;

$$;

-- 7. Create configuration-driven business calendar stored procedure
create or replace procedure SPG_DAP01.PBI.SP_BUILD_BUSINESS_CALENDAR(
  START_DATE DATE default '' 2015-01-01 '',
  END_DATE DATE default '' 2035-12-31 '',
  DATE_GRAIN STRING default '' day '',
  TIMEZONE STRING default '' Australia/
Adelaide'',
    include_gregorian boolean default true,
    include_au_fiscal boolean default true,
    include_retail_calendar boolean default true,
    retail_calendar_type string default ''
445''
)
returns string
language sql
as
$$
DECLARE
    status_message STRING DEFAULT ''SUCCESS'';
    current_run_ts TIMESTAMP_LTZ(9);
    components ARRAY;
BEGIN
    -- Set the session timezone
    EXECUTE IMMEDIATE ''ALTER SESSION SET TIMEZONE = '''''' || timezone || '''''''';

    -- Get current timestamp after timezone is set
    current_run_ts := CURRENT_TIMESTAMP();

    -- Determine which components to include
    components := ARRAY_CONSTRUCT(''GREGORIAN'');
    IF include_au_fiscal THEN
        components := ARRAY_APPEND(components, ''AU_FISCAL'');
    END IF;
    IF include_retail_calendar THEN
        components := ARRAY_APPEND(components, ''RETAIL'');
    END IF;

    -- 1. Generate the base Gregorian calendar
    CREATE OR REPLACE TABLE SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE AS
    SELECT * FROM TABLE(SPG_DAP01.PBI.FN_GENERATE_GREGORIAN_CALENDAR(
        :start_date, :end_date, :date_grain, :timezone
    ));

    -- 2. Create a temp table to store this calendar for processing by other functions
    CREATE OR REPLACE TEMPORARY TABLE calendar_base_dates AS
    SELECT date AS calendar_date
    FROM SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE;

    -- 3. Apply holiday processing to add holiday flags
    CALL SPG_DAP01.PBI.PR_PROCESS_HOLIDAYS(''calendar_base_dates'', ''SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE'');

    -- 4. Add AU Fiscal calendar if requested
    IF (include_au_fiscal) THEN
        -- Create temporary table with AU Fiscal data
        CREATE TEMPORARY TABLE temp_au_fiscal AS
        SELECT * FROM TABLE(SPG_DAP01.PBI.FN_GENERATE_AU_FISCAL_CALENDAR(
            ''calendar_base_dates''
        ));

        -- Add AU Fiscal columns if they don''t exist
        CALL SPG_DAP01.PBI.FN_ENSURE_COLUMNS_EXIST(''SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE'',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_start_date_for_year'', ''type'', ''DATE''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_end_date_for_year'', ''type'', ''DATE''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_year_num'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_year_desc'', ''type'', ''STRING''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_quarter_num'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_quarter_year_key'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_quarter_desc'', ''type'', ''STRING''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_quarter_start_date'', ''type'', ''DATE''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_quarter_end_date'', ''type'', ''DATE''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_month_num'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_month_year_key'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_month_desc'', ''type'', ''STRING''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_month_start_date'', ''type'', ''DATE''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_month_end_date'', ''type'', ''DATE''),
                OBJECT_CONSTRUCT(''name'', ''au_fiscal_week_num'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''same_business_day_last_year'', ''type'', ''DATE'')
            )
        );

        -- Update with AU fiscal calendar details
        UPDATE SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE b
        SET
            au_fiscal_start_date_for_year = f.au_fiscal_start_date_for_year,
            au_fiscal_end_date_for_year = f.au_fiscal_end_date_for_year,
            au_fiscal_year_num = f.au_fiscal_year_num,
            au_fiscal_year_desc = f.au_fiscal_year_desc,
            au_fiscal_quarter_num = f.au_fiscal_quarter_num,
            au_fiscal_quarter_year_key = f.au_fiscal_quarter_year_key,
            au_fiscal_quarter_desc = f.au_fiscal_quarter_desc,
            au_fiscal_quarter_start_date = f.au_fiscal_quarter_start_date,
            au_fiscal_quarter_end_date = f.au_fiscal_quarter_end_date,
            au_fiscal_month_num = f.au_fiscal_month_num,
            au_fiscal_month_year_key = f.au_fiscal_month_year_key,
            au_fiscal_month_desc = f.au_fiscal_month_desc,
            au_fiscal_month_start_date = f.au_fiscal_month_start_date,
            au_fiscal_month_end_date = f.au_fiscal_month_end_date,
            au_fiscal_week_num = f.au_fiscal_week_num,
            same_business_day_last_year = f.same_business_day_last_year
        FROM temp_au_fiscal f
        WHERE b.date = f.date;

        -- Drop the temporary table
        DROP TABLE IF EXISTS temp_au_fiscal;
    END IF;

    -- 5. Add Retail calendar if requested
    IF (include_retail_calendar) THEN
        -- Generate Retail calendar based on selected pattern
        CREATE TEMPORARY TABLE temp_retail AS
        SELECT * FROM TABLE(SPG_DAP01.PBI.FN_GENERATE_RETAIL_CALENDAR(
            ''calendar_base_dates'',
            :retail_calendar_type
        ));

        -- Add the retail calendar columns if they don''t exist
        CALL SPG_DAP01.PBI.FN_ENSURE_COLUMNS_EXIST(''SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE'',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(''name'', ''retail_start_of_year'', ''type'', ''DATE''),
                OBJECT_CONSTRUCT(''name'', ''retail_end_of_year'', ''type'', ''DATE''),
                OBJECT_CONSTRUCT(''name'', ''retail_year_num'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''retail_year_desc'', ''type'', ''STRING''),
                OBJECT_CONSTRUCT(''name'', ''retail_week_num'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''retail_half_num'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''retail_half_desc'', ''type'', ''STRING''),
                OBJECT_CONSTRUCT(''name'', ''retail_quarter_num'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''retail_quarter_desc'', ''type'', ''STRING''),
                OBJECT_CONSTRUCT(''name'', ''retail_quarter_year_key'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''retail_period_num'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''retail_period_desc'', ''type'', ''STRING''),
                OBJECT_CONSTRUCT(''name'', ''retail_year_month_key'', ''type'', ''NUMBER''),
                OBJECT_CONSTRUCT(''name'', ''retail_month_short_name'', ''type'', ''STRING''),
                OBJECT_CONSTRUCT(''name'', ''retail_month_long_name'', ''type'', ''STRING''),
                OBJECT_CONSTRUCT(''name'', ''retail_month_year_desc'', ''type'', ''STRING''),
                OBJECT_CONSTRUCT(''name'', ''retail_month_full_year_desc'', ''type'', ''STRING'')
            )
        );

        -- Update with retail calendar details
        UPDATE SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE b
        SET
            retail_start_of_year = r.retail_start_of_year,
            retail_end_of_year = r.retail_end_of_year,
            retail_year_num = r.retail_year_num,
            retail_year_desc = r.retail_year_desc,
            retail_week_num = r.retail_week_num,
            retail_half_num = r.retail_half_num,
            retail_half_desc = r.retail_half_desc,
            retail_quarter_num = r.retail_quarter_num,
            retail_quarter_desc = r.retail_quarter_desc,
            retail_quarter_year_key = r.retail_quarter_year_key,
            retail_period_num = r.retail_period_num,
            retail_period_desc = r.retail_period_desc,
            retail_year_month_key = r.retail_year_month_key,
            retail_month_short_name = r.retail_month_short_name,
            retail_month_long_name = r.retail_month_long_name,
            retail_month_year_desc = r.retail_month_year_desc,
            retail_month_full_year_desc = r.retail_month_full_year_desc
        FROM temp_retail r
        WHERE b.date = r.date;

        -- Drop the temporary table
        DROP TABLE IF EXISTS temp_retail;
    END IF;

    -- 6. Calculate trading days
    CALL SPG_DAP01.PBI.PR_PROCESS_TRADING_DAYS(''calendar_base_dates'', ''SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE'');

    -- 7. Add retail seasons
    CALL SPG_DAP01.PBI.PR_PROCESS_RETAIL_SEASONS(''SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE'');

    -- 8. Add metadata
    CALL SPG_DAP01.PBI.FN_ENSURE_COLUMNS_EXIST(''SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE'',
        ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT(''name'', ''dw_created_ts'', ''type'', ''TIMESTAMP_LTZ''),
            OBJECT_CONSTRUCT(''name'', ''dw_source_system'', ''type'', ''STRING''),
            OBJECT_CONSTRUCT(''name'', ''dw_version_desc'', ''type'', ''STRING'')
        )
    );

    UPDATE SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE
    SET
        dw_created_ts = :current_run_ts,
        dw_source_system = ''SP_BUILD_BUSINESS_CALENDAR'',
        dw_version_desc = ''v3.0 - Component-based calendar generation'';

    -- 9. Add clustering
    ALTER TABLE SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE CLUSTER BY (date);

    -- 10. Create the dynamic calendar view with relative time flags
    CALL SPG_DAP01.PBI.PR_CREATE_CALENDAR_VIEW(
        ''SPG_DAP01.PBI.BUSINESS_CALENDAR_BASE'',
        ''SPG_DAP01.PBI.BUSINESS_CALENDAR'',
        components
    );

    -- 11. Clean up temporary tables
    DROP TABLE IF EXISTS calendar_base_dates;

    -- 12. Return status message with information about which calendars were included
    LET included_calendars STRING :=
        CASE WHEN :include_gregorian THEN ''Gregorian'' ELSE '''' END ||
        CASE WHEN :include_au_fiscal THEN (CASE WHEN :include_gregorian THEN '', '' ELSE '''' END) || ''AU Fiscal'' ELSE '''' END ||
        CASE WHEN :include_retail_calendar THEN
            (CASE WHEN :include_gregorian OR :include_au_fiscal THEN '', '' ELSE '''' END) ||
            ''Retail '' || :retail_calendar_type
        ELSE '''' END;

    status_message := ''SUCCESS: Calendar built with '' || included_calendars || '' calendars. Date range: '' ||
                     :start_date::STRING || '' to '' || :end_date::STRING;
    RETURN status_message;

EXCEPTION
    WHEN OTHER THEN
        status_message := ''ERROR: '' || SQLERRM;
        RETURN status_message;
END;
$$;

-- Add comment to the stored procedure
comment on procedure SPG_DAP01.PBI.SP_BUILD_BUSINESS_CALENDAR(DATE, DATE, STRING, STRING, BOOLEAN, BOOLEAN, BOOLEAN, STRING) is
  'Builds a comprehensive business calendar that supports Gregorian, Australian Fiscal Year (July-June),
and Retail 4-4-5 calendars. The procedure creates a base static calendar table and a dynamic view
with RELATIVE time flags.Use the parameters to customize which calendar types to include, date range, timezone, and retail calendar pattern.
  This improved version uses helper functions and procedures to simplify maintenance and extension:
  - Helper functions for column management
  - Component-based processing of different calendar types
  - Modular design
with CLEAR separation of concerns
  - Improved error handling and reporting

call THIS procedure periodically to refresh the calendar, especially after adding new holiday data
  or when extending the date range is needed.';

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 7: Call stored procedure to build the Business Calendar
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CALL SPG_DAP01.PBI.SP_BUILD_BUSINESS_CALENDAR();

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
7. Holiday flags for each Australian jurisdiction (NSW, VIC, QLD, SA, WA, TAS, ACT, NT, National).
8. Holiday description column that combines holiday names with relevant jurisdictions.

Update Frequency: Infrequent (e.g., yearly) or when holiday/fiscal structure changes.
Query Interface: Use the BUSINESS_CALENDAR view for analysis.
Version: v1.5';

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
    quarter_sort_key COMMENT 'Sort key for quarters (1-4) that ensures proper ordering in BI tools.',
    month_num COMMENT 'Calendar month number (1-12).',
    month_short_name COMMENT 'Abbreviated calendar month name (e.g., Jan).',
    month_long_name COMMENT 'Full calendar month name (e.g., January).',
    year_month_key COMMENT 'Integer key for calendar year and month (YYYYMM).',
    month_year_desc COMMENT 'Calendar month and year description (e.g., Jan 2024).',
    month_sort_key COMMENT 'Sort key for months (1-12) that ensures proper ordering in BI tools.',
    week_of_year_num COMMENT 'Week number within the calendar year (behavior depends on WEEK_START session parameter).',
    year_of_week_num COMMENT 'Year number associated with week_of_year_num (depends on WEEK_START).',
    year_week_desc COMMENT 'Year and week description (depends on WEEK_START).',
    week_sort_key COMMENT 'Sort key for weeks that ensures proper ordering by week number in BI tools.',
    iso_week_num COMMENT 'ISO 8601 week number within the ISO year (Week starts Monday).',
    iso_year_of_week_num COMMENT 'ISO 8601 year number associated with the ISO week.',
    iso_year_week_desc COMMENT 'ISO 8601 year and week description.',
    day_of_month_num COMMENT 'Day number within the calendar month (1-31).',
    day_of_week_num COMMENT 'Day number within the week (0=Sunday, 1=Monday, ..., 6=Saturday). Behavior depends on WEEK_START session parameter.',
    iso_day_of_week_num COMMENT 'ISO 8601 day number within the week (1=Monday, ..., 7=Sunday).',
    day_of_year_num COMMENT 'Day number within the calendar year (1-366).',
    day_short_name COMMENT 'Abbreviated day name (e.g., Mon).',
    day_long_name COMMENT 'Full day name (e.g., Monday).',
    day_of_week_sort_key COMMENT 'Sort key for days of week that ensures proper ordering (1-7, where 1=Monday following ISO standard).',
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
    is_holiday COMMENT 'Indicator (1/0) if the day exists in the AU_PUBLIC_HOLIDAYS table for any jurisdiction.',
    holiday_indicator COMMENT 'Description (Holiday/Non-Holiday) based on is_holiday.',
    holiday_desc COMMENT 'Combined holiday names and jurisdictions for this date, if a holiday.',

    -- Jurisdiction-specific holiday flags
    is_holiday_nsw COMMENT 'Indicator (1/0) if the day is a holiday in New South Wales.',
    is_holiday_vic COMMENT 'Indicator (1/0) if the day is a holiday in Victoria.',
    is_holiday_qld COMMENT 'Indicator (1/0) if the day is a holiday in Queensland.',
    is_holiday_sa COMMENT 'Indicator (1/0) if the day is a holiday in South Australia.',
    is_holiday_wa COMMENT 'Indicator (1/0) if the day is a holiday in Western Australia.',
    is_holiday_tas COMMENT 'Indicator (1/0) if the day is a holiday in Tasmania.',
    is_holiday_act COMMENT 'Indicator (1/0) if the day is a holiday in Australian Capital Territory.',
    is_holiday_nt COMMENT 'Indicator (1/0) if the day is a holiday in Northern Territory.',
    is_holiday_national COMMENT 'Indicator (1/0) if the day is a national holiday in Australia.',

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
    au_fiscal_quarter_sort_key COMMENT 'Sort key for AU fiscal quarters (1-4) that ensures proper ordering in BI tools.',
    au_fiscal_month_num COMMENT 'Month number within the Australian Fiscal Year (1-12, where 1 = July).',
    au_fiscal_month_desc COMMENT 'Australian Fiscal Month description (e.g., Month 01 for July).',
    au_fiscal_month_year_key COMMENT 'Integer key for AU fiscal year and month number (YYYYMM, where YYYY is fiscal year number, MM is fiscal month 1-12).',
    au_fiscal_month_sort_key COMMENT 'Sort key for AU fiscal months (1-12) that ensures proper ordering in BI tools.',
    au_fiscal_week_num COMMENT 'Sequential week number within the Australian Fiscal Year (starting from 1). Simple calculation based on days since FY start.',
    au_fiscal_start_date_for_year COMMENT 'Start date (July 1) of the AU Fiscal Year this date belongs to.',
    au_fiscal_end_date_for_year COMMENT 'End date (June 30) of the AU Fiscal Year this date belongs to.',
    au_fiscal_quarter_start_date COMMENT 'Start date of the AU Fiscal Quarter this date belongs to.',
    au_fiscal_quarter_end_date COMMENT 'End date of the AU Fiscal Quarter this date belongs to.',
    au_fiscal_month_start_date COMMENT 'Start date of the AU Fiscal Month (within the fiscal year) this date belongs to.',
    au_fiscal_month_end_date COMMENT 'End date of the AU Fiscal Month (within the fiscal year) this date belongs to.',

    -- Retail 4-4-5 calendar columns
    f445_year_num COMMENT 'Retail 4-4-5 Year number, designated by the calendar year it ends in (assumption: starts first Monday of July).',
    f445_year_desc COMMENT 'Retail 4-4-5 Year description (e.g., F24).',
    f445_half_num COMMENT 'Half number within the Retail 4-4-5 Year (1 or 2).',
    f445_half_desc COMMENT 'Retail 4-4-5 Half description (e.g., HALF 1).',
    f445_half_sort_key COMMENT 'Sort key for F445 half-years (1-2) that ensures proper ordering in BI tools.',
    f445_quarter_num COMMENT 'Quarter number within the Retail 4-4-5 Year (1-4), based on 4-4-5 period groupings.',
    f445_quarter_desc COMMENT 'Retail 4-4-5 Quarter description (e.g., QTR 1).',
    f445_quarter_year_key COMMENT 'Integer key for F445 year and quarter (YYYYQ, where YYYY is F445 year number).',
    f445_quarter_sort_key COMMENT 'Sort key for F445 quarters (1-4) that ensures proper ordering in BI tools.',
    f445_period_num COMMENT 'Period number (month equivalent) within the Retail 4-4-5 Year (1-12), following a 4-4-5 week pattern.',
    f445_period_desc COMMENT 'Retail 4-4-5 Period description (e.g., MONTH 1).',
    f445_year_month_key COMMENT 'Integer key for F445 year and period number (YYYYPP, where YYYY is F445 year number, PP is period 1-12).',
    f445_period_sort_key COMMENT 'Sort key for F445 periods (1-12) that ensures proper ordering in BI tools.',
    f445_month_short_name COMMENT 'Equivalent calendar month name (approx) for the F445 period (e.g., Jul for Period 1).',
    f445_month_long_name COMMENT 'Full equivalent calendar month name (approx) for the F445 period (e.g., July for Period 1).',
    f445_month_year_desc COMMENT 'F445 equivalent month and year description (e.g., Jul F24).',
    f445_month_full_year_desc COMMENT 'F445 equivalent full month and year description (e.g., July F24).',
    f445_week_num COMMENT 'Sequential week number within the Retail 4-4-5 Year (starting from 1).',
    f445_week_sort_key COMMENT 'Sort key for F445 weeks that ensures proper ordering in BI tools.',
    f445_start_of_year COMMENT 'Start date of the F445 Year this date belongs to (assumed first Monday of July).',
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
    retail_season_sort_key COMMENT 'Sort key for retail seasons to ensure proper ordering in BI tools.',
    holiday_proximity COMMENT 'Identifies specific periods near major holidays: ''Christmas Eve Period'' (Dec 20-24), ''Boxing Day'' (Dec 26), ''Post-Christmas Sale'' (Dec 27-31). NULL otherwise.',
    holiday_proximity_sort_key COMMENT 'Sort key for holiday proximity periods to ensure proper ordering in BI tools.',

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
    -- Select columns from the base table
    base.date,
    base.date_key,
    base.year_num,
    base.year_desc,
    base.quarter_num,
    base.quarter_desc,
    base.year_quarter_key,
    base.quarter_num AS quarter_sort_key,
    base.month_num,
    base.month_short_name,
    base.month_long_name,
    base.year_month_key,
    base.month_year_desc,
    base.month_num AS month_sort_key,
    base.week_of_year_num,
    base.year_of_week_num,
    base.year_week_desc,
    base.year_of_week_num * 100 + base.week_of_year_num AS week_sort_key,
    base.iso_week_num,
    base.iso_year_of_week_num,
    base.iso_year_week_desc,
    base.day_of_month_num,
    base.day_of_week_num,
    base.iso_day_of_week_num,
    base.day_of_year_num,
    base.day_short_name,
    base.day_long_name,
    base.iso_day_of_week_num AS day_of_week_sort_key,
    base.date_full_desc,
    base.date_formatted,
    base.month_start_date,
    base.month_end_date,
    base.quarter_start_date,
    base.quarter_end_date,
    base.year_start_date,
    base.year_end_date,
    base.week_start_date,
    base.week_end_date,
    base.day_of_month_count,
    base.days_in_month_count,
    base.day_of_quarter_count,
    base.days_in_quarter_count,
    base.week_of_month_num,
    base.week_of_quarter_num,

    -- Business indicator columns
    base.is_weekday,
    base.weekday_indicator,
    base.is_holiday,
    base.holiday_indicator,
    base.holiday_desc,

    -- Jurisdiction-specific holiday flags
    base.is_holiday_nsw,
    base.is_holiday_vic,
    base.is_holiday_qld,
    base.is_holiday_sa,
    base.is_holiday_wa,
    base.is_holiday_tas,
    base.is_holiday_act,
    base.is_holiday_nt,
    base.is_holiday_national,

    base.is_trading_day,
    base.trading_day_desc,

    -- YoY comparison columns
    base.same_date_last_year,
    base.same_business_day_last_year,

    -- Australian fiscal calendar columns
    base.au_fiscal_year_num,
    base.au_fiscal_year_desc,
    base.au_fiscal_quarter_num,
    base.au_fiscal_quarter_desc,
    base.au_fiscal_quarter_year_key,
    base.au_fiscal_quarter_num AS au_fiscal_quarter_sort_key,
    base.au_fiscal_month_num,
    base.au_fiscal_month_desc,
    base.au_fiscal_month_year_key,
    base.au_fiscal_month_num AS au_fiscal_month_sort_key,
    base.au_fiscal_week_num,
    base.au_fiscal_start_date_for_year,
    base.au_fiscal_end_date_for_year,
    base.au_fiscal_quarter_start_date,
    base.au_fiscal_quarter_end_date,
    base.au_fiscal_month_start_date,
    base.au_fiscal_month_end_date,

    -- Retail 4-4-5 calendar columns
    base.f445_year_num,
    base.f445_year_desc,
    base.f445_half_num,
    base.f445_half_desc,
    base.f445_half_num AS f445_half_sort_key,
    base.f445_quarter_num,
    base.f445_quarter_desc,
    base.f445_quarter_year_key,
    base.f445_quarter_num AS f445_quarter_sort_key,
    base.f445_period_num,
    base.f445_period_desc,
    base.f445_year_month_key,
    base.f445_period_num AS f445_period_sort_key,
    base.f445_month_short_name,
    base.f445_month_long_name,
    base.f445_month_year_desc,
    base.f445_month_full_year_desc,
    base.f445_week_num,
    base.f445_week_num AS f445_week_sort_key,
    base.f445_start_of_year,
    base.f445_end_of_year,

    -- Trading day sequence & count columns
    base.day_of_month_seq,
    base.trading_day_of_month_seq,
    base.is_first_day_of_month,
    base.is_last_day_of_month,
    base.is_first_day_of_quarter,
    base.is_last_day_of_quarter,
    base.next_date,
    base.previous_date,
    base.next_trading_date,
    base.previous_trading_date,
    base.trading_days_in_month,
    base.trading_days_in_quarter,
    base.trading_days_in_au_fiscal_month,
    base.trading_days_in_au_fiscal_quarter,
    base.trading_days_in_f445_period,
    base.trading_days_in_f445_quarter,

    -- Retail season columns with sort keys
    base.retail_season,
    CASE
        WHEN base.retail_season = 'Christmas Season' THEN 1
        WHEN base.retail_season = 'Back to School' THEN 2
        WHEN base.retail_season = 'Easter Season' THEN 3
        WHEN base.retail_season = 'EOFY Sales' THEN 4
        WHEN base.retail_season = 'Regular Season' THEN 5
        ELSE 9
    END AS retail_season_sort_key,
    base.holiday_proximity,
    CASE
        WHEN base.holiday_proximity = 'Christmas Eve Period' THEN 1
        WHEN base.holiday_proximity = 'Boxing Day' THEN 2
        WHEN base.holiday_proximity = 'Post-Christmas Sale' THEN 3
        ELSE 9
    END AS holiday_proximity_sort_key,

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
COMMENT ON VIEW SPG_DAP01.PBI.BUSINESS_CALENDAR IS 'View providing a comprehensive calendar dimension by combining the static BUSINESS_CALENDAR_BASE table with dynamically calculated relative time period flags (e.g., is_current_month, is_last_7_days) and sort order columns for BI tools. Use this view for all reporting and analysis. Relative flags are always up-to-date based on CURRENT_DATE() at query time. Includes jurisdiction-specific holiday flags for all Australian states and territories.';

END;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- STEP 9: Create calendar comparison functions as User-Defined Functions (UDFs)
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 1. Business Day Navigation Functions
-- Get next business day after a given date
CREATE OR REPLACE FUNCTION FN_NEXT_BUSINESS_DAY(input_date DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
    SELECT MIN(date)
    FROM BUSINESS_CALENDAR
    WHERE date > input_date
    AND is_trading_day = 1
$$;

-- Get previous business day before a given date
CREATE OR REPLACE FUNCTION FN_PREVIOUS_BUSINESS_DAY(input_date DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
    SELECT MAX(date)
    FROM BUSINESS_CALENDAR
    WHERE date < input_date
    AND is_trading_day = 1
$$;

-- Add N business days to a given date
CREATE OR REPLACE FUNCTION FN_ADD_BUSINESS_DAYS(input_date DATE, days_to_add INT)
RETURNS DATE
LANGUAGE SQL
AS
$$
    DECLARE
        result_date DATE;
        remaining_days INT := ABS(days_to_add);
        direction INT := CASE WHEN days_to_add >= 0 THEN 1 ELSE -1 END;
        current_date DATE := input_date;
    BEGIN
        WHILE remaining_days > 0 DO
            -- Move one calendar day in the appropriate direction
            current_date := DATEADD(DAY, direction, current_date);

            -- Check if it's a business day
            IF EXISTS (
                SELECT 1 FROM BUSINESS_CALENDAR
                WHERE date = current_date
                AND is_trading_day = 1
            ) THEN
                remaining_days := remaining_days - 1;
            END IF;
        END WHILE;

        RETURN current_date;
    END;
$$;

-- 2. Business Day Counting Functions
-- Count business days between two dates (inclusive)
CREATE OR REPLACE FUNCTION FN_BUSINESS_DAYS_BETWEEN(start_date DATE, end_date DATE)
RETURNS INT
LANGUAGE SQL
AS
$$
    DECLARE
        biz_days INT;
    BEGIN
        SELECT COUNT(*)
        INTO biz_days
        FROM BUSINESS_CALENDAR
        WHERE date BETWEEN start_date AND end_date
        AND is_trading_day = 1;

        RETURN biz_days;
    END;
$$;

-- Get the Nth business day of a month
CREATE OR REPLACE FUNCTION FN_NTH_BUSINESS_DAY_OF_MONTH(year_num INT, month_num INT, n INT)
RETURNS DATE
LANGUAGE SQL
AS
$$
    SELECT date
    FROM BUSINESS_CALENDAR
    WHERE year_num = year_num
    AND month_num = month_num
    AND is_trading_day = 1
    ORDER BY date
    LIMIT 1 OFFSET (n-1);
$$;

-- Get the Nth last business day of a month
CREATE OR REPLACE FUNCTION FN_NTH_LAST_BUSINESS_DAY_OF_MONTH(year_num INT, month_num INT, n INT)
RETURNS DATE
LANGUAGE SQL
AS
$$
    SELECT date
    FROM BUSINESS_CALENDAR
    WHERE year_num = year_num
    AND month_num = month_num
    AND is_trading_day = 1
    ORDER BY date DESC
    LIMIT 1 OFFSET (n-1);
$$;

-- 3. Fiscal Calendar Functions
-- Convert Gregorian date to Australian Fiscal Year date
CREATE OR REPLACE FUNCTION FN_CONVERT_TO_AU_FISCAL_DATE(input_date DATE)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
    SELECT OBJECT_CONSTRUCT(
        'au_fiscal_year_num', au_fiscal_year_num,
        'au_fiscal_quarter_num', au_fiscal_quarter_num,
        'au_fiscal_month_num', au_fiscal_month_num,
        'au_fiscal_week_num', au_fiscal_week_num
    )
    FROM BUSINESS_CALENDAR
    WHERE date = input_date;
$$;

-- Get equivalent date in previous Fiscal Year
CREATE OR REPLACE FUNCTION FN_SAME_DAY_PREV_FISCAL_YEAR(input_date DATE, n_years INT DEFAULT 1)
RETURNS DATE
LANGUAGE SQL
AS
$$
    SELECT date
    FROM BUSINESS_CALENDAR
    WHERE au_fiscal_month_num = (
        SELECT au_fiscal_month_num FROM BUSINESS_CALENDAR WHERE date = input_date
    )
    AND au_fiscal_year_num = (
        SELECT au_fiscal_year_num - n_years FROM BUSINESS_CALENDAR WHERE date = input_date
    )
    AND day_of_month_num = (
        SELECT day_of_month_num FROM BUSINESS_CALENDAR WHERE date = input_date
    )
    ORDER BY date
    LIMIT 1;
$$;

-- 4. Season Detection Functions
-- Determine if a date falls within a specific retail season
CREATE OR REPLACE FUNCTION FN_IS_IN_RETAIL_SEASON(input_date DATE, season_name STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    SELECT
        CASE WHEN EXISTS (
            SELECT 1
            FROM BUSINESS_CALENDAR
            WHERE date = input_date
            AND retail_season = season_name
        )
        THEN TRUE
        ELSE FALSE
        END
$$;

-- Get all dates in a specific retail season for a given year
CREATE OR REPLACE FUNCTION FN_GET_RETAIL_SEASON_DATES(year_num INT, season_name STRING)
RETURNS TABLE(dates DATE)
LANGUAGE SQL
AS
$$
    SELECT date
    FROM BUSINESS_CALENDAR
    WHERE year_num = year_num
    AND retail_season = season_name
    ORDER BY date
$$;
