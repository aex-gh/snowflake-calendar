-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 1: Setup network rules to allow access to data.gov.au
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE NETWORK RULE allow_data_gov_au MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('data.gov.au:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION apis_access_integration
  ALLOWED_NETWORK_RULES = (allow_data_gov_au) -- Specifies the network rules that this integration uses to control network access.
  ENABLED = true; -- Enables the external access integration.  Must be true for it to function.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 2: Setup SP_LOAD_AU_HOLIDAYS(DATABASE_NAME, SCHEMA_NAME) stored procedure
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
            INFORMATION STRING,
            MORE_INFORMATION STRING,
            JURISDICTION STRING,
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

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 3: Create a view to store the public holidays and allow semantic modification for business requirements
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_CREATE_HOLIDAYS_VIEW(DATABASE_NAME STRING, SCHEMA_NAME STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.AU_PUBLIC_HOLIDAYS_VW AS
  SELECT
      HOLIDAY_DATE as date,
      HOLIDAY_NAME as holiday_name,
      JURISDICTION as state
  FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.AU_PUBLIC_HOLIDAYS';

  RETURN 'Successfully created AU_PUBLIC_HOLIDAYS_VW in ' || DATABASE_NAME || '.' || SCHEMA_NAME;
EXCEPTION
  WHEN OTHER THEN
    RETURN 'Error creating view: ' || SQLERRM;
END;
$$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- STEP 4: Create function for date spine
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION FN_GENERATE_DATE_SPINE(
    start_date DATE,
    end_date DATE,
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
-- STEP 5A: Create stored procedure for holidays
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_PROCESS_HOLIDAYS(
    calendar_table STRING,
    output_table STRING,
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING
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
        LEFT JOIN ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.AU_PUBLIC_HOLIDAYS_VW h ON cal.calendar_date = h.DATE
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
-- STEP 5B: Create stored procedure for Gregorian calendar
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_GENERATE_GREGORIAN_CALENDAR(
    start_date DATE,
    end_date DATE,
    date_grain STRING,
    timezone STRING,
    target_table STRING,
    is_temporary BOOLEAN DEFAULT FALSE
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    sql_command STRING;
    table_type STRING := IFF(is_temporary, 'TEMPORARY TABLE', 'TABLE');
    status STRING DEFAULT 'SUCCESS';
    function_prefix STRING := (SELECT DATABASE()) || '.' || (SELECT SCHEMA());
BEGIN
    -- Build the SQL to create/replace the target table
    sql_command := '
    CREATE OR REPLACE ' || table_type || ' ' || target_table || ' AS
    WITH date_generator AS (
        SELECT calendar_date::DATE AS calendar_date
        FROM TABLE(' || function_prefix || '.FN_GENERATE_DATE_SPINE(
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
-- STEP 5C: Create stored procedure for Fiscal calendar
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_GENERATE_AU_FISCAL_CALENDAR(
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
-- STEP 5D: Create stored procedure for Retail calendar (445,454,544)
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_GENERATE_RETAIL_CALENDAR(
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
-- STEP 5E: Create procedure for refreshing dynamic calendar flags
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_REFRESH_CALENDAR_FLAGS(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    INCLUDE_AU_FISCAL BOOLEAN DEFAULT TRUE,
    INCLUDE_RETAIL BOOLEAN DEFAULT TRUE
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    fully_qualified_prefix STRING := DATABASE_NAME || '.' || SCHEMA_NAME;
    base_table STRING := fully_qualified_prefix || '.BUSINESS_CALENDAR_BASE';
    flags_table STRING := fully_qualified_prefix || '.CALENDAR_DYNAMIC_FLAGS';
    status_message STRING DEFAULT 'SUCCESS';
BEGIN
    -- Create the dynamic flags table with current date-based calculations
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || flags_table || ' AS
    WITH current_values AS (
        -- Pre-calculate values based on CURRENT_DATE() once for efficiency
        SELECT
            CURRENT_DATE() AS current_date_val,
            DATE_TRUNC(''MONTH'', CURRENT_DATE()) AS current_month_start,
            DATE_TRUNC(''QUARTER'', CURRENT_DATE()) AS current_quarter_start,
            DATE_TRUNC(''YEAR'', CURRENT_DATE()) AS current_year_start,
            (SELECT MAX(b.year_month_key) FROM ' || base_table || ' b
             WHERE b.date < DATE_TRUNC(''MONTH'', CURRENT_DATE())) AS prev_month_key,
            (SELECT MAX(b.year_quarter_key) FROM ' || base_table || ' b
             WHERE b.date < DATE_TRUNC(''QUARTER'', CURRENT_DATE())) AS prev_quarter_key' ||

            -- Conditionally include AU Fiscal values if requested
            CASE WHEN INCLUDE_AU_FISCAL THEN ',
            (SELECT b.au_fiscal_year_num FROM ' || base_table || ' b
             WHERE b.date = CURRENT_DATE()) AS current_au_fiscal_year_num,
            (SELECT b.au_fiscal_start_date_for_year FROM ' || base_table || ' b
             WHERE b.date = CURRENT_DATE()) AS current_au_fiscal_start_date'
            ELSE '' END ||

            -- Conditionally include Retail values if requested
            CASE WHEN INCLUDE_RETAIL THEN ',
            (SELECT b.retail_year_num FROM ' || base_table || ' b
             WHERE b.date = CURRENT_DATE()) AS current_retail_year_num,
            (SELECT b.retail_start_of_year FROM ' || base_table || ' b
             WHERE b.date = CURRENT_DATE()) AS current_retail_start_of_year'
            ELSE '' END || '
    )

    SELECT
        base.date,
        -- Basic time flags
        CASE WHEN base.date = cv.current_date_val THEN 1 ELSE 0 END AS is_current_date,
        CASE WHEN base.year_month_key = TO_NUMBER(TO_CHAR(cv.current_date_val, ''YYYYMM'')) THEN 1 ELSE 0 END AS is_current_month,
        CASE WHEN base.year_quarter_key = (YEAR(cv.current_date_val) * 10 + QUARTER(cv.current_date_val)) THEN 1 ELSE 0 END AS is_current_quarter,
        CASE WHEN base.year_num = YEAR(cv.current_date_val) THEN 1 ELSE 0 END AS is_current_year,' ||

        -- AU Fiscal dynamic flags (if included)
        CASE WHEN INCLUDE_AU_FISCAL THEN '
        CASE WHEN base.au_fiscal_year_num = cv.current_au_fiscal_year_num THEN 1 ELSE 0 END AS is_current_fiscal_year,'
        ELSE '' END ||

        -- Retail dynamic flags (if included)
        CASE WHEN INCLUDE_RETAIL THEN '
        CASE WHEN base.retail_year_num = cv.current_retail_year_num THEN 1 ELSE 0 END AS is_current_retail_year,'
        ELSE '' END || '

        CASE WHEN base.date BETWEEN DATEADD(DAY, -6, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_last_7_days,
        CASE WHEN base.date BETWEEN DATEADD(DAY, -29, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_last_30_days,
        CASE WHEN base.date BETWEEN DATEADD(DAY, -89, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_last_90_days,
        CASE WHEN base.year_month_key = cv.prev_month_key THEN 1 ELSE 0 END AS is_previous_month,
        CASE WHEN base.year_quarter_key = cv.prev_quarter_key THEN 1 ELSE 0 END AS is_previous_quarter,
        CASE WHEN base.date BETWEEN DATEADD(MONTH, -12, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_rolling_12_months,
        CASE WHEN base.date BETWEEN DATEADD(MONTH, -3, cv.current_date_val) AND cv.current_date_val THEN 1 ELSE 0 END AS is_rolling_quarter,
        CASE WHEN base.year_num = YEAR(cv.current_date_val) AND base.date BETWEEN cv.current_year_start AND cv.current_date_val THEN 1 ELSE 0 END AS is_year_to_date' ||

        -- AU Fiscal YTD (if included)
        CASE WHEN INCLUDE_AU_FISCAL THEN ',
        CASE WHEN base.au_fiscal_year_num = cv.current_au_fiscal_year_num AND base.date BETWEEN cv.current_au_fiscal_start_date AND cv.current_date_val THEN 1 ELSE 0 END AS is_fiscal_year_to_date'
        ELSE '' END ||

        -- Retail YTD (if included)
        CASE WHEN INCLUDE_RETAIL THEN ',
        CASE WHEN base.retail_year_num = cv.current_retail_year_num AND base.date BETWEEN cv.current_retail_start_of_year AND cv.current_date_val THEN 1 ELSE 0 END AS is_retail_year_to_date'
        ELSE '' END || ',

        CASE WHEN base.year_quarter_key = (YEAR(cv.current_date_val) * 10 + QUARTER(cv.current_date_val)) AND base.date BETWEEN cv.current_quarter_start AND cv.current_date_val THEN 1 ELSE 0 END AS is_quarter_to_date,
        CASE WHEN base.year_month_key = TO_NUMBER(TO_CHAR(cv.current_date_val, ''YYYYMM'')) AND base.date BETWEEN cv.current_month_start AND cv.current_date_val THEN 1 ELSE 0 END AS is_month_to_date

    FROM ' || base_table || ' base
    CROSS JOIN current_values cv';

    -- Update the table metadata with refresh timestamp
    EXECUTE IMMEDIATE 'ALTER TABLE ' || flags_table || ' SET COMMENT = ''Dynamic calendar flags, last refreshed: '' || CURRENT_TIMESTAMP()';

    -- Add a refresh timestamp column for tracking
    EXECUTE IMMEDIATE 'ALTER TABLE ' || flags_table || ' ADD COLUMN IF NOT EXISTS refresh_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()';

    -- Update the refresh timestamp for the entire table
    EXECUTE IMMEDIATE 'UPDATE ' || flags_table || ' SET refresh_timestamp = CURRENT_TIMESTAMP()';

    RETURN 'Successfully refreshed calendar flags table ' || flags_table || ' at ' || CURRENT_TIMESTAMP();

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR refreshing calendar flags: ' || SQLERRM;
END;
$$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- STEP 5F: Create procedure for building the combined calendar view
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_CREATE_CALENDAR_VIEW(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    fully_qualified_prefix STRING := DATABASE_NAME || '.' || SCHEMA_NAME;
    base_table STRING := fully_qualified_prefix || '.BUSINESS_CALENDAR_STATIC';
    flags_table STRING := fully_qualified_prefix || '.CALENDAR_DYNAMIC_FLAGS';
    calendar_view STRING := fully_qualified_prefix || '.BUSINESS_CALENDAR';
    status_message STRING DEFAULT 'SUCCESS';
BEGIN
    -- Create the combined view
    EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW ' || calendar_view || ' AS
    SELECT
        -- All columns from the static calendar
        base.*,

        -- Dynamic flags from the refreshed table
        flags.is_current_date,
        flags.is_current_month,
        flags.is_current_quarter,
        flags.is_current_year,
        flags.is_current_fiscal_year,
        flags.is_current_retail_year,
        flags.is_last_7_days,
        flags.is_last_30_days,
        flags.is_last_90_days,
        flags.is_previous_month,
        flags.is_previous_quarter,
        flags.is_rolling_12_months,
        flags.is_rolling_quarter,
        flags.is_year_to_date,
        flags.is_fiscal_year_to_date,
        flags.is_retail_year_to_date,
        flags.is_quarter_to_date,
        flags.is_month_to_date,
        flags.refresh_timestamp AS flags_last_refreshed

    FROM ' || base_table || ' base
    JOIN ' || flags_table || ' flags ON base.date = flags.date
    ';

    RETURN 'Successfully created combined calendar view ' || calendar_view;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR creating calendar view: ' || SQLERRM;
END;
$$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- STEP 5G: Create procedure for processing retail seasons
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_PROCESS_RETAIL_SEASONS(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    base_table STRING := DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR_BASE';
    result STRING;
BEGIN
    -- Add retail season columns if they don't exist
    EXECUTE IMMEDIATE 'ALTER TABLE ' || base_table || ' ADD COLUMN IF NOT EXISTS retail_season STRING';
    EXECUTE IMMEDIATE 'ALTER TABLE ' || base_table || ' ADD COLUMN IF NOT EXISTS holiday_proximity STRING';

    -- Calculate retail seasons
    EXECUTE IMMEDIATE 'UPDATE ' || base_table || ' bc
    SET
        retail_season = (
            WITH good_fridays AS (
                SELECT year_num, MIN(date) as good_friday_date
                FROM ' || base_table || '
                WHERE is_holiday = 1 AND month_num IN (3, 4) AND day_long_name = ''Friday''
                GROUP BY year_num
            )
            SELECT
                CASE
                    WHEN (bc.month_num = 11) OR (bc.month_num = 12 AND bc.day_of_month_num <= 24) THEN ''Christmas Season''
                    WHEN (bc.month_num = 1 AND bc.day_of_month_num >= 15) OR (bc.month_num = 2 AND bc.day_of_month_num <= 15) THEN ''Back to School''
                    WHEN gf.good_friday_date IS NOT NULL AND bc.date BETWEEN DATEADD(DAY, -21, gf.good_friday_date) AND DATEADD(DAY, 1, gf.good_friday_date) THEN ''Easter Season''
                    WHEN bc.month_num = 6 THEN ''EOFY Sales''
                    ELSE ''Regular Season''
                END
            FROM good_fridays gf
            WHERE gf.year_num = bc.year_num
        ),
        holiday_proximity = (
            CASE
                WHEN bc.month_num = 12 AND bc.day_of_month_num BETWEEN 20 AND 24 THEN ''Christmas Eve Period''
                WHEN bc.month_num = 12 AND bc.day_of_month_num = 26 THEN ''Boxing Day''
                WHEN bc.month_num = 12 AND bc.day_of_month_num BETWEEN 27 AND 31 THEN ''Post-Christmas Sale''
                ELSE NULL
            END
        )';

    RETURN 'Successfully processed retail seasons for ' || DATABASE_NAME || '.' || SCHEMA_NAME;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR processing retail seasons: ' || SQLERRM;
END;
$$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- STEP 6: Create the configuration-driven business calendar system
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_BUILD_BUSINESS_CALENDAR(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    START_DATE DATE DEFAULT '2015-01-01',
    END_DATE DATE DEFAULT '2035-12-31',
    DATE_GRAIN STRING DEFAULT 'day',
    TIMEZONE STRING DEFAULT 'Australia/Adelaide',
    INCLUDE_AU_FISCAL BOOLEAN DEFAULT TRUE,
    INCLUDE_RETAIL BOOLEAN DEFAULT TRUE,
    RETAIL_PATTERN STRING DEFAULT '445',
    AUTO_ACTIVATE_TASK BOOLEAN DEFAULT FALSE
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    fully_qualified_prefix STRING;
    final_calendar_base STRING;
    temp_holidays STRING;
    temp_au_fiscal STRING;
    temp_retail STRING;
    status_message STRING;
    calendar_proc STRING;
    holidays_proc STRING;
    au_fiscal_proc STRING;
    retail_proc STRING;
    seasons_proc STRING;
    refresh_flags_proc STRING;
    create_view_proc STRING;
    include_fiscal_str STRING;
    include_retail_str STRING;
    task_activation_status STRING;
    should_add_fiscal NUMBER;
    should_add_retail NUMBER;
BEGIN
    -- Initialize variables
    fully_qualified_prefix := DATABASE_NAME || '.' || SCHEMA_NAME;
    final_calendar_base := fully_qualified_prefix || '.BUSINESS_CALENDAR_BASE';
    temp_holidays := 'TEMP_HOLIDAYS';
    temp_au_fiscal := 'TEMP_AU_FISCAL';
    temp_retail := 'TEMP_RETAIL';
    status_message := 'SUCCESS';
    include_fiscal_str := CASE WHEN INCLUDE_AU_FISCAL THEN 'TRUE' ELSE 'FALSE' END;
    include_retail_str := CASE WHEN INCLUDE_RETAIL THEN 'TRUE' ELSE 'FALSE' END;
    task_activation_status := CASE WHEN AUTO_ACTIVATE_TASK THEN 'RESUME' ELSE 'SUSPEND' END;
    should_add_fiscal := CASE WHEN INCLUDE_AU_FISCAL THEN 1 ELSE 0 END;
    should_add_retail := CASE WHEN INCLUDE_RETAIL THEN 1 ELSE 0 END;

    -- Set procedure names
    calendar_proc := DATABASE_NAME || '.' || SCHEMA_NAME || '.SP_GENERATE_GREGORIAN_CALENDAR';
    holidays_proc := DATABASE_NAME || '.' || SCHEMA_NAME || '.SP_PROCESS_HOLIDAYS';
    au_fiscal_proc := DATABASE_NAME || '.' || SCHEMA_NAME || '.SP_GENERATE_AU_FISCAL_CALENDAR';
    retail_proc := DATABASE_NAME || '.' || SCHEMA_NAME || '.SP_GENERATE_RETAIL_CALENDAR';
    seasons_proc := DATABASE_NAME || '.' || SCHEMA_NAME || '.SP_PROCESS_RETAIL_SEASONS';
    refresh_flags_proc := DATABASE_NAME || '.' || SCHEMA_NAME || '.SP_REFRESH_CALENDAR_FLAGS';
    create_view_proc := DATABASE_NAME || '.' || SCHEMA_NAME || '.SP_CREATE_CALENDAR_VIEW';

    -- 1. Generate the base Gregorian calendar
    EXECUTE IMMEDIATE 'CALL ' || calendar_proc || '('''
        || START_DATE || ''', '''
        || END_DATE || ''', '''
        || DATE_GRAIN || ''', '''
        || TIMEZONE || ''', '''
        || final_calendar_base || ''', '
        || 'FALSE)';

    -- 2. Process holidays
    EXECUTE IMMEDIATE 'CALL ' || holidays_proc || '('''
        || final_calendar_base || ''', '''
        || temp_holidays || ''', '''
        || DATABASE_NAME || ''', '''
        || SCHEMA_NAME || ''')';

    -- 3. Add AU Fiscal calendar if requested (using numerical check instead of boolean)
    IF (should_add_fiscal = 1) THEN
        EXECUTE IMMEDIATE 'CALL ' || au_fiscal_proc || '('''
            || final_calendar_base || ''', '''
            || temp_au_fiscal || ''')';

        -- Add the fiscal columns to the base table
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_start_date_for_year DATE';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_end_date_for_year DATE';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_year_num NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_year_desc STRING';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_quarter_num NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_quarter_year_key NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_quarter_desc STRING';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_quarter_start_date DATE';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_quarter_end_date DATE';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_month_num NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_month_year_key NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_month_desc STRING';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_month_start_date DATE';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_month_end_date DATE';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS au_fiscal_week_num NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS same_business_day_last_year DATE';

        -- Update the base table with AU fiscal calendar data
        EXECUTE IMMEDIATE 'UPDATE ' || final_calendar_base || ' b
        SET
            b.au_fiscal_start_date_for_year = f.au_fiscal_start_date_for_year,
            b.au_fiscal_end_date_for_year = f.au_fiscal_end_date_for_year,
            b.au_fiscal_year_num = f.au_fiscal_year_num,
            b.au_fiscal_year_desc = f.au_fiscal_year_desc,
            b.au_fiscal_quarter_num = f.au_fiscal_quarter_num,
            b.au_fiscal_quarter_year_key = f.au_fiscal_quarter_year_key,
            b.au_fiscal_quarter_desc = f.au_fiscal_quarter_desc,
            b.au_fiscal_quarter_start_date = f.au_fiscal_quarter_start_date,
            b.au_fiscal_quarter_end_date = f.au_fiscal_quarter_end_date,
            b.au_fiscal_month_num = f.au_fiscal_month_num,
            b.au_fiscal_month_year_key = f.au_fiscal_month_year_key,
            b.au_fiscal_month_desc = f.au_fiscal_month_desc,
            b.au_fiscal_month_start_date = f.au_fiscal_month_start_date,
            b.au_fiscal_month_end_date = f.au_fiscal_month_end_date,
            b.au_fiscal_week_num = f.au_fiscal_week_num,
            b.same_business_day_last_year = f.same_business_day_last_year
        FROM ' || temp_au_fiscal || ' f
        WHERE b.date = f.date';
    END IF;

    -- 4. Add Retail calendar if requested (using numerical check instead of boolean)
    IF (should_add_retail = 1) THEN
        EXECUTE IMMEDIATE 'CALL ' || retail_proc || '('''
            || final_calendar_base || ''', '''
            || temp_retail || ''', '''
            || RETAIL_PATTERN || ''')';

        -- Add the retail columns to the base table
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_start_of_year DATE';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_end_of_year DATE';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_year_num NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_year_desc STRING';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_week_num NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_half_num NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_half_desc STRING';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_quarter_num NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_quarter_desc STRING';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_quarter_year_key NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_period_num NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_period_desc STRING';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_year_month_key NUMBER';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_month_short_name STRING';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_month_long_name STRING';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_month_year_desc STRING';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' ADD COLUMN IF NOT EXISTS retail_month_full_year_desc STRING';

        -- Update the base table with retail calendar data
        EXECUTE IMMEDIATE 'UPDATE ' || final_calendar_base || ' b
        SET
            b.retail_start_of_year = r.retail_start_of_year,
            b.retail_end_of_year = r.retail_end_of_year,
            b.retail_year_num = r.retail_year_num,
            b.retail_year_desc = r.retail_year_desc,
            b.retail_week_num = r.retail_week_num,
            b.retail_half_num = r.retail_half_num,
            b.retail_half_desc = r.retail_half_desc,
            b.retail_quarter_num = r.retail_quarter_num,
            b.retail_quarter_desc = r.retail_quarter_desc,
            b.retail_quarter_year_key = r.retail_quarter_year_key,
            b.retail_period_num = r.retail_period_num,
            b.retail_period_desc = r.retail_period_desc,
            b.retail_year_month_key = r.retail_year_month_key,
            b.retail_month_short_name = r.retail_month_short_name,
            b.retail_month_long_name = r.retail_month_long_name,
            b.retail_month_year_desc = r.retail_month_year_desc,
            b.retail_month_full_year_desc = r.retail_month_full_year_desc
        FROM ' || temp_retail || ' r
        WHERE b.date = r.date';
    END IF;

    -- 5. Process retail seasons
    EXECUTE IMMEDIATE 'CALL ' || seasons_proc || '('''
        || DATABASE_NAME || ''', '''
        || SCHEMA_NAME || ''')';

    -- 6. Create static calendar view
    EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR_STATIC AS
    SELECT * FROM ' || final_calendar_base;

    -- 7. Perform initial population of the dynamic flags table
    EXECUTE IMMEDIATE 'CALL ' || refresh_flags_proc || '('''
        || DATABASE_NAME || ''', '''
        || SCHEMA_NAME || ''', '
        || include_fiscal_str || ', '
        || include_retail_str || ')';

    -- 8. Create the combined calendar view
    EXECUTE IMMEDIATE 'CALL ' || create_view_proc || '('''
        || DATABASE_NAME || ''', '''
        || SCHEMA_NAME || ''')';

    -- 9. Create or replace the scheduled task for daily flag refreshes
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TASK ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.REFRESH_CALENDAR_FLAGS
      WAREHOUSE = CURRENT_WAREHOUSE()
      SCHEDULE = ''USING CRON 0 1 * * * UTC''
      AS
      CALL ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.SP_REFRESH_CALENDAR_FLAGS('''
      || DATABASE_NAME || ''', ''' || SCHEMA_NAME || ''', '
      || include_fiscal_str || ', '
      || include_retail_str || ');';

    -- Set task state based on AUTO_ACTIVATE_TASK parameter
    EXECUTE IMMEDIATE 'ALTER TASK ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.REFRESH_CALENDAR_FLAGS ' || task_activation_status;

    -- 10. Clean up temporary tables
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || temp_holidays;
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || temp_au_fiscal;
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || temp_retail;

    -- 11. Add clustering to the base table for better performance
    EXECUTE IMMEDIATE 'ALTER TABLE ' || final_calendar_base || ' CLUSTER BY (date)';

    -- Return simple status message
    RETURN status_message || ' Calendar built in ' || fully_qualified_prefix ||
           ' with date range: ' || START_DATE::STRING || ' to ' || END_DATE::STRING;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 6A: Create procedures for calendar maintenance
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_REFRESH_CALENDAR_ONLY(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    refresh_status STRING;
    au_fiscal_exists BOOLEAN;
    retail_exists BOOLEAN;
    proc_prefix STRING;
    check_columns_sql STRING;
    count_val NUMBER;
    call_stmt STRING;
BEGIN
    -- Set the procedure prefix
    proc_prefix := DATABASE_NAME || '.' || SCHEMA_NAME || '.';

    -- Check if calendar components exist - check for au_fiscal_year_num column
    check_columns_sql := 'SELECT COUNT(*) AS count_val
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = UPPER(''' || SCHEMA_NAME || ''')
        AND TABLE_NAME = ''BUSINESS_CALENDAR_BASE''
        AND COLUMN_NAME = ''AU_FISCAL_YEAR_NUM''';

    EXECUTE IMMEDIATE :check_columns_sql;
    au_fiscal_exists := FALSE;

    BEGIN
        SELECT COUNT_VAL INTO :count_val FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        IF (count_val > 0) THEN
            au_fiscal_exists := TRUE;
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            au_fiscal_exists := FALSE;
    END;

    -- Check for retail_year_num column
    check_columns_sql := 'SELECT COUNT(*) AS count_val
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = UPPER(''' || SCHEMA_NAME || ''')
        AND TABLE_NAME = ''BUSINESS_CALENDAR_BASE''
        AND COLUMN_NAME = ''RETAIL_YEAR_NUM''';

    EXECUTE IMMEDIATE :check_columns_sql;
    retail_exists := FALSE;

    BEGIN
        SELECT COUNT_VAL INTO :count_val FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        IF (count_val > 0) THEN
            retail_exists := TRUE;
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            retail_exists := FALSE;
    END;

    -- Call the refresh procedure with existing calendar component flags
    call_stmt := 'CALL ' || proc_prefix || 'SP_REFRESH_CALENDAR_FLAGS('''
        || DATABASE_NAME || ''', '''
        || SCHEMA_NAME || ''', '
        || (CASE WHEN au_fiscal_exists THEN 'TRUE' ELSE 'FALSE' END) || ', '
        || (CASE WHEN retail_exists THEN 'TRUE' ELSE 'FALSE' END) || ')';

    EXECUTE IMMEDIATE :call_stmt;

    SELECT * INTO :refresh_status FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    RETURN refresh_status;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR in SP_REFRESH_CALENDAR_ONLY: ' || SQLERRM;
END;
$$;

-- Procedure to verify and activate the task if not already running
CREATE OR REPLACE PROCEDURE SP_ACTIVATE_CALENDAR_TASK(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    task_exists BOOLEAN DEFAULT FALSE;
    task_state STRING DEFAULT NULL;
    task_name STRING;
    task_count NUMBER;
BEGIN
    -- Set the task name
    task_name := DATABASE_NAME || '.' || SCHEMA_NAME || '.REFRESH_CALENDAR_FLAGS';

    -- Check if task exists using SHOW TASKS
    EXECUTE IMMEDIATE 'SHOW TASKS LIKE ''' || task_name || ''' IN ' || DATABASE_NAME || '.' || SCHEMA_NAME;

    -- Get the count of matching tasks
    SELECT COUNT(*) INTO :task_count FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    -- Set task_exists based on count
    IF (task_count > 0) THEN
        task_exists := TRUE;

        -- Get the state of the task
        EXECUTE IMMEDIATE 'SELECT "state" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) LIMIT 1';
        SELECT * INTO :task_state FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    END IF;

    -- Check if task exists
    IF (NOT task_exists) THEN
        RETURN 'ERROR: Calendar refresh task does not exist. Run SP_BUILD_BUSINESS_CALENDAR first.';
    END IF;

    -- Resume task if it's suspended
    IF (task_state = 'SUSPENDED') THEN
        EXECUTE IMMEDIATE 'ALTER TASK ' || task_name || ' RESUME';
        RETURN 'Calendar refresh task activated successfully.';
    ELSE
        RETURN 'Calendar refresh task is already active.';
    END IF;
END;
$$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 7: Call stored procedure to build the Business Calendar
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Example of how to call the stored procedure with parameters
CALL SP_BUILD_BUSINESS_CALENDAR(
    'spg_dap01',  -- DATABASE_NAME
    'pbi',        -- SCHEMA_NAME
    '2015-01-01', -- START_DATE
    '2035-12-31', -- END_DATE
    'day',        -- DATE_GRAIN
    'Australia/Adelaide', -- TIMEZONE
    TRUE,         -- INCLUDE_AU_FISCAL
    TRUE,         -- INCLUDE_RETAIL
    '445',        -- RETAIL_PATTERN
    TRUE         -- AUTO_ACTIVATE_TASK (set to TRUE if you want to automatically activate the refresh task)
);

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 8: Create calendar comparison functions as User-Defined Functions (UDFs)
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 1. Business Day Navigation Functions
-- Get next business day after a given date
CREATE OR REPLACE FUNCTION FN_NEXT_BUSINESS_DAY(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    input_date DATE
)
RETURNS DATE
LANGUAGE SQL
AS
$$
    DECLARE
        result DATE;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT MIN(date)
            FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR
            WHERE date > :1
            AND is_trading_day = 1'
        INTO result
        USING input_date;

        RETURN result;
    END;
$$;

-- Get previous business day before a given date
CREATE OR REPLACE FUNCTION FN_PREVIOUS_BUSINESS_DAY(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    input_date DATE
)
RETURNS DATE
LANGUAGE SQL
AS
$$
    DECLARE
        result DATE;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT MAX(date)
            FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR
            WHERE date < :1
            AND is_trading_day = 1'
        INTO result
        USING input_date;

        RETURN result;
    END;
$$;

-- Add N business days to a given date
CREATE OR REPLACE FUNCTION FN_ADD_BUSINESS_DAYS(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    input_date DATE,
    days_to_add INT
)
RETURNS DATE
LANGUAGE SQL
AS
$$
    DECLARE
        result_date DATE;
        remaining_days INT := ABS(days_to_add);
        direction INT := CASE WHEN days_to_add >= 0 THEN 1 ELSE -1 END;
        current_date DATE := input_date;
        is_trading_day BOOLEAN;
    BEGIN
        WHILE remaining_days > 0 DO
            -- Move one calendar day in the appropriate direction
            current_date := DATEADD(DAY, direction, current_date);

            -- Check if it's a business day
            EXECUTE IMMEDIATE 'SELECT COUNT(*) > 0 FROM ' ||
                DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR
                WHERE date = :1
                AND is_trading_day = 1'
            INTO is_trading_day
            USING current_date;

            IF is_trading_day THEN
                remaining_days := remaining_days - 1;
            END IF;
        END WHILE;

        RETURN current_date;
    END;
$$;

-- 2. Business Day Counting Functions
-- Count business days between two dates (inclusive)
CREATE OR REPLACE FUNCTION FN_BUSINESS_DAYS_BETWEEN(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    start_date DATE,
    end_date DATE
)
RETURNS INT
LANGUAGE SQL
AS
$$
    DECLARE
        biz_days INT;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT COUNT(*)
        FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR
        WHERE date BETWEEN :1 AND :2
        AND is_trading_day = 1'
        INTO biz_days
        USING start_date, end_date;

        RETURN biz_days;
    END;
$$;

-- Get the Nth business day of a month
CREATE OR REPLACE FUNCTION FN_NTH_BUSINESS_DAY_OF_MONTH(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    year_num INT,
    month_num INT,
    n INT
)
RETURNS DATE
LANGUAGE SQL
AS
$$
    DECLARE
        result_date DATE;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT date
        FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR
        WHERE year_num = :1
        AND month_num = :2
        AND is_trading_day = 1
        ORDER BY date
        LIMIT 1 OFFSET :3'
        INTO result_date
        USING year_num, month_num, (n-1);

        RETURN result_date;
    END;
$$;

-- Get the Nth last business day of a month
CREATE OR REPLACE FUNCTION FN_NTH_LAST_BUSINESS_DAY_OF_MONTH(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    year_num INT,
    month_num INT,
    n INT
)
RETURNS DATE
LANGUAGE SQL
AS
$$
    DECLARE
        result_date DATE;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT date
        FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR
        WHERE year_num = :1
        AND month_num = :2
        AND is_trading_day = 1
        ORDER BY date DESC
        LIMIT 1 OFFSET :3'
        INTO result_date
        USING year_num, month_num, (n-1);

        RETURN result_date;
    END;
$$;

-- 3. Fiscal Calendar Functions
-- Convert Gregorian date to Australian Fiscal Year date
CREATE OR REPLACE FUNCTION FN_CONVERT_TO_AU_FISCAL_DATE(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    input_date DATE
)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
    DECLARE
        result OBJECT;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT OBJECT_CONSTRUCT(
            ''au_fiscal_year_num'', au_fiscal_year_num,
            ''au_fiscal_quarter_num'', au_fiscal_quarter_num,
            ''au_fiscal_month_num'', au_fiscal_month_num,
            ''au_fiscal_week_num'', au_fiscal_week_num
        )
        FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR
        WHERE date = :1'
        INTO result
        USING input_date;

        RETURN result;
    END;
$$;

-- Get equivalent date in previous Fiscal Year
CREATE OR REPLACE FUNCTION FN_SAME_DAY_PREV_FISCAL_YEAR(DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    input_date DATE,
    n_years INT DEFAULT 1
)
RETURNS DATE
LANGUAGE SQL
AS
$$
    DECLARE
        result_date DATE;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT date
        FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR
        WHERE au_fiscal_month_num = (
            SELECT au_fiscal_month_num FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR WHERE date = :1
        )
        AND au_fiscal_year_num = (
            SELECT au_fiscal_year_num - :2 FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR WHERE date = :1
        )
        AND day_of_month_num = (
            SELECT day_of_month_num FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR WHERE date = :1
        )
        ORDER BY date
        LIMIT 1'
        INTO result_date
        USING input_date, n_years;

        RETURN result_date;
    END;
$$;

-- 4. Season Detection Functions
-- Determine if a date falls within a specific retail season
CREATE OR REPLACE FUNCTION FN_IS_IN_RETAIL_SEASON(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    input_date DATE,
    season_name STRING
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    DECLARE
        is_in_season BOOLEAN;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT COUNT(*) > 0
        FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR
        WHERE date = :1
        AND retail_season = :2'
        INTO is_in_season
        USING input_date, season_name;

        RETURN is_in_season;
    END;
$$;

-- Get all dates in a specific retail season for a given year
CREATE OR REPLACE FUNCTION FN_GET_RETAIL_SEASON_DATES(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    year_num INT,
    season_name STRING
)
RETURNS TABLE(dates DATE)
LANGUAGE SQL
AS
$$
    SELECT date
    FROM IDENTIFIER(DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR')
    WHERE year_num = year_num
    AND retail_season = season_name
    ORDER BY date
$$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 9: Create example queries and usage examples
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Example queries that can be used with the calendar:

-- 1. Basic date dimension lookup
-- SELECT * FROM [DATABASE].[SCHEMA].BUSINESS_CALENDAR WHERE date = CURRENT_DATE();

-- 2. Finding all trading days in the current month
-- SELECT date, day_long_name
-- FROM [DATABASE].[SCHEMA].BUSINESS_CALENDAR
-- WHERE is_current_month = 1 AND is_trading_day = 1
-- ORDER BY date;

-- 3. Year-to-date comparison (this year vs last year)
-- WITH sales_data AS (
--   SELECT s.date, s.amount
--   FROM sales s
-- )
-- SELECT
--   SUM(CASE WHEN bc.is_fiscal_year_to_date = 1 THEN s.amount ELSE 0 END) as ytd_sales,
--   SUM(CASE WHEN bc.date BETWEEN bc.same_business_day_last_year AND CURRENT_DATE()
--       AND YEAR(bc.same_business_day_last_year) = YEAR(CURRENT_DATE())-1
--       THEN s.amount ELSE 0 END) as prev_ytd_sales
-- FROM sales_data s
-- JOIN [DATABASE].[SCHEMA].BUSINESS_CALENDAR bc ON s.date = bc.date;

-- 4. Finding the next trading day
-- SELECT * FROM TABLE([DATABASE].[SCHEMA].FN_NEXT_BUSINESS_DAY('[DATABASE]', '[SCHEMA]', CURRENT_DATE()));

-- 5. Counting business days in a period
-- SELECT [DATABASE].[SCHEMA].FN_BUSINESS_DAYS_BETWEEN('[DATABASE]', '[SCHEMA]', '2023-01-01', '2023-01-31');

-- 6. Checking which retail season a date falls into
-- SELECT [DATABASE].[SCHEMA].FN_IS_IN_RETAIL_SEASON('[DATABASE]', '[SCHEMA]', CURRENT_DATE(), 'Christmas Season');

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 10: Add comments to objects and provide completion message
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Add comment on the main calendar view
COMMENT ON VIEW IF EXISTS BUSINESS_CALENDAR IS
'Comprehensive business calendar with dynamically calculated relative time flags.
Supports Gregorian, AU Fiscal, and Retail calendars with proper sort keys for BI tools.
This calendar view automatically updates its dynamic flags each day via a scheduled task.

Key features:
- Standard Gregorian calendar (Jan-Dec)
- Australian fiscal year calendar (Jul-Jun)
- Retail 4-4-5 calendar (starting first Monday of July)
- Trading day calculations and holiday integrations
- Retail season indicators
- Year-over-Year mappings for comparative analysis
- Dynamically updated time period flags (is_current_month, is_fiscal_ytd, etc.)

Last flags refresh: See flags_last_refreshed column.';

-- Add comment on the base calendar table
COMMENT ON TABLE IF EXISTS BUSINESS_CALENDAR_BASE IS
'Base table containing all calendar attributes. Do not query directly - use BUSINESS_CALENDAR view instead.
This table contains date attributes for the period YYYY-MM-DD to YYYY-MM-DD.
To extend the date range, re-run the SP_BUILD_BUSINESS_CALENDAR procedure.';

-- Add comment on the static calendar view
COMMENT ON VIEW IF EXISTS BUSINESS_CALENDAR_STATIC IS
'Static view of BUSINESS_CALENDAR_BASE table. Used by the BUSINESS_CALENDAR view.
Do not query directly - use BUSINESS_CALENDAR view instead.';

-- Add comment on the dynamic flags table
COMMENT ON TABLE IF EXISTS CALENDAR_DYNAMIC_FLAGS IS
'Dynamic flags for the business calendar, updated daily via the REFRESH_CALENDAR_FLAGS task.
Do not query directly - use BUSINESS_CALENDAR view instead.';

-- Add comment on the refresh task
COMMENT ON TASK IF EXISTS REFRESH_CALENDAR_FLAGS IS
'Scheduled task that runs daily at 1 AM UTC to refresh the calendar dynamic flags.
This task must be activated using ALTER TASK REFRESH_CALENDAR_FLAGS RESUME;';

-- Print completion message
SELECT 'Business Calendar System setup complete.
To build the calendar in your database and schema, run:
CALL SP_BUILD_BUSINESS_CALENDAR(''YOUR_DATABASE'', ''YOUR_SCHEMA'');

To activate the refresh task after building, run:
CALL SP_ACTIVATE_CALENDAR_TASK(''YOUR_DATABASE'', ''YOUR_SCHEMA'');' AS COMPLETION_MESSAGE;
