-- Snowflake allow egress to data.gov.au
-- This network rule allows outbound connections from Snowflake to the data.gov.au domain on port 443 (HTTPS).
CREATE
OR REPLACE NETWORK RULE allow_data_gov_au MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('data.gov.au:443');

-- External Access Integration for accessing external APIs.
-- This integration defines the network rules and secrets (if any) that Snowflake is allowed to use when accessing external resources.
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION apis_access_integration
  ALLOWED_NETWORK_RULES = (allow_data_gov_au) -- Specifies the network rules that this integration uses to control network access.
  ENABLED = true; -- Enables the external access integration.  Must be true for it to function.

------------------------------------------------------------------------------------------------------------------------
-- Stored Procedure to Load Australian Public Holidays from data.gov.au
------------------------------------------------------------------------------------------------------------------------
-- This stored procedure retrieves Australian public holiday data from the data.gov.au API,
-- transforms it, and loads it into a Snowflake table.

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
