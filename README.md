- Standard Gregorian calendar
- Australian Fiscal Year calendar (July-June)
- Retail 4-4-5 calendar (starting first Monday of July)
- Trading day calculations
- Retail season identification
- Dynamic relative time flags
- Jurisdiction-specific holiday flags for all Australian states and territories
- Sort order keys for proper ordering in BI tools# Snowflake Business Calendar System

## Overview

This repository contains SQL code for implementing a comprehensive business calendar system in Snowflake. The calendar supports multiple date frameworks, including:

- Standard Gregorian calendar
- Australian Fiscal Year calendar (July-June)
- Retail 4-4-5 calendar (starting first Monday of July)
- Trading day calculations
- Retail season identification
- Dynamic relative time flags
- Jurisdiction-specific holiday flags for all Australian states and territories

## Features

- **Public Holiday Integration**: Automatically fetches Australian public holidays from data.gov.au with jurisdiction-specific flags (NSW, VIC, QLD, SA, WA, TAS, ACT, NT, National)
- **Multiple Calendar Frameworks**: Supports Gregorian, Australian Fiscal, and Retail 4-4-5 calendars
- **Trading Day Logic**: Identifies business days (weekdays excluding holidays)
- **Year-over-Year Mapping**: Supports date comparisons via:
  - Same calendar date from previous year
  - Same business day from previous fiscal year
- **Retail Seasons**: Identifies key retail periods (Christmas, Back to School, Easter, EOFY)
- **Comprehensive Attributes**: 100+ calendar attributes for any date analysis need
- **Dynamic Relative Time Flags**: Automatically calculates time period flags (current month, YTD, MTD, etc.)
- **BI-Friendly Sort Keys**: Dedicated sort order columns for months, quarters, weeks, and days to ensure proper ordering in BI tools

## Implementation Structure

The implementation follows a systematic approach:

1. **Network Rule Setup**: Creates necessary network rules to allow access to data.gov.au
2. **Holiday Loader Procedure**: Creates `LOAD_AU_HOLIDAYS` stored procedure to fetch public holidays
3. **Business Calendar Procedure**: Implements `SP_BUILD_BUSINESS_CALENDAR` to generate the base calendar table
4. **Dynamic View**: Creates `BUSINESS_CALENDAR` view with relative time flags that update automatically

## Components

### 1. Network Rules and External Access Integration

```sql
CREATE OR REPLACE NETWORK RULE allow_data_gov_au 
MODE = EGRESS TYPE = HOST_PORT 
VALUE_LIST = ('data.gov.au:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION apis_access_integration
  ALLOWED_NETWORK_RULES = (allow_data_gov_au)
  ENABLED = true;
```

### 2. Australian Public Holidays Loader

A Python-based stored procedure that fetches public holiday data from data.gov.au and loads it into Snowflake.

```sql
CREATE PROCEDURE LOAD_AU_HOLIDAYS(DATABASE_NAME VARCHAR, SCHEMA_NAME VARCHAR)
RETURNS String
LANGUAGE PYTHON
...
```

### 3. Business Calendar Generator

A SQL stored procedure that builds a comprehensive calendar dimension table with multiple time frameworks.

```sql
CREATE OR REPLACE PROCEDURE SP_BUILD_BUSINESS_CALENDAR()
RETURNS VARCHAR
LANGUAGE SQL
...
```

### 4. Dynamic Calendar View

A view that combines the static calendar table with dynamic relative time flags.

```sql
CREATE OR REPLACE VIEW SPG_DAP01.PBI.BUSINESS_CALENDAR
...
```

## Usage

### Setting Up

Execute the scripts in this order:

1. Network rules setup
2. Create the `LOAD_AU_HOLIDAYS` procedure
3. Call the procedure with target database and schema: `CALL LOAD_AU_HOLIDAYS('YOUR_DATABASE','YOUR_SCHEMA');`
4. Create the `SP_BUILD_BUSINESS_CALENDAR` procedure
5. Call the procedure: `CALL SP_BUILD_BUSINESS_CALENDAR();`
6. Create the `BUSINESS_CALENDAR` view

### Using the Calendar in Queries

Simple date dimension lookup:
```sql
SELECT * FROM BUSINESS_CALENDAR WHERE date = CURRENT_DATE();
```

Finding all trading days in the current month:
```sql
SELECT date, day_long_name FROM BUSINESS_CALENDAR 
WHERE is_current_month = 1 AND is_trading_day = 1
ORDER BY date;
```

Year-to-date comparison:
```sql
SELECT 
  SUM(CASE WHEN bc.is_fiscal_year_to_date = 1 THEN s.amount ELSE 0 END) as ytd_sales,
  SUM(CASE WHEN bc.date BETWEEN bc.same_business_day_last_year AND CURRENT_DATE() 
      AND YEAR(bc.same_business_day_last_year) = YEAR(CURRENT_DATE())-1 
      THEN s.amount ELSE 0 END) as prev_ytd_sales
FROM sales s
JOIN BUSINESS_CALENDAR bc ON s.date = bc.date;
```

## Data Dictionary

The business calendar contains 85+ columns including:

- **Basic date attributes**: date, year, month, quarter, etc.
- **Business indicators**: weekdays, holidays, trading days
- **Fiscal calendar**: AU fiscal year, quarters, months
- **Retail calendar**: 4-4-5 year, periods, weeks
- **Trading day metrics**: trading day counts, sequences
- **Retail seasons**: Christmas, Back to School, Easter, EOFY
- **Relative time flags**: current_month, year_to_date, etc.

For a complete data dictionary, refer to the column comments in the view definition.

## Maintenance

- **Holiday Updates**: The holiday data should be refreshed periodically by calling `LOAD_AU_HOLIDAYS`
- **Calendar Extension**: The calendar currently spans from 2015 to 2035, modify the date range in `SP_BUILD_BUSINESS_CALENDAR` if needed
- **Performance**: The base calendar table is clustered by date for optimal query performance

## Requirements

- Snowflake account with appropriate privileges
- External network access to data.gov.au
- Python runtime support in Snowflake

## Future Enhancements

Possible enhancements that could be implemented:

- Support for additional regions/countries
- Extended retail metrics (trading weeks, promotional periods)
- Custom holiday categorisation
- Calendar alert system for fiscal period boundaries
- Data visualisation templates

## License

This project is licensed under the MIT License - see the LICENSE file for details.