-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Snowflake Business Calendar Functions
-- Purpose: Extend the Business Calendar system with useful UDFs for date operations, business day calculations, and advanced calendar features
-- Dependencies: Requires the BUSINESS_CALENDAR view to be properly set up
-- Author: [Your Name]
-- Date: [Current Date]
-- Version: 1.0
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- DATABASE AND SCHEMA CONFIGURATION
-- Replace these with your actual database and schema names if different
USE DATABASE SPG_DAP01;
USE SCHEMA PBI;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- SECTION 1: BUSINESS DAY NAVIGATION FUNCTIONS
-- Functions to move between business days (trading days) and perform common business date operations
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/**
 * Returns the next business day after the provided date
 * A business day is defined as a weekday that is not a holiday (is_trading_day = 1)
 *
 * @param input_date The starting date
 * @return The next business day
 *
 * Example: SELECT FN_NEXT_BUSINESS_DAY('2023-12-22'); -- Returns 2023-12-27 (skipping weekend and Christmas holiday)
 */
CREATE OR REPLACE FUNCTION FN_NEXT_BUSINESS_DAY(input_date DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            SELECT MIN(date)
            FROM BUSINESS_CALENDAR
            WHERE date > input_date
            AND is_trading_day = 1
        )
    END
$$;


/**
 * Returns the previous business day before the provided date
 * A business day is defined as a weekday that is not a holiday (is_trading_day = 1)
 *
 * @param input_date The starting date
 * @return The previous business day
 *
 * Example: SELECT FN_PREVIOUS_BUSINESS_DAY('2023-12-27'); -- Returns 2023-12-22 (skipping weekend and Christmas holiday)
 */
CREATE OR REPLACE FUNCTION FN_PREVIOUS_BUSINESS_DAY(input_date DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            SELECT MAX(date)
            FROM BUSINESS_CALENDAR
            WHERE date < input_date
            AND is_trading_day = 1
        )
    END
$$;


/**
 * Returns the date that is N business days from the provided date
 * If days_to_add is positive, adds that many business days
 * If days_to_add is negative, subtracts that many business days
 * If days_to_add is zero, returns the input date (if it's a business day) or the next business day
 *
 * @param input_date The starting date
 * @param days_to_add The number of business days to add (can be negative)
 * @return The date that is N business days from input_date
 *
 * Example: SELECT FN_ADD_BUSINESS_DAYS('2023-12-22', 3); -- Returns 2023-12-29 (adding 3 business days)
 * Example: SELECT FN_ADD_BUSINESS_DAYS('2023-12-22', -3); -- Returns 2023-12-19 (subtracting 3 business days)
 */
CREATE OR REPLACE FUNCTION FN_ADD_BUSINESS_DAYS(input_date DATE, days_to_add INT)
RETURNS DATE
LANGUAGE JAVASCRIPT
AS
$$
    // Input validation
    if (INPUT_DATE === null || DAYS_TO_ADD === null) {
        return null;
    }

    // Initialize variables
    const query = `
        WITH target_index AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY date) AS row_num, 
                date
            FROM BUSINESS_CALENDAR
            WHERE is_trading_day = 1
        ),
        input_info AS (
            SELECT 
                CASE 
                    -- If input date is a business day, use its index
                    WHEN EXISTS (SELECT 1 FROM BUSINESS_CALENDAR WHERE date = '${INPUT_DATE}' AND is_trading_day = 1) 
                        THEN (SELECT row_num FROM target_index WHERE date = '${INPUT_DATE}')
                    -- If input date is not a business day and days_to_add >= 0, use the next business day's index
                    WHEN ${DAYS_TO_ADD} >= 0 
                        THEN (SELECT MIN(row_num) FROM target_index WHERE date > '${INPUT_DATE}')
                    -- If input date is not a business day and days_to_add < 0, use the previous business day's index
                    ELSE (SELECT MAX(row_num) FROM target_index WHERE date < '${INPUT_DATE}')
                END AS start_row_num
        )
        SELECT date 
        FROM target_index
        WHERE row_num = (SELECT start_row_num FROM input_info) + ${DAYS_TO_ADD}`;
    
    // Execute the query
    const stmt = snowflake.createStatement({sqlText: query});
    const result = stmt.execute();
    
    // Return the result or null if no matching date found
    if (result.next()) {
        return result.getColumnValue(1);
    } else {
        return null;
    }
$$;


/**
 * Returns the date of the first business day in a given month
 *
 * @param year The year
 * @param month The month (1-12)
 * @return The first business day of the month
 *
 * Example: SELECT FN_FIRST_BUSINESS_DAY_OF_MONTH(2023, 1); -- Returns 2023-01-02 (assuming Jan 1 is a Sunday)
 */
CREATE OR REPLACE FUNCTION FN_FIRST_BUSINESS_DAY_OF_MONTH(year INT, month INT)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN year IS NULL OR month IS NULL OR month < 1 OR month > 12 THEN 
            NULL
        ELSE (
            SELECT MIN(date)
            FROM BUSINESS_CALENDAR
            WHERE year_num = year
            AND month_num = month
            AND is_trading_day = 1
        )
    END
$$;


/**
 * Returns the date of the last business day in a given month
 *
 * @param year The year
 * @param month The month (1-12)
 * @return The last business day of the month
 *
 * Example: SELECT FN_LAST_BUSINESS_DAY_OF_MONTH(2023, 12); -- Returns 2023-12-29 (assuming Dec 30-31 are weekend)
 */
CREATE OR REPLACE FUNCTION FN_LAST_BUSINESS_DAY_OF_MONTH(year INT, month INT)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN year IS NULL OR month IS NULL OR month < 1 OR month > 12 THEN 
            NULL
        ELSE (
            SELECT MAX(date)
            FROM BUSINESS_CALENDAR
            WHERE year_num = year
            AND month_num = month
            AND is_trading_day = 1
        )
    END
$$;


-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- SECTION 2: BUSINESS DAY COUNTING AND COMPARISON FUNCTIONS
-- Functions to count business days and compare dates in a business context
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/**
 * Counts the number of business days between two dates (inclusive)
 *
 * @param start_date The start date
 * @param end_date The end date
 * @return The number of business days between start_date and end_date (inclusive)
 *
 * Example: SELECT FN_BUSINESS_DAYS_BETWEEN('2023-12-01', '2023-12-15'); -- Returns the number of business days in this period
 */
CREATE OR REPLACE FUNCTION FN_BUSINESS_DAYS_BETWEEN(start_date DATE, end_date DATE)
RETURNS INT
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN start_date IS NULL OR end_date IS NULL THEN 
            NULL
        WHEN end_date < start_date THEN
            0
        ELSE (
            SELECT COUNT(*)
            FROM BUSINESS_CALENDAR
            WHERE date BETWEEN start_date AND end_date
            AND is_trading_day = 1
        )
    END
$$;


/**
 * Gets the Nth business day of a month
 * Returns NULL if N exceeds the number of business days in the month
 *
 * @param year The year
 * @param month The month (1-12)
 * @param n The ordinal position of the business day to find (1-based)
 * @return The date of the Nth business day in the specified month
 *
 * Example: SELECT FN_NTH_BUSINESS_DAY_OF_MONTH(2023, 12, 5); -- Returns the 5th business day in December 2023
 */
CREATE OR REPLACE FUNCTION FN_NTH_BUSINESS_DAY_OF_MONTH(year INT, month INT, n INT)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN year IS NULL OR month IS NULL OR n IS NULL THEN 
            NULL
        WHEN month < 1 OR month > 12 OR n < 1 THEN
            NULL
        ELSE (
            SELECT date
            FROM BUSINESS_CALENDAR
            WHERE year_num = year
            AND month_num = month
            AND is_trading_day = 1
            ORDER BY date
            LIMIT 1 OFFSET (n-1)
        )
    END
$$;


/**
 * Gets the Nth-to-last business day of a month
 * Returns NULL if N exceeds the number of business days in the month
 *
 * @param year The year
 * @param month The month (1-12)
 * @param n The ordinal position from the end of the business day to find (1-based)
 * @return The date of the Nth-to-last business day in the specified month
 *
 * Example: SELECT FN_NTH_LAST_BUSINESS_DAY_OF_MONTH(2023, 12, 2); -- Returns the 2nd-to-last business day in December 2023
 */
CREATE OR REPLACE FUNCTION FN_NTH_LAST_BUSINESS_DAY_OF_MONTH(year INT, month INT, n INT)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN year IS NULL OR month IS NULL OR n IS NULL THEN 
            NULL
        WHEN month < 1 OR month > 12 OR n < 1 THEN
            NULL
        ELSE (
            SELECT date
            FROM BUSINESS_CALENDAR
            WHERE year_num = year
            AND month_num = month
            AND is_trading_day = 1
            ORDER BY date DESC
            LIMIT 1 OFFSET (n-1)
        )
    END
$$;


/**
 * Determines if a date is a business day
 *
 * @param input_date The date to check
 * @return Boolean indicating if the date is a business day (1 for yes, 0 for no)
 *
 * Example: SELECT FN_IS_BUSINESS_DAY('2023-12-25'); -- Returns 0 (Christmas is a holiday)
 */
CREATE OR REPLACE FUNCTION FN_IS_BUSINESS_DAY(input_date DATE)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            SELECT is_trading_day = 1
            FROM BUSINESS_CALENDAR
            WHERE date = input_date
        )
    END
$$;


/**
 * Determines how many business days have elapsed in the current month as of the given date
 *
 * @param input_date The reference date
 * @return The count of business days elapsed in the month up to and including input_date
 *
 * Example: SELECT FN_BUSINESS_DAYS_ELAPSED_IN_MONTH('2023-12-15'); -- Returns business days from Dec 1 to Dec 15
 */
CREATE OR REPLACE FUNCTION FN_BUSINESS_DAYS_ELAPSED_IN_MONTH(input_date DATE)
RETURNS INT
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            SELECT COUNT(*)
            FROM BUSINESS_CALENDAR
            WHERE date BETWEEN DATE_TRUNC('MONTH', input_date) AND input_date
            AND is_trading_day = 1
        )
    END
$$;


/**
 * Determines how many business days remain in the current month after the given date
 *
 * @param input_date The reference date
 * @return The count of business days remaining in the month after input_date
 *
 * Example: SELECT FN_BUSINESS_DAYS_REMAINING_IN_MONTH('2023-12-15'); -- Returns business days from Dec 16 to Dec 31
 */
CREATE OR REPLACE FUNCTION FN_BUSINESS_DAYS_REMAINING_IN_MONTH(input_date DATE)
RETURNS INT
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            SELECT COUNT(*)
            FROM BUSINESS_CALENDAR
            WHERE date BETWEEN DATEADD(DAY, 1, input_date) AND LAST_DAY(input_date)
            AND is_trading_day = 1
        )
    END
$$;


-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- SECTION 3: FISCAL CALENDAR FUNCTIONS
-- Functions specific to the Australian Fiscal Year calendar
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/**
 * Converts a date to its Australian Fiscal Year information
 * Returns a structured object with fiscal year details
 *
 * @param input_date The date to convert
 * @return JSON object with Australian Fiscal Year information
 *
 * Example: SELECT FN_AU_FISCAL_DATE_INFO('2023-12-15'); -- Returns {"au_fiscal_year_num": 2024, ...}
 */
CREATE OR REPLACE FUNCTION FN_AU_FISCAL_DATE_INFO(input_date DATE)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            SELECT OBJECT_CONSTRUCT(
                'au_fiscal_year_num', au_fiscal_year_num,
                'au_fiscal_year_desc', au_fiscal_year_desc,
                'au_fiscal_quarter_num', au_fiscal_quarter_num,
                'au_fiscal_quarter_desc', au_fiscal_quarter_desc,
                'au_fiscal_month_num', au_fiscal_month_num,
                'au_fiscal_week_num', au_fiscal_week_num,
                'au_fiscal_start_date_for_year', au_fiscal_start_date_for_year,
                'au_fiscal_end_date_for_year', au_fiscal_end_date_for_year
            )
            FROM BUSINESS_CALENDAR
            WHERE date = input_date
        )
    END
$$;


/**
 * Gets the same date in the previous fiscal year
 * If exact date doesn't exist (e.g., leap year Feb 29), returns the last day of the month
 *
 * @param input_date The reference date
 * @param n_years Number of fiscal years to go back (default 1)
 * @return The equivalent date in the previous fiscal year
 *
 * Example: SELECT FN_SAME_DAY_PREV_FISCAL_YEAR('2023-12-15'); -- Returns 2022-12-15
 * Example: SELECT FN_SAME_DAY_PREV_FISCAL_YEAR('2023-12-15', 2); -- Returns 2021-12-15
 */
CREATE OR REPLACE FUNCTION FN_SAME_DAY_PREV_FISCAL_YEAR(input_date DATE, n_years INT DEFAULT 1)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        WHEN n_years IS NULL OR n_years < 0 THEN
            NULL
        ELSE (
            WITH base_date AS (
                SELECT 
                    date,
                    month_num,
                    day_of_month_num,
                    au_fiscal_year_num,
                    days_in_month_count
                FROM BUSINESS_CALENDAR
                WHERE date = input_date
            ),
            target_date AS (
                SELECT 
                    TO_DATE(
                        (au_fiscal_year_num - n_years) || '-' || 
                        LPAD(month_num::STRING, 2, '0') || '-' || 
                        LPAD(LEAST(day_of_month_num, 
                                   -- Handle cases like Feb 29 in leap years
                                   (SELECT days_in_month_count 
                                    FROM BUSINESS_CALENDAR 
                                    WHERE year_num = (au_fiscal_year_num - n_years)
                                    AND month_num = base_date.month_num
                                    LIMIT 1)
                                   )::STRING, 2, '0')
                    ) AS result_date
                FROM base_date
            )
            SELECT result_date FROM target_date
        )
    END
$$;


/**
 * Returns the first day of the Australian Fiscal Year for a given date
 *
 * @param input_date The reference date
 * @return The first day (July 1) of the fiscal year that contains input_date
 *
 * Example: SELECT FN_FIRST_DAY_OF_FISCAL_YEAR('2023-12-15'); -- Returns 2023-07-01
 */
CREATE OR REPLACE FUNCTION FN_FIRST_DAY_OF_FISCAL_YEAR(input_date DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            SELECT au_fiscal_start_date_for_year
            FROM BUSINESS_CALENDAR
            WHERE date = input_date
        )
    END
$$;


/**
 * Returns the last day of the Australian Fiscal Year for a given date
 *
 * @param input_date The reference date
 * @return The last day (June 30) of the fiscal year that contains input_date
 *
 * Example: SELECT FN_LAST_DAY_OF_FISCAL_YEAR('2023-12-15'); -- Returns 2024-06-30
 */
CREATE OR REPLACE FUNCTION FN_LAST_DAY_OF_FISCAL_YEAR(input_date DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            SELECT au_fiscal_end_date_for_year
            FROM BUSINESS_CALENDAR
            WHERE date = input_date
        )
    END
$$;


/**
 * Returns the first business day of the Australian Fiscal Year for a given date
 *
 * @param input_date The reference date
 * @return The first business day of the fiscal year that contains input_date
 *
 * Example: SELECT FN_FIRST_BUSINESS_DAY_OF_FISCAL_YEAR('2023-12-15'); -- Returns the first business day on/after July 1, 2023
 */
CREATE OR REPLACE FUNCTION FN_FIRST_BUSINESS_DAY_OF_FISCAL_YEAR(input_date DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            WITH fiscal_year_start AS (
                SELECT au_fiscal_start_date_for_year
                FROM BUSINESS_CALENDAR
                WHERE date = input_date
            )
            SELECT MIN(date)
            FROM BUSINESS_CALENDAR, fiscal_year_start
            WHERE date >= fiscal_year_start.au_fiscal_start_date_for_year
            AND is_trading_day = 1
            AND au_fiscal_year_num = (
                SELECT au_fiscal_year_num
                FROM BUSINESS_CALENDAR
                WHERE date = input_date
            )
        )
    END
$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- SECTION 7: EXAMPLES & USAGE DEMOS
-- These queries demonstrate how to use the calendar functions
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Example 1: Find the next 5 business days from today
-- SELECT 
--     FN_ADD_BUSINESS_DAYS(CURRENT_DATE(), n) AS business_day,
--     n+1 AS business_day_number
-- FROM (
--     SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS n
--     FROM TABLE(GENERATOR(ROWCOUNT => 5))
-- );

-- Example 2: Calculate trading days between two months
-- WITH params AS (
--     SELECT 
--         TO_DATE('2023-01-01') AS start_month,
--         TO_DATE('2023-12-31') AS end_month
-- )
-- SELECT 
--     MONTHNAME(bc.date) AS month,
--     COUNT(CASE WHEN bc.is_trading_day = 1 THEN 1 END) AS trading_days,
--     COUNT(CASE WHEN bc.is_trading_day = 0 THEN 1 END) AS non_trading_days
-- FROM 
--     BUSINESS_CALENDAR bc,
--     params
-- WHERE 
--     bc.date BETWEEN DATE_TRUNC('MONTH', params.start_month) AND LAST_DAY(params.end_month)
-- GROUP BY 
--     bc.year_num, bc.month_num, MONTHNAME(bc.date)
-- ORDER BY 
--     bc.year_num, bc.month_num;

-- Example 3: Compare fiscal year-to-date performance
-- WITH current_fiscal_period AS (
--     SELECT 
--         au_fiscal_year_num,
--         au_fiscal_start_date_for_year,
--         CURRENT_DATE() AS current_date
--     FROM BUSINESS_CALENDAR
--     WHERE date = CURRENT_DATE()
-- )
-- SELECT 
--     'Current FYTD' AS period,
--     COUNT(CASE WHEN bc.is_trading_day = 1 THEN 1 END) AS trading_days_fytd
-- FROM 
--     BUSINESS_CALENDAR bc,
--     current_fiscal_period cfp
-- WHERE 
--     bc.date BETWEEN cfp.au_fiscal_start_date_for_year AND cfp.current_date
-- UNION ALL
-- SELECT 
--     'Previous FYTD' AS period,
--     COUNT(CASE WHEN bc.is_trading_day = 1 THEN 1 END) AS trading_days_fytd
-- FROM 
--     BUSINESS_CALENDAR bc,
--     current_fiscal_period cfp
-- WHERE 
--     bc.au_fiscal_year_num = cfp.au_fiscal_year_num - 1
-- AND bc.date BETWEEN bc.au_fiscal_start_date_for_year AND 
--     DATEADD(YEAR, -1, cfp.current_date);

-- Example 4: Map Christmas dates across multiple years to retail calendar
-- SELECT 
--     date AS christmas_day,
--     FN_RETAIL_DATE_INFO(date) AS retail_calendar_info
-- FROM BUSINESS_CALENDAR
-- WHERE month_num = 12 AND day_of_month_num = 25
-- AND year_num BETWEEN 2020 AND 2025
-- ORDER BY date;

-- Example 5: Find Easter dates and season info
-- SELECT 
--     year_num,
--     FN_GET_HOLIDAY_DATE(year_num, 'Good Friday') AS good_friday_date,
--     FN_FIRST_DAY_OF_RETAIL_SEASON(FN_GET_HOLIDAY_DATE(year_num, 'Good Friday')) AS easter_season_start,
--     FN_LAST_DAY_OF_RETAIL_SEASON(FN_GET_HOLIDAY_DATE(year_num, 'Good Friday')) AS easter_season_end,
--     FN_BUSINESS_DAYS_BETWEEN(
--         FN_FIRST_DAY_OF_RETAIL_SEASON(FN_GET_HOLIDAY_DATE(year_num, 'Good Friday')),
--         FN_LAST_DAY_OF_RETAIL_SEASON(FN_GET_HOLIDAY_DATE(year_num, 'Good Friday'))
--     ) AS business_days_in_easter_season
-- FROM (
--     SELECT DISTINCT year_num
--     FROM BUSINESS_CALENDAR
--     WHERE year_num BETWEEN 2020 AND 2025
-- )
-- ORDER BY year_num;


/**
 * Returns the last business day of the Australian Fiscal Year for a given date
 *
 * @param input_date The reference date
 * @return The last business day of the fiscal year that contains input_date
 *
 * Example: SELECT FN_LAST_BUSINESS_DAY_OF_FISCAL_YEAR('2023-12-15'); -- Returns the last business day on/before June 30, 2024
 */
CREATE OR REPLACE FUNCTION FN_LAST_BUSINESS_DAY_OF_FISCAL_YEAR(input_date DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            WITH fiscal_year_end AS (
                SELECT au_fiscal_end_date_for_year
                FROM BUSINESS_CALENDAR
                WHERE date = input_date
            )
            SELECT MAX(date)
            FROM BUSINESS_CALENDAR, fiscal_year_end
            WHERE date <= fiscal_year_end.au_fiscal_end_date_for_year
            AND is_trading_day = 1
            AND au_fiscal_year_num = (
                SELECT au_fiscal_year_num
                FROM BUSINESS_CALENDAR
                WHERE date = input_date
            )
        )
    END
$$;


-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- SECTION 4: RETAIL CALENDAR FUNCTIONS
-- Functions specific to the Retail 4-4-5 calendar
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/**
 * Converts a date to its Retail Calendar information
 * Returns a structured object with retail calendar details
 *
 * @param input_date The date to convert
 * @return JSON object with Retail Calendar information
 *
 * Example: SELECT FN_RETAIL_DATE_INFO('2023-12-15'); -- Returns {"retail_year_num": 2024, ...}
 */
CREATE OR REPLACE FUNCTION FN_RETAIL_DATE_INFO(input_date DATE)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            SELECT OBJECT_CONSTRUCT(
                'retail_year_num', retail_year_num,
                'retail_year_desc', retail_year_desc,
                'retail_half_num', retail_half_num,
                'retail_quarter_num', retail_quarter_num,
                'retail_period_num', retail_period_num,
                'retail_week_num', retail_week_num,
                'retail_start_of_year', retail_start_of_year,
                'retail_end_of_year', retail_end_of_year
            )
            FROM BUSINESS_CALENDAR
            WHERE date = input_date
        )
    END
$$;


/**
 * Returns all dates in a specific retail period (month)
 *
 * @param retail_year The retail year number
 * @param retail_period The retail period number (1-12)
 * @return Table of dates that fall within the specified retail period
 *
 * Example: SELECT * FROM TABLE(FN_GET_RETAIL_PERIOD_DATES(2023, 4)); -- Returns all dates in retail period 4 of 2023
 */
CREATE OR REPLACE FUNCTION FN_GET_RETAIL_PERIOD_DATES(retail_year INT, retail_period INT)
RETURNS TABLE(date DATE)
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN retail_year IS NULL OR retail_period IS NULL THEN 
            CAST(NULL AS DATE)
        WHEN retail_period < 1 OR retail_period > 12 THEN
            CAST(NULL AS DATE)
        ELSE date
    END
    FROM BUSINESS_CALENDAR
    WHERE retail_year_num = retail_year
    AND retail_period_num = retail_period
    ORDER BY date
$$;


/**
 * Returns the first day of the retail period (month) that contains the input date
 *
 * @param input_date The reference date
 * @return The first day of the retail period
 *
 * Example: SELECT FN_FIRST_DAY_OF_RETAIL_PERIOD('2023-12-15'); -- Returns the first day of retail period containing Dec 15
 */
CREATE OR REPLACE FUNCTION FN_FIRST_DAY_OF_RETAIL_PERIOD(input_date DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            WITH period_info AS (
                SELECT retail_year_num, retail_period_num
                FROM BUSINESS_CALENDAR
                WHERE date = input_date
            )
            SELECT MIN(date)
            FROM BUSINESS_CALENDAR, period_info
            WHERE retail_year_num = period_info.retail_year_num
            AND retail_period_num = period_info.retail_period_num
        )
    END
$$;


/**
 * Returns the last day of the retail period (month) that contains the input date
 *
 * @param input_date The reference date
 * @return The last day of the retail period
 *
 * Example: SELECT FN_LAST_DAY_OF_RETAIL_PERIOD('2023-12-15'); -- Returns the last day of retail period containing Dec 15
 */
CREATE OR REPLACE FUNCTION FN_LAST_DAY_OF_RETAIL_PERIOD(input_date DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            WITH period_info AS (
                SELECT retail_year_num, retail_period_num
                FROM BUSINESS_CALENDAR
                WHERE date = input_date
            )
            SELECT MAX(date)
            FROM BUSINESS_CALENDAR, period_info
            WHERE retail_year_num = period_info.retail_year_num
            AND retail_period_num = period_info.retail_period_num
        )
    END
$$;


-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- SECTION 5: RETAIL SEASON FUNCTIONS
-- Functions for working with retail seasons (Christmas, Easter, etc.)
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/**
 * Determines if a date falls within a specific retail season
 *
 * @param input_date The date to check
 * @param season_name The retail season name to check against (e.g., 'Christmas Season', 'Easter Season')
 * @return Boolean indicating if the date is in the specified season (1 for yes, 0 for no)
 *
 * Example: SELECT FN_IS_IN_RETAIL_SEASON('2023-12-15', 'Christmas Season'); -- Returns 1 (true)
 */
CREATE OR REPLACE FUNCTION FN_IS_IN_RETAIL_SEASON(input_date DATE, season_name STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL OR season_name IS NULL THEN 
            NULL
        ELSE (
            SELECT retail_season = season_name
            FROM BUSINESS_CALENDAR
            WHERE date = input_date
        )
    END
$$;


/**
 * Returns all dates for a specific retail season in a given year
 *
 * @param year The year to check
 * @param season_name The retail season name (e.g., 'Christmas Season', 'Easter Season')
 * @return Table of dates that fall within the specified retail season for the year
 *
 * Example: SELECT * FROM TABLE(FN_GET_RETAIL_SEASON_DATES(2023, 'Christmas Season')); -- Returns all Christmas season dates in 2023
 */
CREATE OR REPLACE FUNCTION FN_GET_RETAIL_SEASON_DATES(year INT, season_name STRING)
RETURNS TABLE(date DATE)
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN year IS NULL OR season_name IS NULL THEN 
            CAST(NULL AS DATE)
        ELSE date
    END
    FROM BUSINESS_CALENDAR
    WHERE year_num = year
    AND retail_season = season_name
    ORDER BY date
$$;


/**
 * Returns the first day of the retail season that contains the input date
 *
 * @param input_date The reference date
 * @return The first day of the retail season
 *
 * Example: SELECT FN_FIRST_DAY_OF_RETAIL_SEASON('2023-12-15'); -- Returns the first day of Christmas season
 */
CREATE OR REPLACE FUNCTION FN_FIRST_DAY_OF_RETAIL_SEASON(input_date DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            WITH season_info AS (
                SELECT retail_season, year_num
                FROM BUSINESS_CALENDAR
                WHERE date = input_date
            )
            SELECT MIN(date)
            FROM BUSINESS_CALENDAR, season_info
            WHERE retail_season = season_info.retail_season
            AND year_num = season_info.year_num
        )
    END
$$;


/**
 * Returns the last day of the retail season that contains the input date
 *
 * @param input_date The reference date
 * @return The last day of the retail season
 *
 * Example: SELECT FN_LAST_DAY_OF_RETAIL_SEASON('2023-12-15'); -- Returns the last day of Christmas season (Dec 24)
 */
CREATE OR REPLACE FUNCTION FN_LAST_DAY_OF_RETAIL_SEASON(input_date DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL THEN 
            NULL
        ELSE (
            WITH season_info AS (
                SELECT retail_season, year_num
                FROM BUSINESS_CALENDAR
                WHERE date = input_date
            )
            SELECT MAX(date)
            FROM BUSINESS_CALENDAR, season_info
            WHERE retail_season = season_info.retail_season
            AND year_num = season_info.year_num
        )
    END
$$;


-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- SECTION 6: DATE COMPARISON AND MAPPING FUNCTIONS
-- Functions for mapping between different date frameworks
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/**
 * Maps a date to its equivalent in a different calendar framework
 * Converts between Gregorian, AU Fiscal Year, and Retail 4-4-5 calendars
 *
 * @param input_date The date to map
 * @param source_calendar The source calendar type ('GREGORIAN', 'FISCAL', 'RETAIL')
 * @param target_calendar The target calendar type ('GREGORIAN', 'FISCAL', 'RETAIL')
 * @param preserve_day Boolean to determine if day-of-month should be preserved (default true)
 * @return JSON object with mapped date information
 *
 * Example: SELECT FN_MAP_ACROSS_CALENDARS('2023-12-15', 'GREGORIAN', 'FISCAL'); -- Maps Dec 15 to its fiscal equivalent
 */
/**
 * Gets the date for a specific day of week in a given week and year
 * Useful for finding dates like "First Monday of January" or "Last Friday of December"
 *
 * @param year The year
 * @param month The month (1-12)
 * @param day_of_week The day of week (1=Monday, 7=Sunday) using ISO convention
 * @param occurrence Which occurrence of the day in the month (1-5, negative counts from end)
 * @return The date of the specified day of week occurrence
 *
 * Example: SELECT FN_GET_DAY_OF_WEEK_IN_MONTH(2023, 1, 1, 1); -- Returns first Monday in January 2023
 * Example: SELECT FN_GET_DAY_OF_WEEK_IN_MONTH(2023, 12, 5, -1); -- Returns last Friday in December 2023
 */
CREATE OR REPLACE FUNCTION FN_GET_DAY_OF_WEEK_IN_MONTH(year INT, month INT, day_of_week INT, occurrence INT)
RETURNS DATE
LANGUAGE SQL
AS
$
    -- Input validation
    SELECT CASE
        WHEN year IS NULL OR month IS NULL OR day_of_week IS NULL OR occurrence IS NULL THEN 
            NULL
        WHEN month < 1 OR month > 12 THEN
            NULL
        WHEN day_of_week < 1 OR day_of_week > 7 THEN
            NULL
        WHEN occurrence = 0 THEN
            NULL
        ELSE (
            WITH month_days AS (
                SELECT date, iso_day_of_week_num,
                       RANK() OVER (PARTITION BY iso_day_of_week_num ORDER BY date) AS asc_rank,
                       RANK() OVER (PARTITION BY iso_day_of_week_num ORDER BY date DESC) AS desc_rank
                FROM BUSINESS_CALENDAR
                WHERE year_num = year
                AND month_num = month
                AND iso_day_of_week_num = day_of_week
            )
            SELECT date FROM month_days
            WHERE CASE
                WHEN occurrence > 0 THEN asc_rank = occurrence
                ELSE desc_rank = ABS(occurrence)
            END
        )
    END
$;


/**
 * Finds the date of a specific holiday in a given year
 * Use this for holidays that move around each year such as Good Friday or Labour Day
 *
 * @param year The year
 * @param holiday_name The holiday name (or partial name) to search for
 * @param jurisdiction Optional jurisdiction to limit search (e.g., 'NSW', 'VIC')
 * @return The date of the holiday in the specified year
 *
 * Example: SELECT FN_GET_HOLIDAY_DATE(2023, 'Good Friday'); -- Returns the date of Good Friday in 2023
 * Example: SELECT FN_GET_HOLIDAY_DATE(2023, 'Labour Day', 'NSW'); -- Returns the date of NSW Labour Day in 2023
 */
CREATE OR REPLACE FUNCTION FN_GET_HOLIDAY_DATE(year INT, holiday_name STRING, jurisdiction STRING DEFAULT NULL)
RETURNS DATE
LANGUAGE SQL
AS
$
    -- Input validation
    SELECT CASE
        WHEN year IS NULL OR holiday_name IS NULL THEN 
            NULL
        ELSE (
            WITH holiday_candidates AS (
                SELECT date, holiday_desc,
                       -- Calculate similarity score based on jurisdiction match
                       CASE
                           WHEN jurisdiction IS NOT NULL AND 
                                CONTAINS(UPPER(holiday_desc), 
                                         CONCAT('(', UPPER(jurisdiction), ')'))
                               THEN 2  -- Extra weight for jurisdiction match
                           ELSE 1
                       END * 
                       -- Combined with name match
                       CASE
                           WHEN CONTAINS(UPPER(holiday_desc), UPPER(holiday_name))
                               THEN 2  -- Direct match gets higher score
                           WHEN CONTAINS(UPPER(REGEXP_REPLACE(holiday_desc, '\\(.*\\)', '')), 
                                        UPPER(holiday_name))
                               THEN 1  -- Match without jurisdiction part
                           ELSE 0
                       END AS match_score
                FROM BUSINESS_CALENDAR
                WHERE year_num = year
                AND is_holiday = 1
            )
            -- Get the best match
            SELECT date
            FROM holiday_candidates
            WHERE match_score > 0
            ORDER BY match_score DESC, date
            LIMIT 1
        )
    END
$;


/**
 * Calculates the number of working days required to complete a task
 * Based on a daily completion percentage and including public holidays
 *
 * @param start_date The start date of the task
 * @param work_units The total work units to complete (e.g., story points, hours)
 * @param daily_capacity The work units that can be completed per business day
 * @param include_start_date Whether to include the start date in the calculation (default true)
 * @return The estimated completion date
 *
 * Example: SELECT FN_CALCULATE_COMPLETION_DATE('2023-12-01', 10, 2); -- Returns the date after completing 10 units at 2 per day
 */
CREATE OR REPLACE FUNCTION FN_CALCULATE_COMPLETION_DATE(
    start_date DATE, 
    work_units FLOAT, 
    daily_capacity FLOAT,
    include_start_date BOOLEAN DEFAULT TRUE)
RETURNS DATE
LANGUAGE SQL
AS
$
    -- Input validation
    SELECT CASE
        WHEN start_date IS NULL OR work_units IS NULL OR daily_capacity IS NULL THEN 
            NULL
        WHEN daily_capacity <= 0 THEN
            NULL  -- Prevent division by zero
        ELSE (
            -- Calculate days needed (ceiling of work_units / daily_capacity)
            WITH days_needed AS (
                SELECT CEILING(work_units / daily_capacity) AS days
            ),
            -- Get suitable start date (either the given date or the next business day)
            adjusted_start AS (
                SELECT 
                    CASE WHEN include_start_date AND 
                              EXISTS (SELECT 1 FROM BUSINESS_CALENDAR 
                                      WHERE date = start_date AND is_trading_day = 1) 
                         THEN start_date
                         ELSE (SELECT MIN(date) FROM BUSINESS_CALENDAR 
                               WHERE date >= start_date AND is_trading_day = 1)
                    END AS work_start_date
            ),
            -- Get completion date by adding required business days
            completion AS (
                SELECT FN_ADD_BUSINESS_DAYS(work_start_date, days - CASE WHEN include_start_date THEN 1 ELSE 0 END)
                FROM days_needed, adjusted_start
            )
            SELECT * FROM completion
        )
    END
$;


/**
 * Maps a date to its equivalent in a different calendar framework
 * Converts between Gregorian, AU Fiscal Year, and Retail 4-4-5 calendars
 *
 * @param input_date The date to map
 * @param source_calendar The source calendar type ('GREGORIAN', 'FISCAL', 'RETAIL')
 * @param target_calendar The target calendar type ('GREGORIAN', 'FISCAL', 'RETAIL')
 * @param preserve_day Boolean to determine if day-of-month should be preserved (default true)
 * @return JSON object with mapped date information
 *
 * Example: SELECT FN_MAP_ACROSS_CALENDARS('2023-12-15', 'GREGORIAN', 'FISCAL'); -- Maps Dec 15 to its fiscal equivalent
 */
CREATE OR REPLACE FUNCTION FN_MAP_ACROSS_CALENDARS(
    input_date DATE, 
    source_calendar STRING, 
    target_calendar STRING,
    preserve_day BOOLEAN DEFAULT TRUE)
RETURNS OBJECT
LANGUAGE SQL
AS
$
    -- Input validation
    SELECT CASE
        WHEN input_date IS NULL OR source_calendar IS NULL OR target_calendar IS NULL THEN 
            NULL
        WHEN UPPER(source_calendar) NOT IN ('GREGORIAN', 'FISCAL', 'RETAIL') OR
             UPPER(target_calendar) NOT IN ('GREGORIAN', 'FISCAL', 'RETAIL') THEN
            NULL
        WHEN UPPER(source_calendar) = UPPER(target_calendar) THEN
            -- If source and target are the same, just return the input date info
            OBJECT_CONSTRUCT(
                'input_date', input_date,
                'mapped_date', input_date,
                'source_calendar', UPPER(source_calendar),
                'target_calendar', UPPER(target_calendar),
                'mapping_type', 'IDENTICAL'
            )
        ELSE (
            -- Get the source date information
            WITH source_info AS (
                SELECT 
                    date,
                    year_num,
                    month_num,
                    day_of_month_num,
                    au_fiscal_year_num,
                    au_fiscal_month_num,
                    retail_year_num,
                    retail_period_num
                FROM BUSINESS_CALENDAR
                WHERE date = input_date
            ),
            -- Map to target calendar
            target_date AS (
                SELECT 
                    CASE 
                        -- Gregorian to Fiscal mapping
                        WHEN UPPER(source_calendar) = 'GREGORIAN' AND UPPER(target_calendar) = 'FISCAL' THEN
                            (SELECT date FROM BUSINESS_CALENDAR
                             WHERE au_fiscal_year_num = source_info.au_fiscal_year_num
                             AND au_fiscal_month_num = source_info.au_fiscal_month_num
                             AND (
                                 CASE WHEN preserve_day 
                                     -- Try to preserve day of month
                                     THEN day_of_month_num = LEAST(source_info.day_of_month_num, 
                                                                 (SELECT MAX(day_of_month_num)
                                                                  FROM BUSINESS_CALENDAR
                                                                  WHERE au_fiscal_year_num = source_info.au_fiscal_year_num
                                                                  AND au_fiscal_month_num = source_info.au_fiscal_month_num))
                                     -- Or get first day of equivalent fiscal month
                                     ELSE day_of_month_num = 1
                                 END
                             )
                             LIMIT 1)
                             
                        -- Gregorian to Retail mapping
                        WHEN UPPER(source_calendar) = 'GREGORIAN' AND UPPER(target_calendar) = 'RETAIL' THEN
                            (SELECT date FROM BUSINESS_CALENDAR
                             WHERE retail_year_num = source_info.retail_year_num
                             AND retail_period_num = source_info.retail_period_num
                             AND (
                                 CASE WHEN preserve_day 
                                     -- For retail calendar, try to get same position in period
                                     THEN DATEDIFF(DAY, 
                                                (SELECT MIN(date) FROM BUSINESS_CALENDAR
                                                 WHERE retail_year_num = source_info.retail_year_num
                                                 AND retail_period_num = source_info.retail_period_num),
                                                date) =
                                          LEAST(DATEDIFF(DAY, 
                                                     (SELECT MIN(date) FROM BUSINESS_CALENDAR
                                                      WHERE retail_year_num = source_info.retail_year_num
                                                      AND retail_period_num = source_info.retail_period_num),
                                                     input_date),
                                                (SELECT DATEDIFF(DAY, 
                                                             MIN(date), 
                                                             MAX(date))
                                                 FROM BUSINESS_CALENDAR
                                                 WHERE retail_year_num = source_info.retail_year_num
                                                 AND retail_period_num = source_info.retail_period_num))
                                     -- Or get first day of equivalent retail period
                                     ELSE date = (SELECT MIN(date) FROM BUSINESS_CALENDAR
                                                 WHERE retail_year_num = source_info.retail_year_num
                                                 AND retail_period_num = source_info.retail_period_num)
                                 END
                             )
                             LIMIT 1)
                             
                        -- Fiscal to Gregorian mapping
                        WHEN UPPER(source_calendar) = 'FISCAL' AND UPPER(target_calendar) = 'GREGORIAN' THEN
                            (SELECT date FROM BUSINESS_CALENDAR
                             WHERE year_num = source_info.year_num
                             AND month_num = (
                                 -- Convert fiscal month to calendar month
                                 CASE 
                                     WHEN source_info.au_fiscal_month_num <= 6 
                                         THEN source_info.au_fiscal_month_num + 6
                                     ELSE source_info.au_fiscal_month_num - 6
                                 END
                             )
                             AND (
                                 CASE WHEN preserve_day 
                                     THEN day_of_month_num = LEAST(source_info.day_of_month_num, 
                                                                 (SELECT MAX(day_of_month_num)
                                                                  FROM BUSINESS_CALENDAR
                                                                  WHERE year_num = source_info.year_num
                                                                  AND month_num = (
                                                                     CASE 
                                                                         WHEN source_info.au_fiscal_month_num <= 6 
                                                                             THEN source_info.au_fiscal_month_num + 6
                                                                         ELSE source_info.au_fiscal_month_num - 6
                                                                     END
                                                                  )))
                                     ELSE day_of_month_num = 1
                                 END
                             )
                             LIMIT 1)
                             
                        -- Fiscal to Retail mapping (via Gregorian)
                        WHEN UPPER(source_calendar) = 'FISCAL' AND UPPER(target_calendar) = 'RETAIL' THEN
                            -- Two-step mapping via Gregorian
                            (SELECT mapped_date FROM TABLE(FN_MAP_ACROSS_CALENDARS(
                                (SELECT mapped_date FROM TABLE(FN_MAP_ACROSS_CALENDARS(
                                    input_date, 'FISCAL', 'GREGORIAN', preserve_day
                                ))),
                                'GREGORIAN', 'RETAIL', preserve_day
                            )))
                            
                        -- Retail to Gregorian mapping
                        WHEN UPPER(source_calendar) = 'RETAIL' AND UPPER(target_calendar) = 'GREGORIAN' THEN
                            -- For Retail to Gregorian, map to closest calendar month
                            (SELECT date FROM BUSINESS_CALENDAR
                             WHERE year_num = (
                                 CASE 
                                     -- Handle fiscal year crossover for retail calendar
                                     WHEN source_info.retail_period_num >= 7 
                                         THEN source_info.retail_year_num - 1
                                     ELSE source_info.retail_year_num
                                 END
                             )
                             AND month_num = (
                                 -- Convert retail period to closest calendar month
                                 CASE source_info.retail_period_num
                                     WHEN 1 THEN 7 -- July
                                     WHEN 2 THEN 8 -- August
                                     WHEN 3 THEN 9 -- September
                                     WHEN 4 THEN 10 -- October
                                     WHEN 5 THEN 11 -- November
                                     WHEN 6 THEN 12 -- December
                                     WHEN 7 THEN 1 -- January
                                     WHEN 8 THEN 2 -- February
                                     WHEN 9 THEN 3 -- March
                                     WHEN 10 THEN 4 -- April
                                     WHEN 11 THEN 5 -- May
                                     WHEN 12 THEN 6 -- June
                                 END
                             )
                             AND (
                                 CASE WHEN preserve_day 
                                     THEN DATEDIFF(DAY, 
                                                (SELECT MIN(date) FROM BUSINESS_CALENDAR
                                                 WHERE year_num = (
                                                     CASE 
                                                         WHEN source_info.retail_period_num >= 7 
                                                             THEN source_info.retail_year_num - 1
                                                         ELSE source_info.retail_year_num
                                                     END
                                                 )
                                                 AND month_num = (
                                                     CASE source_info.retail_period_num
                                                         WHEN 1 THEN 7 WHEN 2 THEN 8 WHEN 3 THEN 9
                                                         WHEN 4 THEN 10 WHEN 5 THEN 11 WHEN 6 THEN 12
                                                         WHEN 7 THEN 1 WHEN 8 THEN 2 WHEN 9 THEN 3
                                                         WHEN 10 THEN 4 WHEN 11 THEN 5 WHEN 12 THEN 6
                                                     END
                                                 )),
                                                date) =
                                          LEAST(DATEDIFF(DAY, 
                                                     (SELECT MIN(date) FROM BUSINESS_CALENDAR
                                                      WHERE retail_year_num = source_info.retail_year_num
                                                      AND retail_period_num = source_info.retail_period_num),
                                                     input_date),
                                                (SELECT DATEDIFF(DAY, 
                                                             MIN(date), 
                                                             MAX(date))
                                                 FROM BUSINESS_CALENDAR
                                                 WHERE year_num = (
                                                     CASE 
                                                         WHEN source_info.retail_period_num >= 7 
                                                             THEN source_info.retail_year_num - 1
                                                         ELSE source_info.retail_year_num
                                                     END
                                                 )
                                                 AND month_num = (
                                                     CASE source_info.retail_period_num
                                                         WHEN 1 THEN 7 WHEN 2 THEN 8 WHEN 3 THEN 9
                                                         WHEN 4 THEN 10 WHEN 5 THEN 11 WHEN 6 THEN 12
                                                         WHEN 7 THEN 1 WHEN 8 THEN 2 WHEN 9 THEN 3
                                                         WHEN 10 THEN 4 WHEN 11 THEN 5 WHEN 12 THEN 6
                                                     END
                                                 )))
                                     ELSE date = (SELECT MIN(date) FROM BUSINESS_CALENDAR
                                                 WHERE year_num = (
                                                     CASE 
                                                         WHEN source_info.retail_period_num >= 7 
                                                             THEN source_info.retail_year_num - 1
                                                         ELSE source_info.retail_year_num
                                                     END
                                                 )
                                                 AND month_num = (
                                                     CASE source_info.retail_period_num
                                                         WHEN 1 THEN 7 WHEN 2 THEN 8 WHEN 3 THEN 9
                                                         WHEN 4 THEN 10 WHEN 5 THEN 11 WHEN 6 THEN 12
                                                         WHEN 7 THEN 1 WHEN 8 THEN 2 WHEN 9 THEN 3
                                                         WHEN 10 THEN 4 WHEN 11 THEN 5 WHEN 12 THEN 6
                                                     END
                                                 ))
                                 END
                             )
                             LIMIT 1)
                            
                        -- Retail to Fiscal mapping (via Gregorian)
                        WHEN UPPER(source_calendar) = 'RETAIL' AND UPPER(target_calendar) = 'FISCAL' THEN
                            -- Two-step mapping via Gregorian
                            (SELECT mapped_date FROM target_date
                             WHERE source_calendar = 'RETAIL'
                             AND target_calendar = 'GREGORIAN'
                             LIMIT 1)
                            
                        ELSE NULL
                    END AS mapped_date
                FROM source_info
            )
            
            SELECT OBJECT_CONSTRUCT(
                'input_date', input_date,
                'mapped_date', (SELECT mapped_date FROM target_date),
                'source_calendar', UPPER(source_calendar),
                'target_calendar', UPPER(target_calendar),
                'mapping_type', CASE 
                                    WHEN preserve_day THEN 'POSITION_PRESERVING'
                                    ELSE 'FIRST_DAY'
                                END
            )
        )
    END
$;
