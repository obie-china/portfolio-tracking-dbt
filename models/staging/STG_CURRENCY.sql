{{ config(materialized='ephemeral') }}

with src_data as (
    SELECT
    ALPHABETICCODE as ALPHABETIC_CODE --VARCHAR(16777216),
	, NUMERICCODE as NUMERIC_CODE --NUMBER(38,0),
	, DECIMALDIGITS as DECIMAL_DIGITS --NUMBER(38,0),
	, CURRENCYNAME as CURRENCY_NAME -- VARCHAR(16777216),
	, LOCATIONS as LOCATIONS --VARCHAR(16777216),
	, LOAD_TS as LOAD_TS --TIMESTAMP_NTZ(9)
    , 'SEED.CURRENCY' as RECORD_SOURCE
    FROM {{ source("seeds", "CURRENCY") }}
),
default_record as (
    SELECT 
	     'Missing' as ALPHABETIC_CODE 
	    , '-1' as NUMERIC_CODE 
	    , '-1' as DECIMAL_DIGITS
	    , 'Missing' as CURRENCY_NAME 
	    , 'Missing' as LOCATIONS 
	    , '2020-01-01' as LOAD_TS
        , 'System.DefaultKey' as RECORD_SOURCE
),
hashed as (
    SELECT
        concat_ws('|', ALPHABETIC_CODE) as CURRENCY_HKEY
      , concat_ws('|', ALPHABETIC_CODE,
                       NUMERIC_CODE)
        as CURRENCY_HDIFF
        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM src_data
)
SELECT * FROM hashed