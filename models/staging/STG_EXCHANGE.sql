{{ config(materialized='ephemeral') }}
with src_data as (
    SELECT
        NAME as NAME --VARCHAR(16777216),
	    , ID as ID --VARCHAR(16777216),
	    , COUNTRY as COUNTRY -- VARCHAR(16777216),
	    , CITY as CITY --VARCHAR(16777216),
	    , ZONE as ZONE --VARCHAR(16777216),
	    , DELTA as DELTA --FLOAT,
	    , DST_PERIOD as DST_PERIOD -- VARCHAR(16777216),
	    , OPEN as OPEN --VARCHAR(16777216),
	    , CLOSE as CLOSE --VARCHAR(16777216),
	    , LUNCH as LUNCH --VARCHAR(16777216),
	    , OPEN_UTC as OPEN_UTC --VARCHAR(16777216),
	    , CLOSE_UTC as CLOSE_UTC --VARCHAR(16777216),
	    , LUNCH_UTC as LUNCH_UTC --VARCHAR(16777216),
	    , LOAD_TS as LOAD_TS  --TIMESTAMP_NTZ(9)
        , 'SEED.EXCHANGE' as RECORD_SOURCE
    FROM {{ source("seeds", "EXCHANGE") }}
),
default_record as (
    SELECT 
	     'Missing' as ID 
	    , 'Missing' as COUNTRY 
	    , 'Missing' as CITY
	    , 'Missing' as ZONE 
	    , '-1' as DELTA 
	    , 'Missing' as DST_PERIOD 
	    , 'Missing' as OPEN 
	    , 'Missing' as CLOSE 
	    , 'Missing' as LUNCH 
	    , 'Missing' as OPEN_UTC 
	    , 'Missing' as CLOSE_UTC 
	    , 'Missing' as LUNCH_UTC 
	    , '2020-01-01' as LOAD_TS
        , 'System.DefaultKey' as RECORD_SOURCE
),
hashed as (
    SELECT
        concat_ws('|', ID) as EXCHANGE_HKEY
      , concat_ws('|', ID,
                       COUNTRY, CITY,
                       ZONE )
        as EXCHANGE_HDIFF
        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM src_data
)
SELECT * FROM hashed
