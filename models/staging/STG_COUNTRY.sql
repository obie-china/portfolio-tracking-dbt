{{ config(materialized='ephemeral') }}

with src_data as (
    SELECT *
    , 'SEED.COUNTRY' as RECORD_SOURCE
    FROM {{ source("seeds", "COUNTRY") }}
),
default_record as (
    SELECT 
	     'Missing' as COUNTRY_NAME
	    , 'Missing' as COUNTRY_CODE_2_LETTER  
	    , 'Missing' as COUNTRY_CODE_3_LETTER
	    , '-1'      as ISO_3166_2
        , 'Missing' as REGION
	    , 'Missing' as SUB_REGION 
	    , 'Missing' as INTERMEDIATE_REGION
        , 'Missing' as INTERMEDIATE_REGION
	    , '-1'      as REGION_CODE 
	    , '-1'      as SUB_REGION_CODE
	    , '-1'      as INTERMEDIATE_REGION_CODE
	    , '2020-01-01'as LOAD_TS
        , 'System.DefaultKey' as RECORD_SOURCE
),
hashed as (
    SELECT
        concat_ws('|', COUNTRY_CODE_2_LETTER, COUNTRY_CODE_3_LETTER, ISO_3166_2) as COUNTRY_HKEY
      , concat_ws('|', COUNTRY_CODE_2_LETTER, COUNTRY_CODE_3_LETTER, ISO_3166_2) as COUNTRY_HDIFF
        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM src_data
)
SELECT * FROM hashed