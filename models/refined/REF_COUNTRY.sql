with 
current_from_snapshot as (
    select * EXCLUDE (DBT_SCD_ID, DBT_UPDATED_AT,
                        DBT_VALID_FROM, DBT_VALID_TO)
    FROM {{ ref('SNSH_COUNTRY') }}
    WHERE DBT_valid_to is null
)
select * from current_from_snapshot