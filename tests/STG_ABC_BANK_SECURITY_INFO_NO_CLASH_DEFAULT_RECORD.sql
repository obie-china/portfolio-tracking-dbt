select *
from {{ ref('STG_ABC_BANK_SECURITY_INFO') }}
WHERe SECURITY_CODE = '-1'
and RECORD_SOURCE != 'System.DefaultKey'