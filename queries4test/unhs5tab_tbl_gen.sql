CREATE OR REPLACE TABLE sydonia_opendata.unhs5tab AS
select 
  hs5_cod
  ,valid_from
  ,valid_to
  ,hs5_dsc
  ,hs4_cod
  ,modify_time
  ,'false' as flg_rem
from sydonia.unhs5tab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE