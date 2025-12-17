CREATE OR REPLACE TABLE sydonia_opendata.unhs6tab AS
select 
  hs6_cod
  ,valid_from
  ,valid_to
  ,hs6_dsc
  ,hs4_cod
  ,hs5_cod
  ,modify_time
  ,'false' as flg_rem
from sydonia.unhs6tab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE