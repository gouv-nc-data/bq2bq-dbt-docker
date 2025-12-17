CREATE OR REPLACE TABLE sydonia_opendata.untr2tab AS
select 
  tr1_cod
  ,tr2_cod
  --,valid_from
  --,valid_to
  ,tr2_dsc
  --,tr2_dsc2
  --,tr2_dsc3
  --,modify_time
  --,flg_rem
from sydonia.untr2tab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE