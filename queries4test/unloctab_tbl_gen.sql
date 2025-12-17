CREATE OR REPLACE TABLE sydonia_opendata.unloctab AS
select 
  loc_cod
  --,valid_from
  --,valid_to
  ,loc_dsc
  --,loc_dsc2
  --,loc_dsc3
  --,modify_time
  --,flg_rem
from sydonia.unloctab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE