CREATE OR REPLACE TABLE sydonia_opendata.untoptab AS
select 
  top_cod
  --,valid_from
  --,valid_to
  ,top_dsc
  ,top_dsc2
  ,top_dsc3
  --,modify_time
  --,flg_rem
from sydonia.untoptab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE