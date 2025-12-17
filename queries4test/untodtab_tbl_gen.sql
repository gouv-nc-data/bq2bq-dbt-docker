CREATE OR REPLACE TABLE sydonia_opendata.untodtab AS
select 
  tod_cod
  --,valid_from
  --,valid_to
  ,tod_dsc
  --,tod_dsc2
  --,tod_dsc3
  --,modify_time
  --,flg_rem
from sydonia.untodtab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE