CREATE OR REPLACE TABLE sydonia_opendata.unctntab AS
select 
  ctn_typ
  --,valid_from
  --,valid_to
  ,ctn_dsc
  --,modify_time
  --,flg_rem
from sydonia.unctntab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE