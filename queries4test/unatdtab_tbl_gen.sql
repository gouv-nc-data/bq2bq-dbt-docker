CREATE OR REPLACE TABLE sydonia_opendata.unatdtab AS
select 
  atd_cod
  ,atd_dsc
  ,atd_dsc2
  ,atd_dsc3
  --,valid_from
  --,valid_to
  --,modify_time
  --,flg_rem
from sydonia.unatdtab
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE