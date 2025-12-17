CREATE OR REPLACE TABLE sydonia_opendata.unprftab AS
select 
  prf_cod
  --,valid_from
  --,valid_to
  ,prf_dsc
  --,prf_dsc2
  --,prf_dsc3
  --,rul_cod
  --,prf_quo
  --,modify_time
  --,flg_rem
from sydonia.unprftab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE