CREATE OR REPLACE TABLE sydonia_opendata.untr1tab AS
select 
  tr1_cod
  --,valid_from
  --,valid_to
  ,tr1_dsc
  --,tr1_dsc2
  --,tr1_dsc3
  --,modify_time
  --,flg_rem
from sydonia.untr1tab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE