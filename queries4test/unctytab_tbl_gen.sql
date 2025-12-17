CREATE OR REPLACE TABLE sydonia_opendata.unctytab AS
select 
  cty_cod
  --,valid_from
  --,valid_to
  ,cty_dsc
  --,cty_dsc2
  --,cty_dsc3
  --,rul_cod
  --,modify_time
  --,flg_rem
from sydonia.unctytab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE