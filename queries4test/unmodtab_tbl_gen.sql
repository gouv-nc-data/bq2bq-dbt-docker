CREATE OR REPLACE TABLE sydonia_opendata.unmodtab AS
select 
  mod_cod
  ,cp1_cod
  --,valid_from
  --,valid_to
  ,mod_dsc
  --,mod_dsc2
  --,mod_dsc3
  ,mod_flw
  --,cus_ser
  --,ass_ser
  --,mod_con
  --,modify_time
  --,flg_rem
  --,simplified_sad
from sydonia.unmodtab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE