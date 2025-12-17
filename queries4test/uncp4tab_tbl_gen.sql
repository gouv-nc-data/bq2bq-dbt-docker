CREATE OR REPLACE TABLE sydonia_opendata.uncp4tab AS
select 
  cp4_cod
  --,valid_from
  --,valid_to
  ,cp4_dsc
  --,cp4_dsc2
  --,cp4_dsc3
  ,cpr_cod
  ,cpp_cod
  ,spe_tra
  ,gen_tra
  --,modify_time
  --,flg_rem
from sydonia.uncp4tab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE