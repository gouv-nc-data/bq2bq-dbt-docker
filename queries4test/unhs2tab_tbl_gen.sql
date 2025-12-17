CREATE OR REPLACE TABLE sydonia_opendata.unhs2tab AS
select 
  hs2_cod
  ,valid_from
  ,valid_to
  ,hs2_dsc
  ,hs1_cod
  ,hs2_txt
  ,not_cod
  ,modify_time
  ,'false' as flg_rem
from sydonia.unhs2tab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE