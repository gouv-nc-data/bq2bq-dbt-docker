CREATE OR REPLACE TABLE sydonia_opendata.unhs1tab AS
select 
  hs1_cod
  ,valid_from
  ,valid_to
  ,hs1_dsc
  ,hs1_txt
  ,not_cod
  ,modify_time
  ,'false' as flg_rem
from sydonia.unhs1tab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE