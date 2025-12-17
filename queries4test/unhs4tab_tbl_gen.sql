CREATE OR REPLACE TABLE sydonia_opendata.unhs4tab AS
select 
hs4_cod
,valid_from
,valid_to
,hs4_dsc
,hs2_cod
,hs3_cod
,modify_time
,'false' as flg_rem
from sydonia.unhs4tab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE