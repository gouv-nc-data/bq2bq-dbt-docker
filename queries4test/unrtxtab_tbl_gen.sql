CREATE OR REPLACE TABLE sydonia_opendata.unrtxtab AS
select 
rtx_cod
--,valid_from
--,valid_to
,rtx_dsc
,rtx_rat
--,modify_time
--,flg_rem
from sydonia.unrtxtab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE