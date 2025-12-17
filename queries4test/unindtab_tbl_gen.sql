CREATE OR REPLACE TABLE sydonia_opendata.unindtab AS
select 
  ind_cod
  --valid_from
  --valid_to
  ,ind_dsc
  --modify_time
  --flg_rem
from sydonia.unindtab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE