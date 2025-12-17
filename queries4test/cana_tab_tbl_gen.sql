CREATE OR REPLACE TABLE sydonia_opendata.cana_tab AS
select 
  cod
  --valid_from
  --valid_to
  ,can_nam
  ,can_dsc
  --modify_time
  --flg_rem
from sydonia.cana_tab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE