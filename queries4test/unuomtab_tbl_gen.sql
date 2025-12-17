CREATE OR REPLACE TABLE sydonia_opendata.unuomtab AS
select 
  uom_cod
  ,uom_dsc
from sydonia.unuomtab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE