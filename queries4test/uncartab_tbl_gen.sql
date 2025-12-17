CREATE OR REPLACE TABLE sydonia_opendata.uncartab AS
select 
  car_cod
  --,valid_from
  --,valid_to
  ,car_nam
  ,car_adr
  ,car_ad2
  ,car_ad3
  ,car_ad4
  ,car_tel
  ,car_fax
  ,car_tlx
  ,cmp_cod
  --,modify_time
  --,flg_rem
from sydonia.uncartab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE