CREATE OR REPLACE TABLE sydonia_opendata.unshdtab AS
select 
  shd_cod
  --,valid_from
  --,valid_to
  ,shd_nam
  ,shd_nam2
  ,shd_nam3
  ,shd_adr
  ,shd_ad2
  ,shd_ad3
  ,shd_ad4
  ,shd_tel
  ,shd_fax
  ,shd_tlx
  ,shd_pub
  --,modify_time
  --,flg_rem
from sydonia.unshdtab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE