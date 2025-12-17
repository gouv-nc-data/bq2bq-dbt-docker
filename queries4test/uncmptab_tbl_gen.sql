CREATE OR REPLACE TABLE sydonia_opendata.uncmptab AS
select 
  cmp_cod
  --valid_from
  --valid_to
  ,cmp_nam
  ,cmp_adr
  ,cmp_ad2
  ,cmp_ad3
  ,cmp_ad4
  ,cmp_tel
  ,cmp_fax
  ,cmp_tlx
  ,cmp_sta
  --modify_time
  --flg_rem
from sydonia.uncmptab 
where valid_to is null 
  and flg_rem = false