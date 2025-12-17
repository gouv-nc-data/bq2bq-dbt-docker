CREATE OR REPLACE TABLE sydonia_opendata.untartab AS
select 
  hs6_cod
  ,tar_pr1
  ,tar_pr2
  ,tar_pr3
  ,tar_pr4
  ,valid_from
  ,valid_to
  ,tar_all
  ,tar_dsc
  ,uom_cod1
  ,uom_cod2
  ,uom_cod3
  ,not_cod
  ,mkt_cod
  ,rul_cod
  ,tar_t01
  ,tar_t02
  ,tar_t03
  ,tar_t04
  ,tar_t05
  ,tar_t06
  ,tar_t07
  ,tar_t08
  ,tar_t09
  ,tar_t10
  ,tar_t11
  ,tar_t12
  ,tar_t13
  ,tar_t14
  ,tar_t15
  --,user_name
  --,operation_name
  --,operation_date
  --,ignore_hs
  --,modify_time
  --,flg_rem
from sydonia.untartab 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE
  and tar_pr1 not like '%*%' and tar_pr2 not like '%*%'