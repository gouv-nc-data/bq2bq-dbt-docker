CREATE OR REPLACE TABLE sydonia_opendata.unpkgtab AS
SELECT 
pkg_cod
--, valid_from
--, valid_to
, pkg_dsc
--, pkg_dsc2
--, pkg_dsc3
, pkg_blk
--, modify_time
--, flg_rem 
FROM 
  `sydonia.unpkgtab`
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE
