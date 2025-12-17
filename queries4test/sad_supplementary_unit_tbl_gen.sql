CREATE OR REPLACE TABLE sydonia_opendata.sad_supplementary_unit AS
SELECT 
  instanceid,
  key_itm_nbr,
  key_sup_rnk,
  tar_sup_cod,
  tar_sup_nam,
  tar_sup_qty
FROM sydonia.sad_supplementary_unit
WHERE instanceid IN (
  SELECT g.instanceid
  FROM (
    SELECT sg.*, DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AS p_date
    FROM sydonia.sad_general_segment sg
  ) g
  WHERE (
    COALESCE(ide_pst_dat, ide_ast_dat) >= p_date
    OR ide_rcp_dat >= p_date
    OR (ide_ast_dat IS NULL AND ide_reg_dat IS NOT NULL)
    OR (pty_sts = 'Cancelled')
  )
)
