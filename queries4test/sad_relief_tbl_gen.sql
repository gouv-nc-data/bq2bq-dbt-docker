CREATE OR REPLACE TABLE sydonia_opendata.sad_relief AS
SELECT *
FROM sydonia.sad_relief
WHERE instanceid IN (
  SELECT g.instanceid
  FROM (
    SELECT 
      sg.*, 
      DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AS p_date
    FROM sydonia.sad_general_segment sg
  ) g
  WHERE 
    (COALESCE(ide_pst_dat, ide_ast_dat) >= p_date
     OR ide_rcp_dat >= p_date
     OR (ide_ast_dat IS NULL AND ide_reg_dat IS NOT NULL)  -- toutes les provisoires
     OR (pty_sts = 'Cancelled')                            -- toutes les annulées
    )
)
