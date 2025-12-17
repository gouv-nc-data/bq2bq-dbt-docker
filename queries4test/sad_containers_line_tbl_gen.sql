CREATE OR REPLACE TABLE sydonia_opendata.sad_containers_line AS
SELECT *
FROM sydonia.sad_containers_line
WHERE instanceid IN (
  SELECT g.instanceid
  FROM (
    SELECT sg.*,
           DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) as p_date
    FROM sydonia.sad_general_segment sg
  ) g
  WHERE (
    COALESCE(ide_pst_dat, ide_ast_dat) >= p_date
    OR ide_rcp_dat >= p_date
    OR (ide_ast_dat IS NULL AND ide_reg_dat IS NOT NULL)
    OR (pty_sts = 'Cancelled')
  )
)
