CREATE OR REPLACE TABLE sydonia_opendata.sad_item_blk AS
SELECT
  instance_id,
  dec_ref_yer,
  ide_cuo_cod,
  dec_cod,
  dec_ref_nbr,
  key_itm_nbr,
  col_adm,
  cri_cod,
  cri_typ
FROM sydonia.sad_itm_blk
WHERE instance_id IN (
  SELECT g.instanceid
  FROM (
    SELECT
      sg.*,
      DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AS p_date
    FROM sydonia.sad_general_segment sg
  ) g
  WHERE
    (
      COALESCE(ide_pst_dat, ide_ast_dat) >= g.p_date
      OR ide_rcp_dat >= g.p_date
      OR (ide_ast_dat IS NULL AND ide_reg_dat IS NOT NULL)  -- toutes les provisoires
      OR pty_sts = 'Cancelled'  -- toutes les annulées
    )
)
