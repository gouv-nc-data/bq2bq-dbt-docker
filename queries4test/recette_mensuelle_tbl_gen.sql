CREATE OR REPLACE TABLE sydonia_opendata.recette_mensuelle AS
WITH liquidation AS (
  SELECT 
    FORMAT_DATE('%Y', DATE(g.ide_ast_dat)) AS annee,
    FORMAT_DATE('%m', DATE(g.ide_ast_dat)) AS mois,
    t.tax_lin_cod AS code,
    SUM(t.tax_lin_amt) AS montant
  FROM `sydonia.sad_general_segment` g
  LEFT JOIN (
    SELECT instanceid, tax_lin_cod, tax_lin_amt, tax_lin_mop 
    FROM `sydonia.sad_tax`
    UNION ALL
    SELECT instanceid, tax_cod AS tax_lin_cod, tax_amt AS tax_lin_amt, tax_mop AS tax_lin_mop FROM sydonia.sad_global_taxes
  ) t
  ON g.instanceid = t.instanceid
  WHERE 
    g.pty_sts <> 'Cancelled'
    AND t.tax_lin_mop = '1'
    AND t.tax_lin_amt > 0
    AND CAST(EXTRACT(YEAR FROM DATE(g.ast_ayr || '-01-01')) AS STRING) >= '2023'
  GROUP BY 1, 2, 3
),

quittance AS (
  SELECT 
    FORMAT_DATE('%Y', DATE(g.ide_rcp_dat)) AS annee,
    FORMAT_DATE('%m', DATE(g.ide_rcp_dat)) AS mois,
    t.tax_lin_cod AS code,
    SUM(t.tax_lin_amt) AS montant
  FROM `sydonia.sad_general_segment` g
  LEFT JOIN (
    SELECT instanceid, tax_lin_cod, tax_lin_amt, tax_lin_mop 
    FROM `sydonia.sad_tax`
    UNION ALL
    SELECT instanceid, tax_cod AS tax_lin_cod, tax_amt AS tax_lin_amt, tax_mop AS tax_lin_mop FROM sydonia.sad_global_taxes
  ) t
  ON g.instanceid = t.instanceid
  WHERE 
    g.pty_sts <> 'Cancelled'
    AND t.tax_lin_mop = '1'
    AND t.tax_lin_amt > 0
    AND FORMAT_DATE('%Y', DATE(g.ide_rcp_dat)) >= '2023'
  GROUP BY 1, 2, 3
),

uc_liquidation AS (
  SELECT 
    FORMAT_DATE('%Y', DATE(g.ide_ast_dat)) AS annee,
    FORMAT_DATE('%m', DATE(g.ide_ast_dat)) AS mois,
    t.tax_lin_cod AS code,
    SUM(IF(i.tar_hsc_nb1 = '27101212', u.tar_sup_qty, 0)) AS volume_27101212,
    MAX(IF(i.tar_hsc_nb1 = '27101212', u.tar_sup_cod, NULL)) AS uc_27101212,
    SUM(IF(i.tar_hsc_nb1 = '27101921', u.tar_sup_qty, 0)) AS volume_27101921,
    MAX(IF(i.tar_hsc_nb1 = '27101921', u.tar_sup_cod, NULL)) AS uc_27101921
  FROM `sydonia.sad_general_segment` g
  JOIN `sydonia.sad_item` i
    ON g.instanceid = i.instanceid
  JOIN `sydonia.sad_tax` t
    ON t.instanceid = i.instanceid AND t.key_itm_nbr = i.key_itm_nbr
  JOIN `sydonia.sad_supplementary_unit` u
    ON i.instanceid = u.instanceid AND i.key_itm_nbr = u.key_itm_nbr
  WHERE 
    g.pty_sts <> 'Cancelled'
    AND u.key_sup_rnk = 1
    AND t.tax_lin_mop = '1'
    AND t.tax_lin_amt > 0
    AND t.tax_lin_cod = 'TPP'
    AND i.tar_hsc_nb1 IN ('27101212', '27101921')
    AND g.ide_rcp_dat IS NOT NULL
    AND CAST(EXTRACT(YEAR FROM DATE(g.ast_ayr || '-01-01')) AS STRING) >= '2023'
  GROUP BY 1, 2, 3
),

uc_quittance AS (
  SELECT 
    FORMAT_DATE('%Y', DATE(g.ide_rcp_dat)) AS annee,
    FORMAT_DATE('%m', DATE(g.ide_rcp_dat)) AS mois,
    t.tax_lin_cod AS code,
    SUM(IF(i.tar_hsc_nb1 = '27101212', u.tar_sup_qty, 0)) AS volume_27101212,
    MAX(IF(i.tar_hsc_nb1 = '27101212', u.tar_sup_cod, NULL)) AS uc_27101212,
    SUM(IF(i.tar_hsc_nb1 = '27101921', u.tar_sup_qty, 0)) AS volume_27101921,
    MAX(IF(i.tar_hsc_nb1 = '27101921', u.tar_sup_cod, NULL)) AS uc_27101921
  FROM `sydonia.sad_general_segment` g
  JOIN `sydonia.sad_item` i
    ON g.instanceid = i.instanceid
  JOIN `sydonia.sad_tax` t
    ON t.instanceid = i.instanceid AND t.key_itm_nbr = i.key_itm_nbr
  JOIN `sydonia.sad_supplementary_unit` u
    ON i.instanceid = u.instanceid AND i.key_itm_nbr = u.key_itm_nbr
  WHERE 
    g.pty_sts <> 'Cancelled'
    AND u.key_sup_rnk = 1
    AND t.tax_lin_mop = '1'
    AND t.tax_lin_amt > 0
    AND t.tax_lin_cod = 'TPP'
    AND i.tar_hsc_nb1 IN ('27101212', '27101921')
    AND g.ide_rcp_dat IS NOT NULL
    AND FORMAT_DATE('%Y', DATE(g.ide_rcp_dat)) >= '2023'
  GROUP BY 1, 2, 3
)

SELECT 
  COALESCE(l.annee, q.annee) AS annee,
  COALESCE(l.mois, q.mois) AS mois,
  COALESCE(l.code, q.code) AS code,
  COALESCE(l.montant, 0) AS montant_liquide,
  COALESCE(q.montant, 0) AS montant_paye,
  ul.volume_27101212 AS volume_liquide_27101212,
  uq.volume_27101212 AS volume_paye_27101212,
  ul.uc_27101212 AS uc_27101212,
  ul.volume_27101921 AS volume_liquide_27101921,
  uq.volume_27101921 AS volume_paye_27101921,
  ul.uc_27101921 AS uc_27101921
-- ,(SELECT FIRST_VALUE(untaxtab.tax_dsc) OVER (PARTITION BY untaxtab.tax_cod ORDER BY untaxtab.valid_from DESC)
--   FROM `sydonia.untaxtab` untaxtab
--   WHERE untaxtab.flg_rem = FALSE AND untaxtab.tax_cod = COALESCE(l.code, q.code)
-- ) AS libelle
FROM liquidation l
FULL OUTER JOIN quittance q
  ON l.annee = q.annee AND l.mois = q.mois AND l.code = q.code
LEFT JOIN uc_liquidation ul
  ON l.annee = ul.annee AND l.mois = ul.mois AND l.code = ul.code
LEFT JOIN uc_quittance uq
  ON l.annee = uq.annee AND l.mois = uq.mois AND l.code = uq.code
