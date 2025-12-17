CREATE OR REPLACE TABLE sydonia_opendata.tarif_douanier_tab AS
WITH section AS (
  SELECT * 
  FROM sydonia.unhs1tab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE
    AND hs1_cod <> '00'
),
chapitre AS (
  SELECT * 
  FROM sydonia.unhs2tab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE
    AND hs2_cod <> '00'
),
position AS (
  SELECT * 
  FROM sydonia.unhs4tab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE
),
sous_position AS (
  SELECT * 
  FROM sydonia.unhs6tab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE
),
notes AS (
  SELECT not_cod,
    REPLACE(REPLACE(REPLACE(not_txt, '<tbody>', ''), '</tbody>', ''), '&acirc;', 'â') AS not_txt
  FROM sydonia.unnottab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND DATE_SUB(COALESCE(DATE(valid_to), DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), INTERVAL 1 DAY)
    AND flg_rem = FALSE
),
tnotes AS (
  SELECT 
    REGEXP_REPLACE(not_cod, r'\D', '') AS not_cod,
    CONCAT(
      '<strong>[', not_dsc, ']</strong> ',
      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(not_txt,
        '<tbody>', ''),
        '</tbody>', ''),
        '&acirc;', 'â'),
        '<html>', ''),
        '</html>', ''),
        '<head>', ''),
        '</head>', ''),
        '<body>', ''),
        '</body>', '')
    ) AS not_txt,
    ROW_NUMBER() OVER (ORDER BY not_cod) AS ordre
  FROM sydonia.unnottab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND DATE_SUB(COALESCE(DATE(valid_to), DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), INTERVAL 1 DAY)
    AND (not_cod LIKE 'R%' OR not_cod LIKE 'X%')
    AND flg_rem = FALSE
),
tarif AS (
  SELECT * FROM sydonia.untartab
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE 
    AND hs6_cod NOT LIKE '%*%'
    AND tar_pr1 NOT LIKE '%*%'
    AND tar_pr2 NOT LIKE '%*%'
),
nomenc AS (
  SELECT 
    hs2_cod,
    hs4_cod,
    hs6_cod,
    hs8_cod,
    tar_pr1,
    tar_pr2,
    `Position SH`,
    `Désignation des marchandises`,
    `Unité supp`,
    `Codification statistique`,
    ROW_NUMBER() OVER (
      ORDER BY hs4_cod, hs6_cod NULLS FIRST, tar_pr1 NULLS FIRST, ordre
    ) AS position_ordre
  FROM (
    SELECT 
      LEFT(REGEXP_REPLACE(not_cod, r'\D', ''), 2) AS hs2_cod,
      LEFT(REGEXP_REPLACE(not_cod, r'\D', ''), 4) AS hs4_cod,
      LEFT(REGEXP_REPLACE(not_cod, r'\D', ''), 6) AS hs6_cod,
      LEFT(REGEXP_REPLACE(not_cod, r'\D', ''), 8) AS hs8_cod,
      SUBSTR(REGEXP_REPLACE(not_cod, r'\D', ''), 7, 9) AS tar_pr1,
      CAST(NULL AS STRING) AS tar_pr2,
      CONCAT(LEFT(REGEXP_REPLACE(not_cod, r'\D', ''), 4), '.', SUBSTR(REGEXP_REPLACE(not_cod, r'\D', ''), 5, 2)) AS `Position SH`,
      not_txt AS `Désignation des marchandises`,
      CAST(NULL AS STRING) AS `Unité supp`,
      CAST(NULL AS STRING) AS `Codification statistique`,
      1 AS ordre
    FROM sydonia.unnottab
    WHERE not_cod LIKE 'T%'
      AND CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND DATE_SUB(COALESCE(DATE(valid_to), DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), INTERVAL 1 DAY)
      AND flg_rem = FALSE

    UNION ALL

    SELECT 
      hs2_cod,
      hs4_cod,
      CAST(NULL AS STRING) AS hs6_cod,
      CAST(NULL AS STRING) AS hs8_cod,
      CAST(NULL AS STRING) AS tar_pr1,
      CAST(NULL AS STRING) AS tar_pr2,
      hs4_cod AS `Position SH`,
      hs4_dsc AS `Désignation des marchandises`,
      CAST(NULL AS STRING) AS `Unité supp`,
      CAST(NULL AS STRING) AS `Codification statistique`,
      2 AS ordre
    FROM position

    UNION ALL

    SELECT DISTINCT
      position.hs2_cod AS hs2_cod,
      position.hs4_cod,
      sous_position.hs6_cod,
      CAST(NULL AS STRING) AS hs8_cod,
      CAST(NULL AS STRING) AS tar_pr1,
      CAST(NULL AS STRING) AS tar_pr2,
      CONCAT(SUBSTR(sous_position.hs6_cod, 1, 4), '.', SUBSTR(sous_position.hs6_cod, 5)) AS `Position SH`,
      sous_position.hs6_dsc AS `Désignation des marchandises`,
      CAST(NULL AS STRING) AS `Unité supp`,
      CAST(NULL AS STRING) AS `Codification statistique`,
      3 AS ordre
    FROM position
    JOIN sous_position ON sous_position.hs4_cod = position.hs4_cod
    LEFT JOIN tarif ON sous_position.hs6_cod = tarif.hs6_cod
    WHERE (sous_position.hs6_dsc <> tarif.tar_all OR tarif.tar_all IS NULL)
      AND (sous_position.hs6_dsc <> position.hs4_dsc)

    UNION ALL

    SELECT 
      LEFT(sous_position.hs6_cod, 2) AS hs2_cod,
      sous_position.hs4_cod,
      sous_position.hs6_cod,
      CONCAT(tarif.hs6_cod, tarif.tar_pr1) AS hs8_cod,
      tarif.tar_pr1,
      tarif.tar_pr2,
      CONCAT(SUBSTR(sous_position.hs6_cod, 1, 4), '.', SUBSTR(sous_position.hs6_cod, 5)) AS `Position SH`,
      COALESCE(tarif.tar_all, sous_position.hs6_dsc) AS `Désignation des marchandises`,
      tarif.uom_cod1 AS `Unité supp`,
      CONCAT(sous_position.hs4_cod, '.', RIGHT(tarif.hs6_cod, 2), '.', LEFT(tarif.tar_pr1, 2)) AS `Codification statistique`,
      4 AS ordre
    FROM sous_position 
    LEFT JOIN tarif ON sous_position.hs6_cod = tarif.hs6_cod
  )
),
tar_col AS (
  SELECT 1 AS tar_rnk, tar_t01 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 
  SELECT 2 AS tar_rnk, tar_t02 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE 

  UNION ALL 

  SELECT 3 AS tar_rnk, tar_t03 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 

  SELECT 4 AS tar_rnk, tar_t04 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 

  SELECT 5 AS tar_rnk, tar_t05 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 

  SELECT 6 AS tar_rnk, tar_t06 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 

  SELECT 7 AS tar_rnk, tar_t07 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 

  SELECT 8 AS tar_rnk, tar_t08 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 

  SELECT 9 AS tar_rnk, tar_t09 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 

  SELECT 10 AS tar_rnk, tar_t10 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 

  SELECT 11 AS tar_rnk, tar_t11 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 

  SELECT 12 AS tar_rnk, tar_t12 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 

  SELECT 13 AS tar_rnk, tar_t13 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 

  SELECT 14 AS tar_rnk, tar_t14 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE

  UNION ALL 

  SELECT 15 AS tar_rnk, tar_t15 AS taux, hs6_cod, tar_pr1, tar_pr2 
  FROM sydonia.untartab 
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE())
    AND flg_rem = FALSE
),
specifique AS (
  SELECT 
    CASE c.cod 
      WHEN 'TCISPEC' THEN 3
      WHEN 'TSASPEC' THEN 7
      WHEN 'TCPSPEC' THEN 8
      WHEN 'TAPSPEC' THEN 11
      ELSE NULL 
    END AS tar_rnk,
    v.tar_nb1 
  FROM sydonia.can_nb1_tab v
  JOIN sydonia.can_tar_tab c ON c.instance_id = v.instance_id
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(c.dat_beg), CURRENT_DATE()) AND COALESCE(DATE(c.dat_end), CURRENT_DATE()) 
    AND c.cod IN ('TCISPEC', 'TSASPEC', 'TCPSPEC', 'TAPSPEC')
),
tax AS (
  SELECT 
    t.hs6_cod, 
    t.tar_pr1, 
    t.tar_pr2, 
    r.rul_nam, 
    COALESCE(CAST(b.rtx_rat AS STRING), t.taux) AS taux, 
    specifique.tar_nb1,
    CASE 
      WHEN r.rul_nam IN ('TTE', 'APP', 'TPP', 'TAT') THEN 'specifique'
      WHEN specifique.tar_nb1 IS NULL THEN 'ad valorem' 
      ELSE 'specifique' 
    END AS droit
  FROM sydonia.taxation_rules r
  JOIN tar_col t ON r.rul_rnk = t.tar_rnk
  LEFT JOIN (
    SELECT rtx_cod, rtx_rat FROM sydonia.unrtxtab 
    WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(valid_from), CURRENT_DATE()) AND COALESCE(DATE(valid_to), CURRENT_DATE()) AND flg_rem = FALSE
  ) b ON b.rtx_cod = t.taux
  LEFT JOIN specifique ON t.tar_rnk = specifique.tar_rnk AND CONCAT(t.hs6_cod, t.tar_pr1, t.tar_pr2) LIKE CONCAT(specifique.tar_nb1, '%')
  WHERE CURRENT_DATE() BETWEEN COALESCE(DATE(r.valid_from), CURRENT_DATE()) AND COALESCE(DATE(r.valid_to), CURRENT_DATE())
    AND r.flg_rem = FALSE
    AND r.rul_typ = 1
),
tab AS (
  SELECT 
    CAST(section.hs1_cod AS INT64) AS section_ordre,
    CAST(chapitre.hs2_cod AS INT64) AS chapitre_ordre,
    nomenc.position_ordre,
    CONCAT('SECTION ', FORMAT('%d', CAST(section.hs1_cod AS INT64))) AS `Section`,
    section.hs1_dsc AS `Titre de la section`,
    notes_section.not_txt AS `Note de section`,
    CONCAT('CHAPITRE ', CAST(chapitre.hs2_cod AS STRING)) AS `Chapitre`,
    chapitre.hs2_dsc AS `Titre du chapitre`,
    notes_chapitre.not_txt AS `Note de chapitre`,
    nomenc.hs4_cod,
    nomenc.hs6_cod,
    nomenc.hs8_cod,
    nomenc.`Position SH`,
    nomenc.`Désignation des marchandises`,
    nomenc.`Unité supp`,
    nomenc.`Codification statistique`,
    MAX(CASE WHEN tax.rul_nam = 'DD' THEN 
      CASE 
        WHEN SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64) = 0 THEN 'EX' 
        ELSE CONCAT(
          CAST(ROUND(SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64), 2) AS STRING),
          CASE WHEN tax.droit = 'ad valorem' THEN '%' ELSE CONCAT('F/', nomenc.`Unité supp`) END
        )
      END
      ELSE NULL END) AS `DD`,
    MAX(CASE WHEN tax.rul_nam = 'TCI' THEN 
      CASE 
        WHEN SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64) = 0 THEN 'EX' 
        ELSE CONCAT(
          CAST(ROUND(SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64), 2) AS STRING),
          CASE WHEN tax.droit = 'ad valorem' THEN '%' ELSE CONCAT('F/', nomenc.`Unité supp`) END
        )
      END
      ELSE NULL END) AS `TCI`,
    MAX(CASE WHEN tax.rul_nam = 'TSA' THEN 
      CASE 
        WHEN SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64) = 0 THEN 'EX' 
        ELSE CONCAT(
          CAST(ROUND(SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64), 2) AS STRING),
          CASE WHEN tax.droit = 'ad valorem' THEN '%' ELSE CONCAT('F/', nomenc.`Unité supp`) END
        )
      END
      ELSE NULL END) AS `TSPA`,
    MAX(CASE WHEN tax.rul_nam = 'TAT' THEN 
      CASE 
        WHEN SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64) = 0 THEN 'EX' 
        ELSE CONCAT(
          CAST(ROUND(SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64), 2) AS STRING),
          CASE WHEN tax.droit = 'ad valorem' THEN '%' ELSE CONCAT('F/', nomenc.`Unité supp`) END
        )
      END
      ELSE NULL END) AS `TAT`,
    MAX(CASE WHEN tax.rul_nam = 'TAP' THEN 
      CASE 
        WHEN SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64) = 0 THEN 'EX' 
        ELSE CONCAT(
          CAST(ROUND(SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64), 2) AS STRING),
          CASE WHEN tax.droit = 'ad valorem' THEN '%' ELSE CONCAT('F/', nomenc.`Unité supp`) END
        )
      END
      ELSE NULL END) AS `TAP`,
    MAX(CASE WHEN tax.rul_nam = 'TPP' THEN 
      CASE 
        WHEN SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64) = 0 THEN 'EX' 
        ELSE CONCAT(
          CAST(ROUND(SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64), 2) AS STRING),
          CASE WHEN tax.droit = 'ad valorem' THEN '%' ELSE CONCAT('F/', nomenc.`Unité supp`) END
        )
      END
      ELSE NULL END) AS `TPP`,
    MAX(CASE WHEN tax.rul_nam = 'APP' THEN 
      CASE 
        WHEN SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64) = 0 THEN 'EX' 
        ELSE CONCAT(
          CAST(ROUND(SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64), 2) AS STRING),
          CASE WHEN tax.droit = 'ad valorem' THEN '%' ELSE CONCAT('F/', nomenc.`Unité supp`) END
        )
      END
      ELSE NULL END) AS `TAPP`,
    MAX(CASE WHEN tax.rul_nam = 'TGC' THEN 
      CASE 
        WHEN SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64) = 0 THEN 'EX' 
        ELSE CONCAT(
          CAST(ROUND(SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64), 2) AS STRING),
          CASE WHEN tax.droit = 'ad valorem' THEN '%' ELSE CONCAT('F/', nomenc.`Unité supp`) END
        )
      END
      ELSE NULL END) AS `TGC`,
    MAX(CASE WHEN tax.rul_nam = 'TTE' THEN 
      CASE 
        WHEN SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64) = 0 THEN 'EX' 
        ELSE CONCAT(
          CAST(ROUND(SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64), 2) AS STRING),
          CASE WHEN tax.droit = 'ad valorem' THEN '%' ELSE CONCAT('F/', nomenc.`Unité supp`) END
        )
      END
      ELSE NULL END) AS `TTE`,
    MAX(CASE WHEN tax.rul_nam = 'TRM' THEN 
      CASE 
        WHEN SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64) = 0 THEN 'EX' 
        ELSE CONCAT(
          CAST(ROUND(SAFE_CAST(NULLIF(tax.taux, '') AS FLOAT64), 2) AS STRING),
          CASE WHEN tax.droit = 'ad valorem' THEN '%' ELSE CONCAT('F/', nomenc.`Unité supp`) END
        )
      END
      ELSE NULL END) AS `TRM`
  FROM 
    section
    LEFT JOIN chapitre ON section.hs1_cod = chapitre.hs1_cod
    LEFT JOIN nomenc ON chapitre.hs2_cod = nomenc.hs2_cod
    LEFT JOIN notes notes_section ON CONCAT('S', section.hs1_cod) = notes_section.not_cod
    LEFT JOIN notes notes_chapitre ON CONCAT('C', chapitre.hs2_cod) = notes_chapitre.not_cod
    LEFT JOIN tax ON 
      nomenc.hs6_cod = tax.hs6_cod 
      AND SAFE_CAST(nomenc.tar_pr1 AS INT64) = SAFE_CAST(tax.tar_pr1 AS INT64)
      AND SAFE_CAST(nomenc.tar_pr2 AS INT64) = SAFE_CAST(tax.tar_pr2 AS INT64)
  GROUP BY 
    section_ordre, chapitre_ordre, position_ordre, `Section`, `Titre de la section`,
    `Note de section`, `Chapitre`, `Titre du chapitre`, `Note de chapitre`, nomenc.hs4_cod,
    nomenc.hs6_cod, nomenc.hs8_cod, `Position SH`, `Désignation des marchandises`, `Unité supp`,
    `Codification statistique`
)
SELECT 
  section_ordre,
  chapitre_ordre,
  position_ordre,
  `Section`,
  `Titre de la section`,
  `Note de section`,
  `Chapitre`,
  `Titre du chapitre`,
  `Note de chapitre`,
  `Position SH`,
  `Désignation des marchandises`,
  `Unité supp`,
  `Codification statistique`,
  `DD`,
  `TCI`,
  `TSPA`,
  `TAT`,
  `TAP`,
  `TPP`,
  `TAPP`,
  `TGC`,
  `TTE`,
  `TRM`,
  CONCAT('<html><head></head><body>', STRING_AGG(renvois.not_txt, '<br/><br/>'), '</body></html>') AS `Note de renvoi`
FROM tab
LEFT JOIN tnotes renvois ON tab.hs8_cod LIKE CONCAT(renvois.not_cod, '%')
GROUP BY 
  section_ordre, chapitre_ordre, position_ordre, `Section`, `Titre de la section`, `Note de section`, `Chapitre`, 
  `Titre du chapitre`, `Note de chapitre`, `Position SH`, `Désignation des marchandises`, `Unité supp`, `Codification statistique`, 
  `DD`, `TCI`, `TSPA`, `TAT`, `TAP`, `TPP`, `TAPP`, `TGC`, `TTE`, `TRM`;
