CREATE OR REPLACE TABLE sydonia_opendata.dau_secours AS
SELECT
  COALESCE(
    FORMAT_DATE('%Y', ide_reg_dat),
    CAST(dec_ref_yer AS STRING)
  ) AS key_year,
  ide_cuo_cod AS key_cuo,
  dec_cod AS key_dec,
  dec_ref_nbr AS key_nber,
  ide_reg_ser AS sad_reg_serial,
  ide_reg_nbr AS sad_reg_nber,
  FORMAT_DATE('%Y-%m-%d', ide_reg_dat) AS sad_reg_date,
  pty_sts AS status,
  0 AS sad_num
FROM (
  SELECT *
  FROM `sydonia.sad_general_segment`
  WHERE ide_reg_dat BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND CURRENT_DATE()
    AND ide_cuo_cod IS NOT NULL
) AS sg