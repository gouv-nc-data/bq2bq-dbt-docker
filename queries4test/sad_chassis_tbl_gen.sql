CREATE OR REPLACE TABLE sydonia_opendata.sad_chassis AS
SELECT 
  g.ide_cuo_cod AS bureau,
  CONCAT(g.ide_typ_sad, ' ', g.ide_typ_prc) AS modele,
  CONCAT(g.ide_reg_ser, ' ', g.ide_reg_nbr) AS numero_enregistrement,
  g.ide_reg_dat AS date_enregistrement,
  c.cha_nbr AS numero_chassis,
  IF(c.normal, 'Oui', 'Non') AS normale,
  IF(c.imp_tmp, 'Oui', 'Non') AS `AT`,
  IF(c.exo_tmp, 'Oui', 'Non') AS EXO,
  c.exo_from AS exo_debut,
  c.exo_to AS exo_fin,
  IF(c.exo_permanent, 'Oui', 'Non') AS Permanent
FROM sydonia.sad_general_segment g
JOIN sydonia.sad_chassis c ON g.instanceid = c.instanceid
WHERE g.pty_bae_ci5 = TRUE