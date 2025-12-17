CREATE OR REPLACE TABLE sydonia_opendata.sel_nat_param_tab AS
SELECT 
  instance_id,
  cri_cod,
  dat_beg,
  dat_end,
  CASE WHEN exp_flg THEN 'O' ELSE 'N' END AS exp_flg,
  CASE WHEN imp_flg THEN 'O' ELSE 'N' END AS imp_flg,
  cri_doc,
  -- cri_rul,
  cri_typ,
  adm_cod
FROM sydonia.sel_nat_param_tab
WHERE dat_end IS NULL
