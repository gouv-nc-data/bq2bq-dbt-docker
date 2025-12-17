CREATE OR REPLACE TABLE sydonia_opendata.sad_item AS
SELECT 
  instanceid,
  key_itm_nbr,
  pck_nbr,
  -- Nettoyage exhaustif pck_mrk1
  NORMALIZE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(pck_mrk1, r'[\x00-\x1F\x7F-\x9F]', ' '),  -- Contrôles invisibles
      r'[“”«»‘’‚„]', '"'  -- Guillemets typographiques → "
    ), 
    NFKC
  ) AS pck_mrk1,
  
  -- Nettoyage exhaustif pck_mrk2
  NORMALIZE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(pck_mrk2, r'[\x00-\x1F\x7F-\x9F]', ' '),
      r'[“”«»‘’‚„]', '"'
    ), 
    NFKC
  ) AS pck_mrk2,
  
  pck_typ_cod,
  pck_typ_nam,
  tar_hsc_nb1,
  tar_hsc_nb2,
  tar_hsc_nb3,
  tar_hsc_nb4,
  tar_hsc_nb5,
  tar_hsc_tsc_dat,
  tar_hsc_tsc_sta,
  tar_prf,
  tar_prc_ext,
  tar_prc_nat,
  tar_quo,
  tar_pri,
  tar_vmt,
  tar_vdt,
  tar_att,
  tar_aic,
  gds_org_cty,
  gds_org_crg,
  gds_ctn_ct1,
  gds_ctn_ct2,
  gds_ctn_ct3,
  gds_ctn_ct4,
  
  -- Nettoyage exhaustif gds_dsc
  NORMALIZE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(gds_dsc, r'[\x00-\x1F\x7F-\x9F]', ' '),
      r'[“”«»‘’‚„]', '"'
    ), 
    NFKC
  ) AS gds_dsc,
  
  -- Nettoyage exhaustif gds_ds3
  NORMALIZE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(gds_ds3, r'[\x00-\x1F\x7F-\x9F]', ' '),
      r'[“”«»‘’‚„]', '"'
    ), 
    NFKC
  ) AS gds_ds3,
  
  lnk_tpt,
  lnk_tpt_sln,
  lnk_prv_doc,
  lnk_prv_whs,
  lic_cod,
  lic_amt_val,
  lic_amt_qty,
  
  -- Nettoyage exhaustif txt_fre
  NORMALIZE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(txt_fre, r'[\x00-\x1F\x7F-\x9F]', ' '),
      r'[“”«»‘’‚„]', '"'
    ), 
    NFKC
  ) AS txt_fre,
  
  txt_rsv,
  tax_amt,
  tax_gty,
  tax_mop,
  tax_ctr,
  tax_dty,
  vit_wgt_grs,
  vit_wgt_net,
  vit_cst,
  vit_cif,
  vit_adj,
  vit_stv,
  vit_alp,
  vit_inv_amt_nmu,
  vit_inv_amt_fcx,
  vit_inv_cur_cod,
  vit_inv_cur_nam,
  vit_inv_cur_rat,
  vit_inv_cur_ref,
  vit_efr_amt_nmu,
  vit_efr_amt_fcx,
  vit_efr_cur_cod,
  vit_efr_cur_nam,
  vit_efr_cur_rat,
  vit_efr_cur_ref,
  vit_ifr_amt_nmu,
  vit_ifr_amt_fcx,
  vit_ifr_cur_cod,
  vit_ifr_cur_nam,
  vit_ifr_cur_rat,
  vit_ifr_cur_ref,
  vit_ins_amt_nmu,
  vit_ins_amt_fcx,
  vit_ins_cur_cod,
  vit_ins_cur_nam,
  vit_ins_cur_rat,
  vit_ins_cur_ref,
  vit_otc_amt_nmu,
  vit_otc_amt_fcx,
  vit_otc_cur_cod,
  vit_otc_cur_nam,
  vit_otc_cur_rat,
  vit_otc_cur_ref,
  vit_ded_amt_nmu,
  vit_ded_amt_fcx,
  vit_ded_cur_cod,
  vit_ded_cur_nam,
  vit_ded_cur_rat,
  vit_ded_cur_ref,
  vit_mkt_rat,
  vit_mkt_cur,
  vit_mkt_amt,
  vit_mkt_bse_dsc,
  vit_mkt_bse_amt,
  blk_vin,
  blk_srp,
  blk_fob,
  quo_id,
  quo_itm_nbr,
  doc_ref_dat,
  doc_ref_nbr,
  wri_sup_cod,
  wri_sup_nam,
  wri_sup_qty,
  wri_prg,
  lnk_ben_cod
FROM sydonia.sad_item AS sad_item
WHERE EXISTS (
  SELECT 1
  FROM (
    SELECT sg.*,
           DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AS p_date
    FROM sydonia.sad_general_segment AS sg
  ) AS g
  WHERE (
      COALESCE(g.ide_pst_dat, g.ide_ast_dat) >= g.p_date
      OR g.ide_rcp_dat >= g.p_date
      OR (g.ide_ast_dat IS NULL AND g.ide_reg_dat IS NOT NULL)
      OR g.pty_sts = 'Cancelled'
     )
     AND g.instanceid = sad_item.instanceid
)