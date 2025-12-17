CREATE OR REPLACE TABLE sydonia_opendata.sel_loc_param_tab AS
select 
  instance_id
  ,cuo_cod
  ,cuo_nam
  ,cri_cod
  ,dat_beg
  ,dat_end
  ,case when exp_flg then 'O' else 'N' end as exp_flg
  ,case when imp_flg then 'O' else 'N' end as imp_flg
  ,cri_doc
  --,cri_rul
  ,cri_typ
  ,adm_cod
from sydonia.sel_loc_param_tab 
where dat_end is null