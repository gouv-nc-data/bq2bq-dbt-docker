{{ config(
    meta={
        'api_trigger_param': 'da_yh2n0k'
    }
) }}

select 
  cp4_cod
  ,cp4_dsc
  ,cpr_cod
  ,cpp_cod
  ,spe_tra
  ,gen_tra
from `{{ env_var('GCP_PROJECT') }}.sydonia.uncp4tab` 
WHERE 
  COALESCE(DATE(valid_to), CURRENT_DATE()) >= CURRENT_DATE()
  AND DATE(valid_from) <= CURRENT_DATE()
  AND flg_rem = FALSE
