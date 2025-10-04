SELECT
  r.*,
  e.cas,
  e.e_mail,
  c.nm,
  c.mm,
  c.cluster_id,
  c.cluster_size,
  c.is_duplicado  
FROM 
  {{ ref('abordagem_repeat') }} as r
LEFT JOIN 
  `rj-smas-dev.dashboard_arcgis.abordagem_filtro_emails` as e
ON
  r.repeat_unidade_cas = e.cas
LEFT JOIN  
  {{ ref('abordagem_repeat_tratamento_duplicatas_cluster') }} as c
ON
  r.globalid = c.globalid
