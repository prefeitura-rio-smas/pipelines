
  
    

    create or replace table `rj-smas-dev`.`dashboard_arcgis`.`abordagem_ficha`
      
    
    

    OPTIONS()
    as (
      -- models/dashboard_arcgis/abordagem_ficha.sql

SELECT 
  uniquerowid,
  unidade_calculo,
  nome_usuario,
  SAFE.PARSE_DATE('%d/%m/%Y', data_nascimento) AS data_nascimento,
  cpf,
  nome_mae,
  filtro_ano_ultima_abordagem,
  filtro_data_abordagem,
  IFNULL(excluir_ficha, 'Não') AS excluir_ficha,  
  created_user

 FROM `rj-smas-dev`.`arcgis_raw`.`abordagem_ficha_raw`
    );
  