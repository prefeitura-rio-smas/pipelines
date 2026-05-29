/* TEMPLATE: DEDUPLICAÇÃO VIA GRAFOS (RECURSIVE CTE)
   Objetivo: Agrupar registros duplicados criando um "ID Consolidado"
*/

WITH RECURSIVE 

-- ==========================================================
-- 1. Base tratada
-- ==========================================================
BaseDados AS (
    SELECT 
        globalid as id,
        repeat_cpf,
        repeat_nome_usuario, 
        repeat_nome_mae,
        repeat_nome_pai,
        repeat_data_nascimento,
        ano_num_data_abordagem as ano,


        REGEXP_REPLACE(repeat_cpf, r'\D', '') as key_cpf,
        UPPER(REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE(repeat_nome_usuario, NFD), r'\p{M}', ''), r'[^A-Z0-9]', '')) as key_nome_usuario,
        UPPER(REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE(repeat_nome_mae, NFD), r'\p{M}', ''), r'[^A-Z0-9]', '')) as key_nome_mae,
        UPPER(REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE(repeat_nome_pai, NFD), r'\p{M}', ''), r'[^A-Z0-9]', '')) as key_nome_pai
        
    FROM rj-smas.arcgis_raw.abordagem_repeat_raw 
),

-- ==========================================================
-- 2. AS REGRAS DE MATCH (CRIAÇÃO DAS ARESTAS)
-- Onde você define: "O que faz A ser igual a B?"
-- ==========================================================
Matches AS (
    -- REGRA 1: CPF
    SELECT 
        id AS id_origem, 
        MIN(id) OVER (PARTITION BY key_cpf, ano) AS id_destino
    FROM BaseDados
    WHERE key_cpf NOT IN ('99999999999', '00000000000', '11111111111')
      AND LENGTH(key_cpf) = 11
    QUALIFY id != id_destino 

    UNION ALL -- Une as regras, usar sempre que colocar novas

    -- REGRA 2: NOME USUÁRIO + NOME MÃE
    SELECT 
        id AS id_origem,
        MIN(id) OVER (PARTITION BY key_nome_usuario, key_nome_mae, ano) AS id_destino
    FROM BaseDados
    WHERE 
        key_nome_usuario IS NOT NULL AND key_nome_usuario != ''
        AND key_nome_mae IS NOT NULL AND key_nome_mae != ''
    QUALIFY id != id_destino

    UNION ALL 

    -- REGRA 3: NOME + CPF + DATA NASCIMENTO
    SELECT 
        id AS id_origem,
        MIN(id) OVER (PARTITION BY key_nome_usuario, key_cpf, repeat_data_nascimento, ano) AS id_destino
    FROM BaseDados
    WHERE key_nome_usuario != '' 
      AND LENGTH(key_cpf) = 11
      AND repeat_data_nascimento IS NOT NULL
    QUALIFY id != id_destino

    UNION ALL

    -- REGRA 4: NOME MÃE + CPF + DATA NASCIMENTO
    SELECT 
        id AS id_origem,
        MIN(id) OVER (PARTITION BY key_nome_mae, key_cpf, repeat_data_nascimento, ano) AS id_destino
    FROM BaseDados
    WHERE 
        key_nome_mae IS NOT NULL AND key_nome_mae != ''
        AND LENGTH(key_cpf) = 11
        AND repeat_data_nascimento IS NOT NULL
    QUALIFY id != id_destino

    UNION ALL

    -- REGRA 5: NOME MÃE + NOME PAI + DATA NASCIMENTO
    SELECT 
        id AS id_origem,
        MIN(id) OVER (PARTITION BY key_nome_mae, key_nome_pai, repeat_data_nascimento,ano) AS id_destino
    FROM BaseDados
    WHERE 
        key_nome_mae IS NOT NULL AND key_nome_mae != ''
        AND key_nome_pai IS NOT NULL AND key_nome_pai != ''
        AND repeat_data_nascimento IS NOT NULL
    QUALIFY id != id_destino

    UNION ALL

    -- REGRA 6: NOME MÃE + NOME PAI + CPF
    SELECT 
        id AS id_origem,
        MIN(id) OVER (PARTITION BY key_nome_mae, key_nome_pai, key_cpf, ano) AS id_destino
    FROM BaseDados
    WHERE 
        key_nome_mae IS NOT NULL AND key_nome_mae != ''
        AND key_nome_pai IS NOT NULL AND key_nome_pai != ''
        AND LENGTH(key_cpf) = 11
    QUALIFY id != id_destino
),

-- ==========================================================
-- 3. O MOTOR DO GRAFO (RECURSÃO) - NÃO PRECISA MEXER
-- Ele navega: Se 1=2 e 2=3, então o motor descobre que 1=3
-- ==========================================================
GrafoConexo AS (
    -- Caso Base: As conexões diretas que achamos no passo anterior
    SELECT id_origem, id_destino
    FROM Matches

    UNION ALL

    -- Passo Recursivo: Procura amigos dos amigos
    SELECT 
        G.id_origem,    -- Mantém a raiz original (o "avô")
        M.id_destino    -- Pega o novo destino (o "neto")
    FROM GrafoConexo G
    JOIN Matches M ON G.id_destino = M.id_origem
),

-- ==========================================================
-- 4. CONSOLIDAÇÃO (GROUP BY ORIGEM) - NÃO PRECISA MEXER
-- ==========================================================
Mapa_De_Para AS (
    SELECT 
        id_origem AS ID_Cliente,      
        MIN(id_destino) AS ID_Mestre  
    FROM GrafoConexo
    GROUP BY id_origem 
)

-- ==========================================================
-- 5. RELATÓRIO FINAL
-- ==========================================================
,
f as (
SELECT 
    Orig.id,
    Orig.repeat_cpf,
    Orig.repeat_nome_usuario,
    Orig.repeat_nome_mae,
    Orig.repeat_nome_pai,
    Orig.repeat_data_nascimento,
    Orig.ano,
    
    COALESCE(Mapa.ID_Mestre, Orig.id) AS ID_Final_Consolidado, 
    
    CASE 
        WHEN Mapa.ID_Mestre IS NOT NULL AND Mapa.ID_Mestre != Orig.id THEN 'Duplicata' 
        ELSE 'Mestre/Único' 
    END AS Status_Registro

FROM BaseDados Orig
LEFT JOIN Mapa_De_Para Mapa ON Orig.id = Mapa.ID_Cliente
ORDER BY ID_Final_Consolidado, Orig.id
)

select * from f