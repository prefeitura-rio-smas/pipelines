-- BigQuery (GoogleSQL). Use o ID retornado por usuario_duas_unidades.sql.
declare usuario_informado string default 'COLE_O_ID_AQUI';

-- Mantém todas as colunas, structs, arrays e linhas do usuário na mart.
select p.*
from `rj-smas-dev.abordagem.planilha_unificada` as p
where p.usuario.id_usuario = trim(usuario_informado);
