-- Projeção de publicação para os cartões do Looker. Uma linha por cidadão, unidade e conjunto de famílias
-- do atendimento. Os nomes e tipos publicados são preservados.
select  -- noqa: AM04
    p.* replace (
        if(
            grupo is null,
            p.familias,
            array(
                select as struct f.* replace (id_referencia as id_familia)
                from unnest(grupo.ids_familia) as id_referencia
                left join unnest(p.familias) as f on f.id_familia = id_referencia
                order by id_referencia
            )
        ) as familias,
        (
            select as struct
                p.atendimentos.* replace (
                    coalesce(array_length(grupo.eventos), 0) as quantidade,
                    (select countif(e.tipo_servico = 'cras') from unnest(grupo.eventos) as e) as quantidade_cras,
                    (select countif(e.tipo_servico = 'creas') from unnest(grupo.eventos) as e) as quantidade_creas,
                    (select countif(e.tipo_servico = 'centro_pop') from unnest(grupo.eventos) as e) as quantidade_centro_pop,
                    (select countif(e.tipo_servico = 'outros') from unnest(grupo.eventos) as e) as quantidade_outros,
                    array(
                        select as struct
                            evento.* replace (
                                array(
                                    select as struct prof.* replace (evento.cadastrante as id_profissional)
                                    from unnest(evento.profissionais) as prof with offset as posicao_profissional
                                    order by posicao_profissional
                                ) as profissionais
                            )
                        from unnest(grupo.eventos) as evento with offset as posicao_evento
                        order by posicao_evento
                    ) as registros
                )
        ) as atendimentos
    )
from __prontuario_base__ as p
left join unnest(array(
    select as struct
        ids_familia,
        array_agg(evento order by posicao) as eventos
    from (
        select
            evento,
            posicao,
            array(
                select distinct trim(prof.id_familia_referencia)
                from unnest(evento.profissionais) as prof
                where nullif(trim(prof.id_familia_referencia), '') is not null
                order by 1
            ) as ids_familia
        from unnest(p.atendimentos.registros) as evento with offset as posicao
    ) as eventos_com_familia
    group by ids_familia
)) as grupo
