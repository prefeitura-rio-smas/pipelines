-- depends_on: {{ ref('fct_atendimentos') }}

with casos as (
    select * from unnest([
        struct(
            'deduplica_representacoes' as caso,
            '12' as principal,
            '0012,12,00013,13' as compartilhados,
            [12, 13] as esperado
        ),
        ('principal_desconhecido', null, '0012,12,0,abc,, 13 ', [null, 12, 13]),
        ('lista_ausente', '12', null, [12]),
        ('lista_vazia', '12', '', [12]),
        ('somente_invalidos', '12', '0,000,abc,9223372036854775808', [12]),
        ('principal_zero_preservado', '0', '0,12', [0, 12]),
        ('principal_invalido', 'abc', '12', [null, 12]),
        ('principal_com_espacos', ' 0012 ', ' 00012 , 13 ', [12, 13])
    ])
),

resultados as (
    select
        caso,
        to_json_string(esperado) as esperado,
        to_json_string(array(
            select id_profissional
            from unnest({{ ids_profissionais_atendimento('principal', 'compartilhados') }}) as id_profissional
            order by id_profissional
        )) as obtido
    from casos
)

select * from resultados
where obtido != esperado
