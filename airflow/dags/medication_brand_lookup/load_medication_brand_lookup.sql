drop materialized view if exists sagerx_dev.medication_brand_lookup;

create materialized view sagerx_dev.medication_brand_lookup as
with base as (

    select distinct
        p.product_rxcui as brand_product_rxcui,
        p.product_name,
        coalesce(
            p.brand_name,
            substring(p.product_name from '\[(.*)\]')
        ) as brand_name,
        p.ingredient_name,
        cpi.ingredient_rxcui,
        p.clinical_product_rxcui,
        p.clinical_product_name
    from sagerx_dev.products p
    left join sagerx_dev.int_rxnorm_clinical_products_to_ingredients cpi
        on p.clinical_product_rxcui = cpi.clinical_product_rxcui
       and p.ingredient_name = cpi.ingredient_name
    where p.brand_vs_generic = 'brand'
      and p.product_tty in ('SBD', 'BPCK')

),

product_atc as (

    select distinct
        a.rxcui as brand_product_rxcui,
        a.atc_3_code,
        a.atc_3_name,
        a.atc_4_code,
        a.atc_4_name
    from sagerx_dev.atc_codes_to_rxnorm_products a

),

ingredient_atc as (

    select distinct
        ingredient_rxcui,
        atc_3_code,
        atc_3_name,
        atc_4_code,
        atc_4_name
    from sagerx_dev.stg_rxnorm__atc_codes

),

uses as (

    select distinct
        d.clinical_product_rxcui,
        d.rela,
        d.disease_name
    from sagerx_dev.clinical_products_to_diseases d
    where d.rela in ('may_treat', 'may_prevent')
      and d.disease_name is not null

),

grouped as (

    select
        b.product_name,
        b.brand_name,
        b.ingredient_name,
        b.ingredient_rxcui,
        b.clinical_product_rxcui,
        b.clinical_product_name,
        b.brand_product_rxcui,

        array_agg(distinct u.disease_name) filter (
            where u.rela = 'may_treat'
              and u.disease_name is not null
        ) as typical_uses_may_treat,

        array_agg(distinct u.disease_name) filter (
            where u.rela = 'may_prevent'
              and u.disease_name is not null
        ) as typical_uses_may_prevent

    from base b
    left join uses u
        on b.clinical_product_rxcui = u.clinical_product_rxcui
    group by
        b.product_name,
        b.brand_name,
        b.ingredient_name,
        b.ingredient_rxcui,
        b.clinical_product_rxcui,
        b.clinical_product_name,
        b.brand_product_rxcui

)

select
    g.product_name,
    g.brand_name,
    g.ingredient_name,
    g.ingredient_rxcui,
    g.clinical_product_rxcui,
    g.clinical_product_name,
    g.brand_product_rxcui,

    (
        select jsonb_agg(
            jsonb_build_object(
                'code', x.atc_3_code,
                'name', x.atc_3_name
            )
            order by x.atc_3_code, x.atc_3_name
        )
        from (
            select distinct
                pa.atc_3_code,
                pa.atc_3_name
            from product_atc pa
            where pa.brand_product_rxcui = g.brand_product_rxcui
              and pa.atc_3_code is not null
              and pa.atc_3_name is not null
        ) x
    ) as product_atc_level_3,

    (
        select jsonb_agg(
            jsonb_build_object(
                'code', x.atc_4_code,
                'name', x.atc_4_name
            )
            order by x.atc_4_code, x.atc_4_name
        )
        from (
            select distinct
                pa.atc_4_code,
                pa.atc_4_name
            from product_atc pa
            where pa.brand_product_rxcui = g.brand_product_rxcui
              and pa.atc_4_code is not null
              and pa.atc_4_name is not null
        ) x
    ) as product_atc_level_4,

    (
        select jsonb_agg(
            jsonb_build_object(
                'code', x.atc_3_code,
                'name', x.atc_3_name
            )
            order by x.atc_3_code, x.atc_3_name
        )
        from (
            select distinct
                ia.atc_3_code,
                ia.atc_3_name
            from ingredient_atc ia
            where ia.ingredient_rxcui = g.ingredient_rxcui
              and ia.atc_3_code is not null
              and ia.atc_3_name is not null
        ) x
    ) as ingredient_atc_level_3,

    (
        select jsonb_agg(
            jsonb_build_object(
                'code', x.atc_4_code,
                'name', x.atc_4_name
            )
            order by x.atc_4_code, x.atc_4_name
        )
        from (
            select distinct
                ia.atc_4_code,
                ia.atc_4_name
            from ingredient_atc ia
            where ia.ingredient_rxcui = g.ingredient_rxcui
              and ia.atc_4_code is not null
              and ia.atc_4_name is not null
        ) x
    ) as ingredient_atc_level_4,

    coalesce(
        (
            select jsonb_agg(
                jsonb_build_object(
                    'code', x.atc_3_code,
                    'name', x.atc_3_name
                )
                order by x.atc_3_code, x.atc_3_name
            )
            from (
                select distinct
                    pa.atc_3_code,
                    pa.atc_3_name
                from product_atc pa
                where pa.brand_product_rxcui = g.brand_product_rxcui
                  and pa.atc_3_code is not null
                  and pa.atc_3_name is not null
            ) x
        ),
        (
            select jsonb_agg(
                jsonb_build_object(
                    'code', x.atc_3_code,
                    'name', x.atc_3_name
                )
                order by x.atc_3_code, x.atc_3_name
            )
            from (
                select distinct
                    ia.atc_3_code,
                    ia.atc_3_name
                from ingredient_atc ia
                where ia.ingredient_rxcui = g.ingredient_rxcui
                  and ia.atc_3_code is not null
                  and ia.atc_3_name is not null
            ) x
        )
    ) as preferred_atc_level_3,

    coalesce(
        (
            select jsonb_agg(
                jsonb_build_object(
                    'code', x.atc_4_code,
                    'name', x.atc_4_name
                )
                order by x.atc_4_code, x.atc_4_name
            )
            from (
                select distinct
                    pa.atc_4_code,
                    pa.atc_4_name
                from product_atc pa
                where pa.brand_product_rxcui = g.brand_product_rxcui
                  and pa.atc_4_code is not null
                  and pa.atc_4_name is not null
            ) x
        ),
        (
            select jsonb_agg(
                jsonb_build_object(
                    'code', x.atc_4_code,
                    'name', x.atc_4_name
                )
                order by x.atc_4_code, x.atc_4_name
            )
            from (
                select distinct
                    ia.atc_4_code,
                    ia.atc_4_name
                from ingredient_atc ia
                where ia.ingredient_rxcui = g.ingredient_rxcui
                  and ia.atc_4_code is not null
                  and ia.atc_4_name is not null
            ) x
        )
    ) as preferred_atc_level_4,

    g.typical_uses_may_treat,
    g.typical_uses_may_prevent

from grouped g;

drop index if exists sagerx_dev.medication_brand_lookup_ingredient_name_idx;
create index medication_brand_lookup_ingredient_name_idx
    on sagerx_dev.medication_brand_lookup (ingredient_name);

drop index if exists sagerx_dev.medication_brand_lookup_brand_name_idx;
create index medication_brand_lookup_brand_name_idx
    on sagerx_dev.medication_brand_lookup (brand_name);

drop index if exists sagerx_dev.medication_brand_lookup_clinical_product_rxcui_idx;
create index medication_brand_lookup_clinical_product_rxcui_idx
    on sagerx_dev.medication_brand_lookup (clinical_product_rxcui);

drop index if exists sagerx_dev.medication_brand_lookup_brand_product_rxcui_idx;
create index medication_brand_lookup_brand_product_rxcui_idx
    on sagerx_dev.medication_brand_lookup (brand_product_rxcui);

drop index if exists sagerx_dev.medication_brand_lookup_uq;
create unique index medication_brand_lookup_uq
    on sagerx_dev.medication_brand_lookup (
        product_name,
        ingredient_rxcui,
        clinical_product_rxcui,
        brand_product_rxcui
    );