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

drop materialized view if exists sagerx_dev.medication_rxcui_lookup;

create materialized view sagerx_dev.medication_rxcui_lookup as
with rxnorm_concepts as (

    select distinct on (c.rxcui)
        c.rxcui,
        c.str as rxcui_name,
        c.tty as rxcui_tty,
        case
            when c.suppress = 'N' then true
            else false
        end as active,
        case
            when c.cvf = '4096' then true
            else false
        end as prescribable
    from sagerx_lake.rxnorm_rxnconso c
    where c.sab = 'RXNORM'
    order by
        c.rxcui,
        case when c.suppress = 'N' then 0 else 1 end,
        case when c.cvf = '4096' then 0 else 1 end,
        case when c.ispref = 'Y' then 0 else 1 end,
        c.tty,
        c.str

),

products as (

    select distinct
        p.product_rxcui,
        p.product_name,
        p.product_tty,
        p.brand_vs_generic,
        coalesce(
            p.brand_name,
            substring(p.product_name from '\[(.*)\]')
        ) as brand_name,
        p.clinical_product_rxcui,
        p.clinical_product_name,
        p.clinical_product_tty
    from sagerx_dev.products p

),

clinical_products as (

    select distinct
        cp.rxcui as clinical_product_rxcui,
        cp.name as clinical_product_name,
        cp.tty as clinical_product_tty
    from sagerx_dev.stg_rxnorm__clinical_products cp

),

clinical_product_components as (

    select distinct
        cpcc.clinical_product_rxcui,
        cpcc.clinical_product_name,
        cpcc.clinical_product_tty,
        cpcc.clinical_product_component_rxcui
    from sagerx_dev.int_rxnorm_clinical_products_to_clinical_product_components cpcc

),

clinical_product_ingredients as (

    select distinct
        cpi.clinical_product_rxcui,
        cpi.clinical_product_name,
        cpi.clinical_product_tty,
        cpi.ingredient_rxcui,
        cpi.ingredient_name,
        cpi.ingredient_tty,
        'ingredient' as ingredient_role
    from sagerx_dev.int_rxnorm_clinical_products_to_ingredients cpi
    where cpi.ingredient_rxcui is not null
      and cpi.ingredient_rxcui not like '% | %'

    union

    select distinct
        cpic.clinical_product_rxcui,
        cpic.clinical_product_name,
        cpic.clinical_product_tty,
        cpic.ingredient_rxcui,
        cpic.ingredient_name,
        cpic.ingredient_tty,
        'ingredient' as ingredient_role
    from sagerx_dev.int_rxnorm_clinical_products_to_ingredient_components cpic
    where cpic.ingredient_rxcui is not null

    union

    select distinct
        cpic.clinical_product_rxcui,
        cpic.clinical_product_name,
        cpic.clinical_product_tty,
        cpic.ingredient_component_rxcui as ingredient_rxcui,
        cpic.ingredient_component_name as ingredient_name,
        cpic.ingredient_component_tty as ingredient_tty,
        'ingredient_component' as ingredient_role
    from sagerx_dev.int_rxnorm_clinical_products_to_ingredient_components cpic
    where cpic.ingredient_component_rxcui is not null

    union

    select distinct
        cpis.clinical_product_rxcui,
        cpis.clinical_product_name,
        cpis.clinical_product_tty,
        cpis.ingredient_strength_rxcui as ingredient_rxcui,
        cpis.ingredient_strength_name as ingredient_name,
        'SCDC' as ingredient_tty,
        'ingredient_strength' as ingredient_role
    from sagerx_dev.int_rxnorm_clinical_products_to_ingredient_strengths cpis
    where cpis.ingredient_strength_rxcui is not null

    union

    select distinct
        cpis.clinical_product_rxcui,
        cpis.clinical_product_name,
        cpis.clinical_product_tty,
        cpis.precise_ingredient_rxcui as ingredient_rxcui,
        cpis.precise_ingredient_name as ingredient_name,
        cpis.precise_ingredient_tty as ingredient_tty,
        'precise_ingredient' as ingredient_role
    from sagerx_dev.int_rxnorm_clinical_products_to_ingredient_strengths cpis
    where cpis.precise_ingredient_rxcui is not null

),

precise_ingredient_forms as (

    select distinct
        pin.rxcui as precise_ingredient_rxcui,
        pin.name as precise_ingredient_name,
        pin.tty as precise_ingredient_tty,
        base_ingredient.rxcui as ingredient_rxcui,
        base_ingredient.str as ingredient_name,
        base_ingredient.tty as ingredient_tty
    from sagerx_dev.stg_rxnorm__precise_ingredients pin
    inner join sagerx_lake.rxnorm_rxnrel form_rel
        on pin.rxcui = form_rel.rxcui1
       and form_rel.rela = 'has_form'
       and form_rel.sab = 'RXNORM'
    inner join sagerx_lake.rxnorm_rxnconso base_ingredient
        on form_rel.rxcui2 = base_ingredient.rxcui
       and base_ingredient.tty = 'IN'
       and base_ingredient.sab = 'RXNORM'

),

concept_to_clinical_products as (

    select distinct
        cp.clinical_product_rxcui as rxcui,
        cp.clinical_product_rxcui,
        cp.clinical_product_name,
        cp.clinical_product_tty
    from clinical_products cp

    union

    select distinct
        p.product_rxcui as rxcui,
        p.clinical_product_rxcui,
        p.clinical_product_name,
        p.clinical_product_tty
    from products p
    where p.product_rxcui is not null
      and p.clinical_product_rxcui is not null

    union

    select distinct
        cpc.clinical_product_component_rxcui as rxcui,
        cpc.clinical_product_rxcui,
        cpc.clinical_product_name,
        cpc.clinical_product_tty
    from clinical_product_components cpc
    where cpc.clinical_product_component_rxcui is not null

    union

    select distinct
        cpi.ingredient_rxcui as rxcui,
        cpi.clinical_product_rxcui,
        cpi.clinical_product_name,
        cpi.clinical_product_tty
    from clinical_product_ingredients cpi
    where cpi.ingredient_rxcui is not null

    union

    select distinct
        pif.precise_ingredient_rxcui as rxcui,
        cpi.clinical_product_rxcui,
        cpi.clinical_product_name,
        cpi.clinical_product_tty
    from precise_ingredient_forms pif
    inner join clinical_product_ingredients cpi
        on pif.ingredient_rxcui = cpi.ingredient_rxcui

    union

    select distinct
        cpi.ingredient_dose_form_rxcui as rxcui,
        cpi.clinical_product_rxcui,
        cpi.clinical_product_name,
        cpi.clinical_product_tty
    from sagerx_dev.int_rxnorm_clinical_products_to_ingredients cpi
    where cpi.ingredient_dose_form_rxcui is not null
      and cpi.ingredient_dose_form_rxcui not like '% | %'

    union

    select distinct
        bpc.rxcui,
        cpc.clinical_product_rxcui,
        cpc.clinical_product_name,
        cpc.clinical_product_tty
    from sagerx_dev.stg_rxnorm__brand_product_components bpc
    inner join clinical_product_components cpc
        on bpc.clinical_product_component_rxcui = cpc.clinical_product_component_rxcui
    where bpc.rxcui is not null

    union

    select distinct
        bpc.brand_rxcui as rxcui,
        cpc.clinical_product_rxcui,
        cpc.clinical_product_name,
        cpc.clinical_product_tty
    from sagerx_dev.stg_rxnorm__brand_product_components bpc
    inner join clinical_product_components cpc
        on bpc.clinical_product_component_rxcui = cpc.clinical_product_component_rxcui
    where bpc.brand_rxcui is not null

),

concept_clinical_products as (

    select distinct
        ctc.rxcui,
        ctc.clinical_product_rxcui,
        ctc.clinical_product_name,
        ctc.clinical_product_tty
    from concept_to_clinical_products ctc

),

concept_products as (

    select distinct
        ccp.rxcui,
        p.product_rxcui,
        p.product_name,
        p.product_tty,
        p.brand_vs_generic,
        p.brand_name
    from concept_clinical_products ccp
    inner join products p
        on ccp.clinical_product_rxcui = p.clinical_product_rxcui
    where p.product_rxcui is not null

),

concept_ingredients as (

    select distinct
        ccp.rxcui,
        cpi.ingredient_rxcui,
        cpi.ingredient_name,
        cpi.ingredient_tty,
        cpi.ingredient_role
    from concept_clinical_products ccp
    inner join clinical_product_ingredients cpi
        on ccp.clinical_product_rxcui = cpi.clinical_product_rxcui
    where cpi.ingredient_rxcui is not null

    union

    select distinct
        cpi.ingredient_rxcui as rxcui,
        cpi.ingredient_rxcui,
        cpi.ingredient_name,
        cpi.ingredient_tty,
        cpi.ingredient_role
    from clinical_product_ingredients cpi
    where cpi.ingredient_rxcui is not null

    union

    select distinct
        pif.precise_ingredient_rxcui as rxcui,
        pif.ingredient_rxcui,
        pif.ingredient_name,
        pif.ingredient_tty,
        'base_ingredient_form' as ingredient_role
    from precise_ingredient_forms pif
    where pif.ingredient_rxcui is not null

),

related_clinical_products as (

    select
        x.rxcui,
        jsonb_agg(
            jsonb_build_object(
                'rxcui', x.clinical_product_rxcui,
                'name', x.clinical_product_name,
                'tty', x.clinical_product_tty
            )
            order by x.clinical_product_tty, x.clinical_product_name, x.clinical_product_rxcui
        ) as related_clinical_products
    from (
        select distinct
            ccp.rxcui,
            ccp.clinical_product_rxcui,
            ccp.clinical_product_name,
            ccp.clinical_product_tty
        from concept_clinical_products ccp
        where ccp.clinical_product_rxcui is not null
    ) x
    group by x.rxcui

),

related_products as (

    select
        x.rxcui,
        jsonb_agg(
            jsonb_build_object(
                'rxcui', x.product_rxcui,
                'name', x.product_name,
                'tty', x.product_tty,
                'brand_vs_generic', x.brand_vs_generic,
                'brand_name', x.brand_name
            )
            order by x.product_tty, x.product_name, x.product_rxcui
        ) as related_products
    from (
        select distinct
            cp.rxcui,
            cp.product_rxcui,
            cp.product_name,
            cp.product_tty,
            cp.brand_vs_generic,
            cp.brand_name
        from concept_products cp
        where cp.product_rxcui is not null
    ) x
    group by x.rxcui

),

related_ingredients as (

    select
        x.rxcui,
        jsonb_agg(
            jsonb_build_object(
                'rxcui', x.ingredient_rxcui,
                'name', x.ingredient_name,
                'tty', x.ingredient_tty,
                'role', x.ingredient_role
            )
            order by x.ingredient_role, x.ingredient_tty, x.ingredient_name, x.ingredient_rxcui
        ) as related_ingredients
    from (
        select distinct
            ci.rxcui,
            ci.ingredient_rxcui,
            ci.ingredient_name,
            ci.ingredient_tty,
            ci.ingredient_role
        from concept_ingredients ci
        where ci.ingredient_rxcui is not null
    ) x
    group by x.rxcui

),

product_atc as (

    select distinct
        cp.rxcui,
        a.atc_3_code,
        a.atc_3_name,
        a.atc_4_code,
        a.atc_4_name
    from concept_products cp
    inner join sagerx_dev.atc_codes_to_rxnorm_products a
        on cp.product_rxcui = a.rxcui

    union

    select distinct
        a.rxcui,
        a.atc_3_code,
        a.atc_3_name,
        a.atc_4_code,
        a.atc_4_name
    from sagerx_dev.atc_codes_to_rxnorm_products a

),

ingredient_atc as (

    select distinct
        ci.rxcui,
        a.atc_3_code,
        a.atc_3_name,
        a.atc_4_code,
        a.atc_4_name
    from concept_ingredients ci
    inner join sagerx_dev.stg_rxnorm__atc_codes a
        on ci.ingredient_rxcui = a.ingredient_rxcui

    union

    select distinct
        a.ingredient_rxcui as rxcui,
        a.atc_3_code,
        a.atc_3_name,
        a.atc_4_code,
        a.atc_4_name
    from sagerx_dev.stg_rxnorm__atc_codes a

),

product_atc_level_3 as (

    select
        x.rxcui,
        jsonb_agg(
            jsonb_build_object(
                'code', x.atc_3_code,
                'name', x.atc_3_name
            )
            order by x.atc_3_code, x.atc_3_name
        ) as product_atc_level_3
    from (
        select distinct
            pa.rxcui,
            pa.atc_3_code,
            pa.atc_3_name
        from product_atc pa
        where pa.atc_3_code is not null
          and pa.atc_3_name is not null
    ) x
    group by x.rxcui

),

product_atc_level_4 as (

    select
        x.rxcui,
        jsonb_agg(
            jsonb_build_object(
                'code', x.atc_4_code,
                'name', x.atc_4_name
            )
            order by x.atc_4_code, x.atc_4_name
        ) as product_atc_level_4
    from (
        select distinct
            pa.rxcui,
            pa.atc_4_code,
            pa.atc_4_name
        from product_atc pa
        where pa.atc_4_code is not null
          and pa.atc_4_name is not null
    ) x
    group by x.rxcui

),

ingredient_atc_level_3 as (

    select
        x.rxcui,
        jsonb_agg(
            jsonb_build_object(
                'code', x.atc_3_code,
                'name', x.atc_3_name
            )
            order by x.atc_3_code, x.atc_3_name
        ) as ingredient_atc_level_3
    from (
        select distinct
            ia.rxcui,
            ia.atc_3_code,
            ia.atc_3_name
        from ingredient_atc ia
        where ia.atc_3_code is not null
          and ia.atc_3_name is not null
    ) x
    group by x.rxcui

),

ingredient_atc_level_4 as (

    select
        x.rxcui,
        jsonb_agg(
            jsonb_build_object(
                'code', x.atc_4_code,
                'name', x.atc_4_name
            )
            order by x.atc_4_code, x.atc_4_name
        ) as ingredient_atc_level_4
    from (
        select distinct
            ia.rxcui,
            ia.atc_4_code,
            ia.atc_4_name
        from ingredient_atc ia
        where ia.atc_4_code is not null
          and ia.atc_4_name is not null
    ) x
    group by x.rxcui

),

uses as (

    select distinct
        ccp.rxcui,
        d.rela,
        d.disease_name
    from concept_clinical_products ccp
    inner join sagerx_dev.clinical_products_to_diseases d
        on ccp.clinical_product_rxcui = d.clinical_product_rxcui
    where d.rela in ('may_treat', 'may_prevent')
      and d.disease_name is not null

    union

    select distinct
        d.via_ingredient_rxcui as rxcui,
        d.rela,
        d.disease_name
    from sagerx_dev.clinical_products_to_diseases d
    where d.rela in ('may_treat', 'may_prevent')
      and d.via_ingredient_rxcui is not null
      and d.disease_name is not null

),

uses_grouped as (

    select
        u.rxcui,
        array_agg(distinct u.disease_name) filter (
            where u.rela = 'may_treat'
              and u.disease_name is not null
        ) as typical_uses_may_treat,
        array_agg(distinct u.disease_name) filter (
            where u.rela = 'may_prevent'
              and u.disease_name is not null
        ) as typical_uses_may_prevent
    from uses u
    group by u.rxcui

),

inactive_ingredient_concepts as (

    -- Product-context DailyMed/SPL signal: the RXCUI appears as inactive
    -- in at least one product, but may still be active in another product.
    select distinct
        pii.inactive_ingredient_rxcui as rxcui
    from sagerx_dev.products_to_inactive_ingredients pii
    where pii.inactive_ingredient_rxcui is not null

)

select
    c.rxcui,
    c.rxcui_name,
    c.rxcui_tty,
    c.active,
    c.prescribable,
    iic.rxcui is not null as is_inactive_ingredient,
    rcp.related_clinical_products,
    rp.related_products,
    ri.related_ingredients,
    pa3.product_atc_level_3,
    pa4.product_atc_level_4,
    ia3.ingredient_atc_level_3,
    ia4.ingredient_atc_level_4,
    coalesce(pa3.product_atc_level_3, ia3.ingredient_atc_level_3) as preferred_atc_level_3,
    coalesce(pa4.product_atc_level_4, ia4.ingredient_atc_level_4) as preferred_atc_level_4,
    ug.typical_uses_may_treat,
    ug.typical_uses_may_prevent
from rxnorm_concepts c
left join related_clinical_products rcp
    on c.rxcui = rcp.rxcui
left join related_products rp
    on c.rxcui = rp.rxcui
left join related_ingredients ri
    on c.rxcui = ri.rxcui
left join product_atc_level_3 pa3
    on c.rxcui = pa3.rxcui
left join product_atc_level_4 pa4
    on c.rxcui = pa4.rxcui
left join ingredient_atc_level_3 ia3
    on c.rxcui = ia3.rxcui
left join ingredient_atc_level_4 ia4
    on c.rxcui = ia4.rxcui
left join uses_grouped ug
    on c.rxcui = ug.rxcui
left join inactive_ingredient_concepts iic
    on c.rxcui = iic.rxcui;

drop index if exists sagerx_dev.medication_rxcui_lookup_rxcui_idx;
create unique index medication_rxcui_lookup_rxcui_idx
    on sagerx_dev.medication_rxcui_lookup (rxcui);

drop index if exists sagerx_dev.medication_rxcui_lookup_rxcui_name_idx;
create index medication_rxcui_lookup_rxcui_name_idx
    on sagerx_dev.medication_rxcui_lookup (rxcui_name);

drop index if exists sagerx_dev.medication_rxcui_lookup_rxcui_tty_idx;
create index medication_rxcui_lookup_rxcui_tty_idx
    on sagerx_dev.medication_rxcui_lookup (rxcui_tty);
