drop materialized view if exists sagerx_dev.rxcui_lookup;

set work_mem = '256MB';
set jit = off;

create index if not exists rxnorm_rxnrel_rxcui2_rela_sab_idx
    on sagerx_lake.rxnorm_rxnrel (rxcui2, rela, sab);

create index if not exists rxnorm_rxnrel_rxnorm_rela_rxcui2_rxcui1_idx
    on sagerx_lake.rxnorm_rxnrel (rela, rxcui2, rxcui1)
    where sab = 'RXNORM';

create index if not exists rxnorm_rxnrel_rxnorm_rela_rxcui1_rxcui2_idx
    on sagerx_lake.rxnorm_rxnrel (rela, rxcui1, rxcui2)
    where sab = 'RXNORM';

create index if not exists rxnorm_rxnconso_rxnorm_tty_rxcui_idx
    on sagerx_lake.rxnorm_rxnconso (tty, rxcui)
    include (str, suppress, cvf, ispref)
    where sab = 'RXNORM';

create index if not exists rxnorm_rxnsat_rxnorm_atn_rxcui_idx
    on sagerx_lake.rxnorm_rxnsat (atn, rxcui)
    include (atv)
    where sab = 'RXNORM';

create index if not exists products_to_inactive_ingredients_rxcui_idx
    on sagerx_dev.products_to_inactive_ingredients (inactive_ingredient_rxcui);

create index if not exists products_to_inactive_ingredients_product_inactive_idx
    on sagerx_dev.products_to_inactive_ingredients (
        product_rxcui
        , inactive_ingredient_rxcui
    );

create index if not exists products_product_rxcui_idx
    on sagerx_dev.products (product_rxcui);

create index if not exists products_clinical_product_rxcui_idx
    on sagerx_dev.products (clinical_product_rxcui);

create index if not exists int_rxnorm_cpic_clinical_product_rxcui_idx
    on sagerx_dev.int_rxnorm_clinical_products_to_ingredient_components (clinical_product_rxcui);

create index if not exists int_rxnorm_cpdf_clinical_product_rxcui_idx
    on sagerx_dev.int_rxnorm_clinical_products_to_dose_forms (clinical_product_rxcui);

create index if not exists atc_codes_to_rxnorm_products_rxcui_idx
    on sagerx_dev.atc_codes_to_rxnorm_products (rxcui);

create index if not exists clinical_products_to_diseases_product_ingredient_idx
    on sagerx_dev.clinical_products_to_diseases (
        clinical_product_rxcui
        , via_ingredient_rxcui
    );

analyze sagerx_lake.rxnorm_rxnconso;
analyze sagerx_lake.rxnorm_rxnrel;
analyze sagerx_lake.rxnorm_rxnsat;
analyze sagerx_dev.products_to_inactive_ingredients;
analyze sagerx_dev.products;
analyze sagerx_dev.int_rxnorm_clinical_products_to_ingredient_components;
analyze sagerx_dev.int_rxnorm_clinical_products_to_dose_forms;
analyze sagerx_dev.atc_codes_to_rxnorm_products;
analyze sagerx_dev.clinical_products_to_diseases;

create materialized view sagerx_dev.rxcui_lookup as
with

rxnorm_concepts as materialized (

    select distinct on (rxcui)
        rxcui::text as rxcui
        , str as rxcui_name
        , tty as rxnorm_tty
        , suppress = 'N' as active
        , cvf = '4096' as prescribable
    from sagerx_lake.rxnorm_rxnconso
    where sab = 'RXNORM'
    order by
        rxcui
        , case tty
            when 'SCD' then 1
            when 'SBD' then 2
            when 'GPCK' then 3
            when 'BPCK' then 4
            when 'SCDC' then 5
            when 'SBDC' then 6
            when 'SCDF' then 7
            when 'SBDF' then 8
            when 'SCDFP' then 9
            when 'SBDFP' then 10
            when 'SCDG' then 11
            when 'SBDG' then 12
            when 'SCDGP' then 13
            when 'IN' then 14
            when 'MIN' then 15
            when 'PIN' then 16
            when 'BN' then 17
            when 'DF' then 18
            when 'DFG' then 19
            when 'PSN' then 20
            when 'SY' then 21
            when 'TMSY' then 22
            when 'ET' then 23
            else 99
            end
        , case when suppress = 'N' then 0 else 1 end
        , case when ispref = 'Y' then 0 else 1 end
        , str

),

has_part_relations as not materialized (

    select
        rxcui1::text as rxcui1
        , rxcui2::text as rxcui2
    from sagerx_lake.rxnorm_rxnrel
    where sab = 'RXNORM'
        and rela = 'has_part'

),

has_ingredient_relations as not materialized (

    select
        rxcui1::text as rxcui1
        , rxcui2::text as rxcui2
    from sagerx_lake.rxnorm_rxnrel
    where sab = 'RXNORM'
        and rela = 'has_ingredient'

),

has_ingredients_relations as not materialized (

    select
        rxcui1::text as rxcui1
        , rxcui2::text as rxcui2
    from sagerx_lake.rxnorm_rxnrel
    where sab = 'RXNORM'
        and rela = 'has_ingredients'

),

has_boss_relations as not materialized (

    select
        rxcui1::text as rxcui1
        , rxcui2::text as rxcui2
    from sagerx_lake.rxnorm_rxnrel
    where sab = 'RXNORM'
        and rela = 'has_boss'

),

has_dose_form_relations as not materialized (

    select
        rxcui1::text as rxcui1
        , rxcui2::text as rxcui2
    from sagerx_lake.rxnorm_rxnrel
    where sab = 'RXNORM'
        and rela = 'has_dose_form'

),

form_of_relations as not materialized (

    select
        rxcui1::text as rxcui1
        , rxcui2::text as rxcui2
    from sagerx_lake.rxnorm_rxnrel
    where sab = 'RXNORM'
        and rela = 'form_of'

),

ingredient_of_relations as not materialized (

    select
        rxcui1::text as rxcui1
        , rxcui2::text as rxcui2
    from sagerx_lake.rxnorm_rxnrel
    where sab = 'RXNORM'
        and rela = 'ingredient_of'

),

isa_relations as not materialized (

    select
        rxcui1::text as rxcui1
        , rxcui2::text as rxcui2
    from sagerx_lake.rxnorm_rxnrel
    where sab = 'RXNORM'
        and rela = 'isa'

),

tradename_of_relations as not materialized (

    select
        rxcui1::text as rxcui1
        , rxcui2::text as rxcui2
    from sagerx_lake.rxnorm_rxnrel
    where sab = 'RXNORM'
        and rela = 'tradename_of'

),

inactive_ingredient_concepts as materialized (

    select distinct
        inactive_ingredient_rxcui::text as rxcui
    from sagerx_dev.products_to_inactive_ingredients
    where inactive_ingredient_rxcui is not null

),

inactive_product_ingredients as materialized (

    select distinct
        product_rxcui::text as product_rxcui
        , inactive_ingredient_rxcui::text as inactive_ingredient_rxcui
    from sagerx_dev.products_to_inactive_ingredients
    where product_rxcui is not null
        and inactive_ingredient_rxcui is not null

),

ingredient_component_parts as materialized (

    select distinct
        has_part.rxcui2 as ingredient_rxcui
        , ingredient_component.rxcui as ingredient_component_rxcui
        , ingredient_component.rxcui_name as ingredient_component_name
    from has_part_relations has_part
    inner join rxnorm_concepts ingredient_component
        on ingredient_component.rxcui = has_part.rxcui1
        and ingredient_component.rxnorm_tty = 'IN'

),

ingredient_concept_ingredients as materialized (

    select distinct
        concept_rxcui
        , ingredient_rxcui
        , ingredient_name
    from (
        select
            ingredient.rxcui as concept_rxcui
            , coalesce(ingredient_component.ingredient_component_rxcui, ingredient.rxcui) as ingredient_rxcui
            , coalesce(ingredient_component.ingredient_component_name, ingredient.rxcui_name) as ingredient_name
        from rxnorm_concepts ingredient
        left join ingredient_component_parts ingredient_component
            on ingredient_component.ingredient_rxcui = ingredient.rxcui
        where ingredient.rxnorm_tty in ('IN', 'MIN')

        union all

        select
            precise_ingredient.rxcui as concept_rxcui
            , precise_ingredient.rxcui as ingredient_rxcui
            , precise_ingredient.rxcui_name as ingredient_name
        from rxnorm_concepts precise_ingredient
        where precise_ingredient.rxnorm_tty = 'PIN'
    ) ingredient_rows

),

product_ingredients as materialized (

    select distinct
        product.product_rxcui::text as concept_rxcui
        , ingredient_component.ingredient_component_rxcui::text as ingredient_rxcui
        , ingredient_component.ingredient_component_name as ingredient_name
    from sagerx_dev.products product
    inner join sagerx_dev.int_rxnorm_clinical_products_to_ingredient_components ingredient_component
        on ingredient_component.clinical_product_rxcui = product.clinical_product_rxcui
    where ingredient_component.ingredient_component_rxcui is not null

),

ingredient_strength_ingredients as (

    select
        ingredient_strength.rxcui as concept_rxcui
        , ingredient_component.ingredient_rxcui
        , ingredient_component.ingredient_name
    from rxnorm_concepts ingredient_strength
    inner join has_ingredient_relations ingredient_relation
        on ingredient_relation.rxcui2 = ingredient_strength.rxcui
    inner join ingredient_concept_ingredients ingredient_component
        on ingredient_component.concept_rxcui = ingredient_relation.rxcui1
    where ingredient_strength.rxnorm_tty = 'SCDC'

),

clinical_drug_ingredients as (

    select
        clinical_drug.rxcui as concept_rxcui
        , ingredient_component.ingredient_rxcui
        , ingredient_component.ingredient_name
    from rxnorm_concepts clinical_drug
    inner join has_ingredients_relations ingredient_strength_relation
        on ingredient_strength_relation.rxcui2 = clinical_drug.rxcui
    inner join ingredient_strength_ingredients ingredient_component
        on ingredient_component.concept_rxcui = ingredient_strength_relation.rxcui1
    where clinical_drug.rxnorm_tty = 'SCD'

),

ingredient_dose_form_ingredients as materialized (

    select
        concept.rxcui as concept_rxcui
        , ingredient_component.ingredient_rxcui
        , ingredient_component.ingredient_name
    from rxnorm_concepts concept
    inner join has_ingredient_relations ingredient_relation
        on ingredient_relation.rxcui2 = concept.rxcui
    inner join ingredient_concept_ingredients ingredient_component
        on ingredient_component.concept_rxcui = ingredient_relation.rxcui1
    where concept.rxnorm_tty in ('SCDF', 'SCDG')

),

precise_ingredient_form_ingredients as (

    select
        concept.rxcui as concept_rxcui
        , ingredient_component.ingredient_rxcui
        , ingredient_component.ingredient_name
    from rxnorm_concepts concept
    inner join has_boss_relations boss_relation
        on boss_relation.rxcui2 = concept.rxcui
    inner join ingredient_concept_ingredients ingredient_component
        on ingredient_component.concept_rxcui = boss_relation.rxcui1
    where concept.rxnorm_tty = 'SCDFP'

),

ingredient_dose_form_group_ingredients as (

    select
        concept.rxcui as concept_rxcui
        , generic_ingredients.ingredient_rxcui
        , generic_ingredients.ingredient_name
    from rxnorm_concepts concept
    inner join form_of_relations form_relation
        on form_relation.rxcui2 = concept.rxcui
    inner join ingredient_dose_form_ingredients generic_ingredients
        on generic_ingredients.concept_rxcui = form_relation.rxcui1
    where concept.rxnorm_tty = 'SCDGP'

),

generic_concept_ingredients as materialized (

    select distinct
        concept_rxcui
        , ingredient_rxcui
        , ingredient_name
    from (
        select * from product_ingredients

        union all

        select * from ingredient_concept_ingredients

        union all

        select * from ingredient_strength_ingredients

        union all

        select * from clinical_drug_ingredients

        union all

        select * from ingredient_dose_form_ingredients

        union all

        select * from precise_ingredient_form_ingredients

        union all

        select * from ingredient_dose_form_group_ingredients
    ) ingredient_rows

),

branded_generic_concept_ingredients as (

    select
        brand_concept.rxcui as concept_rxcui
        , generic_ingredients.ingredient_rxcui
        , generic_ingredients.ingredient_name
    from rxnorm_concepts brand_concept
    inner join tradename_of_relations generic_relation
        on generic_relation.rxcui2 = brand_concept.rxcui
    inner join generic_concept_ingredients generic_ingredients
        on generic_ingredients.concept_rxcui = generic_relation.rxcui1
    where brand_concept.rxnorm_tty in ('SBDC', 'SBDF', 'SBDFP', 'SBDG')

),

brand_name_direct_ingredients as (

    select
        brand_concept.rxcui as concept_rxcui
        , ingredient_component.ingredient_rxcui
        , ingredient_component.ingredient_name
    from rxnorm_concepts brand_concept
    inner join tradename_of_relations ingredient_relation
        on ingredient_relation.rxcui2 = brand_concept.rxcui
    inner join ingredient_concept_ingredients ingredient_component
        on ingredient_component.concept_rxcui = ingredient_relation.rxcui1
    where brand_concept.rxnorm_tty = 'BN'

),

brand_name_fallback_ingredients as (

    select
        brand_concept.rxcui as concept_rxcui
        , ingredient_component.ingredient_rxcui
        , ingredient_component.ingredient_name
    from rxnorm_concepts brand_concept
    inner join ingredient_of_relations brand_product_relation
        on brand_product_relation.rxcui2 = brand_concept.rxcui
    inner join tradename_of_relations generic_product_relation
        on generic_product_relation.rxcui2 = brand_product_relation.rxcui1
    inner join has_ingredients_relations ingredient_relation
        on ingredient_relation.rxcui2 = generic_product_relation.rxcui1
    inner join ingredient_concept_ingredients ingredient_component
        on ingredient_component.concept_rxcui = ingredient_relation.rxcui1
    where brand_concept.rxnorm_tty = 'BN'

),

concept_ingredients as materialized (

    select distinct
        concept_rxcui
        , ingredient_rxcui
        , ingredient_name
    from (
        select * from generic_concept_ingredients

        union all

        select * from branded_generic_concept_ingredients

        union all

        select * from brand_name_direct_ingredients

        union all

        select * from brand_name_fallback_ingredients
    ) ingredient_rows

),

concept_ingredient_name_summary as (

    select
        concept_rxcui
        , string_agg(ingredient_name, ' / ' order by ingredient_name) as generic_name
    from (
        select
            concept_rxcui
            , lower(ingredient_name) as normalized_ingredient_name
            , min(ingredient_name) as ingredient_name
        from concept_ingredients
        where ingredient_name is not null
        group by
            concept_rxcui
            , lower(ingredient_name)
    ) ingredient_names
    group by concept_rxcui

),

concept_ingredient_summary as (

    select
        concept_ingredients.concept_rxcui
        , count(distinct concept_ingredients.ingredient_rxcui) as ingredient_count
        , count(distinct concept_ingredients.ingredient_rxcui) filter (
            where inactive_ingredient_concepts.rxcui is null
            ) as active_candidate_count
        , case
            when count(distinct concept_ingredients.ingredient_rxcui) = 1
                then min(concept_ingredients.ingredient_rxcui)
            when count(distinct concept_ingredients.ingredient_rxcui) filter (
                where inactive_ingredient_concepts.rxcui is null
                ) = 1
                then min(concept_ingredients.ingredient_rxcui) filter (
                    where inactive_ingredient_concepts.rxcui is null
                    )
            end as selected_ingredient_rxcui
    from concept_ingredients
    left join inactive_ingredient_concepts
        on inactive_ingredient_concepts.rxcui = concept_ingredients.ingredient_rxcui
    group by concept_ingredients.concept_rxcui

),

product_ingredient_summary as (

    select
        product_ingredients.concept_rxcui
        , count(distinct product_ingredients.ingredient_rxcui) as ingredient_count
        , count(distinct product_ingredients.ingredient_rxcui) filter (
            where inactive_product_ingredients.inactive_ingredient_rxcui is null
            ) as active_candidate_count
        , case
            when count(distinct product_ingredients.ingredient_rxcui) = 1
                then min(product_ingredients.ingredient_rxcui)
            when count(distinct product_ingredients.ingredient_rxcui) filter (
                where inactive_product_ingredients.inactive_ingredient_rxcui is null
                ) = 1
                then min(product_ingredients.ingredient_rxcui) filter (
                    where inactive_product_ingredients.inactive_ingredient_rxcui is null
                    )
            end as selected_ingredient_rxcui
    from product_ingredients
    left join inactive_product_ingredients
        on inactive_product_ingredients.product_rxcui = product_ingredients.concept_rxcui
        and inactive_product_ingredients.inactive_ingredient_rxcui = product_ingredients.ingredient_rxcui
    group by product_ingredients.concept_rxcui

),

dose_form_group_map as materialized (

    select distinct
        dose_form.rxcui as dose_form_rxcui
        , dose_form_group.rxcui_name as dose_form_group_name
    from rxnorm_concepts dose_form
    inner join isa_relations dose_form_group_relation
        on dose_form_group_relation.rxcui2 = dose_form.rxcui
    inner join rxnorm_concepts dose_form_group
        on dose_form_group.rxcui = dose_form_group_relation.rxcui1
        and dose_form_group.rxnorm_tty = 'DFG'
    where dose_form.rxnorm_tty = 'DF'

),

product_fields as (

    select
        product.product_rxcui::text as concept_rxcui
        , max(nullif(product.strength_name, '')) as strength
        , string_agg(distinct dose_form_group.dose_form_group_name, ' / ' order by dose_form_group.dose_form_group_name) filter (
            where dose_form_group.dose_form_group_name is not null
            ) as dosage
        , string_agg(distinct coalesce(product_dose_form.dose_form_name, product.dose_form_name), ' / ' order by coalesce(product_dose_form.dose_form_name, product.dose_form_name)) filter (
            where coalesce(product_dose_form.dose_form_name, product.dose_form_name) is not null
            ) as form
    from sagerx_dev.products product
    left join sagerx_dev.int_rxnorm_clinical_products_to_dose_forms product_dose_form
        on product_dose_form.clinical_product_rxcui = product.clinical_product_rxcui
    left join dose_form_group_map dose_form_group
        on dose_form_group.dose_form_rxcui = product_dose_form.dose_form_rxcui
    group by product.product_rxcui

),

strength_attributes as materialized (

    select
        rxnsat.rxcui::text as rxcui
        , max(rxnsat.atv) as strength
    from sagerx_lake.rxnorm_rxnsat rxnsat
    where rxnsat.sab = 'RXNORM'
        and rxnsat.atn = 'RXN_STRENGTH'
    group by rxnsat.rxcui

),

ingredient_strength_fields as (

    select
        ingredient_strength.rxcui as concept_rxcui
        , max(coalesce(nullif(strength_attributes.strength, ''), ingredient_strength.rxcui_name)) as strength
        , null::text as dosage
        , null::text as form
    from rxnorm_concepts ingredient_strength
    left join strength_attributes
        on strength_attributes.rxcui = ingredient_strength.rxcui
    where ingredient_strength.rxnorm_tty = 'SCDC'
    group by ingredient_strength.rxcui

),

dose_form_fields as (

    select
        dose_form.rxcui as concept_rxcui
        , null::text as strength
        , string_agg(distinct dose_form_group.dose_form_group_name, ' / ' order by dose_form_group.dose_form_group_name) filter (
            where dose_form_group.dose_form_group_name is not null
            ) as dosage
        , max(dose_form.rxcui_name) as form
    from rxnorm_concepts dose_form
    left join dose_form_group_map dose_form_group
        on dose_form_group.dose_form_rxcui = dose_form.rxcui
    where dose_form.rxnorm_tty = 'DF'
    group by dose_form.rxcui

),

dose_form_group_fields as (

    select
        dose_form_group.rxcui as concept_rxcui
        , null::text as strength
        , max(dose_form_group.rxcui_name) as dosage
        , null::text as form
    from rxnorm_concepts dose_form_group
    where dose_form_group.rxnorm_tty = 'DFG'
    group by dose_form_group.rxcui

),

ingredient_dose_form_fields as materialized (

    select
        concept.rxcui as concept_rxcui
        , null::text as strength
        , string_agg(distinct dose_form_group.dose_form_group_name, ' / ' order by dose_form_group.dose_form_group_name) filter (
            where dose_form_group.dose_form_group_name is not null
            ) as dosage
        , string_agg(distinct dose_form.rxcui_name, ' / ' order by dose_form.rxcui_name) as form
    from rxnorm_concepts concept
    inner join has_dose_form_relations dose_form_relation
        on dose_form_relation.rxcui2 = concept.rxcui
    inner join rxnorm_concepts dose_form
        on dose_form.rxcui = dose_form_relation.rxcui1
        and dose_form.rxnorm_tty = 'DF'
    left join dose_form_group_map dose_form_group
        on dose_form_group.dose_form_rxcui = dose_form.rxcui
    where concept.rxnorm_tty in ('SCDF', 'SCDG')
    group by concept.rxcui

),

precise_ingredient_form_fields as (

    select
        concept.rxcui as concept_rxcui
        , form_fields.strength
        , form_fields.dosage
        , form_fields.form
    from rxnorm_concepts concept
    inner join form_of_relations form_relation
        on form_relation.rxcui2 = concept.rxcui
    inner join ingredient_dose_form_fields form_fields
        on form_fields.concept_rxcui = form_relation.rxcui1
    where concept.rxnorm_tty in ('SCDFP', 'SCDGP')

),

generic_concept_fields as materialized (

    select
        concept_rxcui
        , string_agg(distinct strength, ' / ' order by strength) filter (
            where strength is not null
            ) as strength
        , string_agg(distinct dosage, ' / ' order by dosage) filter (
            where dosage is not null
            ) as dosage
        , string_agg(distinct form, ' / ' order by form) filter (
            where form is not null
            ) as form
    from (
        select * from product_fields

        union all

        select * from ingredient_strength_fields

        union all

        select * from dose_form_fields

        union all

        select * from dose_form_group_fields

        union all

        select * from ingredient_dose_form_fields

        union all

        select * from precise_ingredient_form_fields
    ) field_rows
    group by concept_rxcui

),

branded_concept_fields as (

    select
        brand_concept.rxcui as concept_rxcui
        , generic_fields.strength
        , generic_fields.dosage
        , generic_fields.form
    from rxnorm_concepts brand_concept
    inner join tradename_of_relations generic_relation
        on generic_relation.rxcui2 = brand_concept.rxcui
    inner join generic_concept_fields generic_fields
        on generic_fields.concept_rxcui = generic_relation.rxcui1
    where brand_concept.rxnorm_tty in ('SBDC', 'SBDF', 'SBDFP', 'SBDG')

),

concept_fields as (

    select
        concept_rxcui
        , string_agg(distinct strength, ' / ' order by strength) filter (
            where strength is not null
            ) as strength
        , string_agg(distinct dosage, ' / ' order by dosage) filter (
            where dosage is not null
            ) as dosage
        , string_agg(distinct form, ' / ' order by form) filter (
            where form is not null
            ) as form
    from (
        select * from generic_concept_fields

        union all

        select * from branded_concept_fields
    ) field_rows
    group by concept_rxcui

),

product_context as materialized (

    select
        product.product_rxcui::text as concept_rxcui
        , product.product_rxcui::text as product_rxcui
        , product.clinical_product_rxcui::text as clinical_product_rxcui
        , product_ingredient_summary.selected_ingredient_rxcui
    from sagerx_dev.products product
    inner join product_ingredient_summary
        on product_ingredient_summary.concept_rxcui = product.product_rxcui
    where product_ingredient_summary.selected_ingredient_rxcui is not null

),

product_atc_keys as (

    select
        concept_rxcui
        , selected_ingredient_rxcui
        , product_rxcui as atc_rxcui
    from product_context

    union all

    select
        concept_rxcui
        , selected_ingredient_rxcui
        , clinical_product_rxcui as atc_rxcui
    from product_context
    where clinical_product_rxcui is not null

),

product_atc_rows as materialized (

    select distinct
        product_atc_keys.concept_rxcui
        , product_atc_keys.selected_ingredient_rxcui
        , atc.atc_3_code
        , atc.atc_3_name
        , atc.atc_4_code
        , atc.atc_4_name
        , atc5.atc_5_code
        , atc5.atc_5_name
    from product_atc_keys
    inner join sagerx_dev.atc_codes_to_rxnorm_products atc
        on atc.rxcui = product_atc_keys.atc_rxcui
    left join sagerx_dev.stg_rxnorm__atc_codes atc5
        on atc5.ingredient_rxcui::text = product_atc_keys.selected_ingredient_rxcui
        and atc5.atc_4_code = atc.atc_4_code

),

product_atc as (

    select
        concept_rxcui
        , jsonb_agg(distinct jsonb_build_object(
            'code', atc_3_code,
            'name', atc_3_name
            )) filter (where atc_3_code is not null) as atc3
        , jsonb_agg(distinct jsonb_build_object(
            'code', atc_4_code,
            'name', atc_4_name
            )) filter (where atc_4_code is not null) as atc4
        , jsonb_agg(distinct jsonb_build_object(
            'code', atc_5_code,
            'name', atc_5_name
            )) filter (where atc_5_code is not null) as atc5
    from product_atc_rows
    group by concept_rxcui

),

product_atc_signatures as (

    select
        selected_ingredient_rxcui
        , concept_rxcui
        , string_agg(
            distinct concat(atc_3_code, '::', atc_3_name)
            , '||' order by concat(atc_3_code, '::', atc_3_name)
            ) filter (where atc_3_code is not null) as atc3_signature
        , string_agg(
            distinct concat(atc_4_code, '::', atc_4_name)
            , '||' order by concat(atc_4_code, '::', atc_4_name)
            ) filter (where atc_4_code is not null) as atc4_signature
        , string_agg(
            distinct concat(atc_5_code, '::', atc_5_name)
            , '||' order by concat(atc_5_code, '::', atc_5_name)
            ) filter (where atc_5_code is not null) as atc5_signature
    from product_atc_rows
    group by
        selected_ingredient_rxcui
        , concept_rxcui

),

ingredient_atc_consistency as (

    select
        selected_ingredient_rxcui as ingredient_rxcui
        , count(distinct atc3_signature) filter (
            where atc3_signature is not null
            ) as atc3_signature_count
        , count(distinct atc4_signature) filter (
            where atc4_signature is not null
            ) as atc4_signature_count
        , count(distinct atc5_signature) filter (
            where atc5_signature is not null
            ) as atc5_signature_count
    from product_atc_signatures
    group by selected_ingredient_rxcui

),

ingredient_atc as (

    select
        product_atc_rows.selected_ingredient_rxcui as ingredient_rxcui
        , case
            when max(ingredient_atc_consistency.atc3_signature_count) = 1
                then jsonb_agg(distinct jsonb_build_object(
                    'code', product_atc_rows.atc_3_code,
                    'name', product_atc_rows.atc_3_name
                    )) filter (where product_atc_rows.atc_3_code is not null)
            end as atc3
        , case
            when max(ingredient_atc_consistency.atc4_signature_count) = 1
                then jsonb_agg(distinct jsonb_build_object(
                    'code', product_atc_rows.atc_4_code,
                    'name', product_atc_rows.atc_4_name
                    )) filter (where product_atc_rows.atc_4_code is not null)
            end as atc4
        , case
            when max(ingredient_atc_consistency.atc5_signature_count) = 1
                then jsonb_agg(distinct jsonb_build_object(
                    'code', product_atc_rows.atc_5_code,
                    'name', product_atc_rows.atc_5_name
                    )) filter (where product_atc_rows.atc_5_code is not null)
            end as atc5
    from product_atc_rows
    inner join ingredient_atc_consistency
        on ingredient_atc_consistency.ingredient_rxcui = product_atc_rows.selected_ingredient_rxcui
    group by product_atc_rows.selected_ingredient_rxcui

),

direct_ingredient_atc as (

    select
        coalesce(atc3.ingredient_rxcui, atc4.ingredient_rxcui, atc5.ingredient_rxcui) as ingredient_rxcui
        , atc3.atc3
        , atc4.atc4
        , atc5.atc5
    from (
        select
            ingredient_rxcui::text as ingredient_rxcui
            , jsonb_agg(
                jsonb_build_object(
                    'code', atc_3_code,
                    'name', atc_3_name
                )
                order by atc_3_code, atc_3_name
            ) as atc3
        from (
            select distinct
                ingredient_rxcui
                , atc_3_code
                , atc_3_name
            from sagerx_dev.stg_rxnorm__atc_codes
            where ingredient_rxcui is not null
                and atc_3_code is not null
        ) atc3_rows
        group by ingredient_rxcui
    ) atc3
    full outer join (
        select
            ingredient_rxcui::text as ingredient_rxcui
            , jsonb_agg(
                jsonb_build_object(
                    'code', atc_4_code,
                    'name', atc_4_name
                )
                order by atc_4_code, atc_4_name
            ) as atc4
        from (
            select distinct
                ingredient_rxcui
                , atc_4_code
                , atc_4_name
            from sagerx_dev.stg_rxnorm__atc_codes
            where ingredient_rxcui is not null
                and atc_4_code is not null
        ) atc4_rows
        group by ingredient_rxcui
    ) atc4
        on atc4.ingredient_rxcui = atc3.ingredient_rxcui
    full outer join (
        select
            ingredient_rxcui::text as ingredient_rxcui
            , jsonb_agg(
                jsonb_build_object(
                    'code', atc_5_code,
                    'name', atc_5_name
                )
                order by atc_5_code, atc_5_name
            ) as atc5
        from (
            select distinct
                ingredient_rxcui
                , atc_5_code
                , atc_5_name
            from sagerx_dev.stg_rxnorm__atc_codes
            where ingredient_rxcui is not null
                and atc_5_code is not null
        ) atc5_rows
        group by ingredient_rxcui
    ) atc5
        on atc5.ingredient_rxcui = coalesce(atc3.ingredient_rxcui, atc4.ingredient_rxcui)

),

clinical_drug_ingredient_atc3_rows as (

    select distinct
        clinical_drug_ingredients.concept_rxcui
        , atc3.value as atc
    from clinical_drug_ingredients
    left join inactive_ingredient_concepts inactive_ingredient
        on inactive_ingredient.rxcui = clinical_drug_ingredients.ingredient_rxcui
    inner join ingredient_atc
        on ingredient_atc.ingredient_rxcui = clinical_drug_ingredients.ingredient_rxcui
    inner join lateral jsonb_array_elements(ingredient_atc.atc3) atc3(value)
        on true
    where inactive_ingredient.rxcui is null
        and atc3.value <> 'null'::jsonb

),

clinical_drug_ingredient_atc4_rows as (

    select distinct
        clinical_drug_ingredients.concept_rxcui
        , atc4.value as atc
    from clinical_drug_ingredients
    left join inactive_ingredient_concepts inactive_ingredient
        on inactive_ingredient.rxcui = clinical_drug_ingredients.ingredient_rxcui
    inner join ingredient_atc
        on ingredient_atc.ingredient_rxcui = clinical_drug_ingredients.ingredient_rxcui
    inner join lateral jsonb_array_elements(ingredient_atc.atc4) atc4(value)
        on true
    where inactive_ingredient.rxcui is null
        and atc4.value <> 'null'::jsonb

),

clinical_drug_ingredient_atc5_rows as (

    select distinct
        clinical_drug_ingredients.concept_rxcui
        , atc5.value as atc
    from clinical_drug_ingredients
    left join inactive_ingredient_concepts inactive_ingredient
        on inactive_ingredient.rxcui = clinical_drug_ingredients.ingredient_rxcui
    inner join ingredient_atc
        on ingredient_atc.ingredient_rxcui = clinical_drug_ingredients.ingredient_rxcui
    inner join lateral jsonb_array_elements(ingredient_atc.atc5) atc5(value)
        on true
    where inactive_ingredient.rxcui is null
        and atc5.value <> 'null'::jsonb

),

clinical_drug_ingredient_atc as (

    select
        coalesce(atc3.concept_rxcui, atc4.concept_rxcui, atc5.concept_rxcui) as concept_rxcui
        , atc3.atc3
        , atc4.atc4
        , atc5.atc5
    from (
        select
            concept_rxcui
            , jsonb_agg(atc order by atc ->> 'code', atc ->> 'name') as atc3
        from clinical_drug_ingredient_atc3_rows
        group by concept_rxcui
    ) atc3
    full outer join (
        select
            concept_rxcui
            , jsonb_agg(atc order by atc ->> 'code', atc ->> 'name') as atc4
        from clinical_drug_ingredient_atc4_rows
        group by concept_rxcui
    ) atc4
        on atc4.concept_rxcui = atc3.concept_rxcui
    full outer join (
        select
            concept_rxcui
            , jsonb_agg(atc order by atc ->> 'code', atc ->> 'name') as atc5
        from clinical_drug_ingredient_atc5_rows
        group by concept_rxcui
    ) atc5
        on atc5.concept_rxcui = coalesce(atc3.concept_rxcui, atc4.concept_rxcui)

),

branded_concept_ingredient_atc3_rows as (

    select distinct
        concept_ingredients.concept_rxcui
        , atc3.value as atc
    from concept_ingredients
    inner join rxnorm_concepts concept
        on concept.rxcui = concept_ingredients.concept_rxcui
        and concept.rxnorm_tty in ('BN', 'SBD', 'SBDC', 'SBDF', 'SBDFP', 'SBDG')
    left join inactive_ingredient_concepts inactive_ingredient
        on inactive_ingredient.rxcui = concept_ingredients.ingredient_rxcui
    left join form_of_relations base_ingredient_relation
        on base_ingredient_relation.rxcui2 = concept_ingredients.ingredient_rxcui
    left join rxnorm_concepts base_ingredient
        on base_ingredient.rxcui = base_ingredient_relation.rxcui1
        and base_ingredient.rxnorm_tty in ('IN', 'MIN')
    inner join ingredient_atc
        on ingredient_atc.ingredient_rxcui = coalesce(base_ingredient.rxcui, concept_ingredients.ingredient_rxcui)
    inner join lateral jsonb_array_elements(ingredient_atc.atc3) atc3(value)
        on true
    where inactive_ingredient.rxcui is null
        and atc3.value <> 'null'::jsonb

),

branded_concept_ingredient_atc4_rows as (

    select distinct
        concept_ingredients.concept_rxcui
        , atc4.value as atc
    from concept_ingredients
    inner join rxnorm_concepts concept
        on concept.rxcui = concept_ingredients.concept_rxcui
        and concept.rxnorm_tty in ('BN', 'SBD', 'SBDC', 'SBDF', 'SBDFP', 'SBDG')
    left join inactive_ingredient_concepts inactive_ingredient
        on inactive_ingredient.rxcui = concept_ingredients.ingredient_rxcui
    left join form_of_relations base_ingredient_relation
        on base_ingredient_relation.rxcui2 = concept_ingredients.ingredient_rxcui
    left join rxnorm_concepts base_ingredient
        on base_ingredient.rxcui = base_ingredient_relation.rxcui1
        and base_ingredient.rxnorm_tty in ('IN', 'MIN')
    inner join ingredient_atc
        on ingredient_atc.ingredient_rxcui = coalesce(base_ingredient.rxcui, concept_ingredients.ingredient_rxcui)
    inner join lateral jsonb_array_elements(ingredient_atc.atc4) atc4(value)
        on true
    where inactive_ingredient.rxcui is null
        and atc4.value <> 'null'::jsonb

),

branded_concept_ingredient_atc5_rows as (

    select distinct
        concept_ingredients.concept_rxcui
        , atc5.value as atc
    from concept_ingredients
    inner join rxnorm_concepts concept
        on concept.rxcui = concept_ingredients.concept_rxcui
        and concept.rxnorm_tty in ('BN', 'SBD', 'SBDC', 'SBDF', 'SBDFP', 'SBDG')
    left join inactive_ingredient_concepts inactive_ingredient
        on inactive_ingredient.rxcui = concept_ingredients.ingredient_rxcui
    left join form_of_relations base_ingredient_relation
        on base_ingredient_relation.rxcui2 = concept_ingredients.ingredient_rxcui
    left join rxnorm_concepts base_ingredient
        on base_ingredient.rxcui = base_ingredient_relation.rxcui1
        and base_ingredient.rxnorm_tty in ('IN', 'MIN')
    inner join ingredient_atc
        on ingredient_atc.ingredient_rxcui = coalesce(base_ingredient.rxcui, concept_ingredients.ingredient_rxcui)
    inner join lateral jsonb_array_elements(ingredient_atc.atc5) atc5(value)
        on true
    where inactive_ingredient.rxcui is null
        and atc5.value <> 'null'::jsonb

),

branded_concept_ingredient_atc as (

    select
        coalesce(atc3.concept_rxcui, atc4.concept_rxcui, atc5.concept_rxcui) as concept_rxcui
        , atc3.atc3
        , atc4.atc4
        , atc5.atc5
    from (
        select
            concept_rxcui
            , jsonb_agg(atc order by atc ->> 'code', atc ->> 'name') as atc3
        from branded_concept_ingredient_atc3_rows
        group by concept_rxcui
    ) atc3
    full outer join (
        select
            concept_rxcui
            , jsonb_agg(atc order by atc ->> 'code', atc ->> 'name') as atc4
        from branded_concept_ingredient_atc4_rows
        group by concept_rxcui
    ) atc4
        on atc4.concept_rxcui = atc3.concept_rxcui
    full outer join (
        select
            concept_rxcui
            , jsonb_agg(atc order by atc ->> 'code', atc ->> 'name') as atc5
        from branded_concept_ingredient_atc5_rows
        group by concept_rxcui
    ) atc5
        on atc5.concept_rxcui = coalesce(atc3.concept_rxcui, atc4.concept_rxcui)

),

clinical_product_context as materialized (

    select distinct
        clinical_product_rxcui
        , selected_ingredient_rxcui
    from product_context

),

clinical_product_disease_rows as (

    select distinct
        clinical_product_context.clinical_product_rxcui
        , clinical_product_context.selected_ingredient_rxcui
        , disease.disease_id
        , disease.disease_source
        , disease.class_name
        , disease.disease_name
    from clinical_product_context
    inner join sagerx_dev.clinical_products_to_diseases disease
        on disease.clinical_product_rxcui = clinical_product_context.clinical_product_rxcui
        and disease.via_ingredient_rxcui = clinical_product_context.selected_ingredient_rxcui

),

clinical_product_diseases as (

    select
        clinical_product_rxcui
        , selected_ingredient_rxcui
        , jsonb_agg(jsonb_build_object(
            'disease_id', disease_id,
            'disease_source', disease_source,
            'class_name', class_name,
            'disease_name', disease_name
            )) filter (where disease_id is not null) as diseases
    from clinical_product_disease_rows
    group by
        clinical_product_rxcui
        , selected_ingredient_rxcui

),

product_diseases as (

    select
        product_context.concept_rxcui
        , clinical_product_diseases.diseases
    from product_context
    inner join clinical_product_diseases
        on clinical_product_diseases.clinical_product_rxcui = product_context.clinical_product_rxcui
        and clinical_product_diseases.selected_ingredient_rxcui = product_context.selected_ingredient_rxcui

)

select
    concept.rxcui
    , concept.rxcui_name
    , concept.rxnorm_tty
    , concept_fields.strength
    , concept_fields.dosage
    , concept_fields.form
    , concept.active
    , concept.prescribable
    , inactive_ingredient_concepts.rxcui is not null as is_inactive_ingredient
    , concept_ingredient_name_summary.generic_name
    , nullif(case
        when product_context.concept_rxcui is not null
            then coalesce(product_atc.atc3, direct_ingredient_atc.atc3, ingredient_atc.atc3, clinical_drug_ingredient_atc.atc3, branded_concept_ingredient_atc.atc3)
        else coalesce(direct_ingredient_atc.atc3, ingredient_atc.atc3, clinical_drug_ingredient_atc.atc3, branded_concept_ingredient_atc.atc3)
        end, '[null]'::jsonb) as atc3
    , nullif(case
        when product_context.concept_rxcui is not null
            then coalesce(product_atc.atc4, direct_ingredient_atc.atc4, ingredient_atc.atc4, clinical_drug_ingredient_atc.atc4, branded_concept_ingredient_atc.atc4)
        else coalesce(direct_ingredient_atc.atc4, ingredient_atc.atc4, clinical_drug_ingredient_atc.atc4, branded_concept_ingredient_atc.atc4)
        end, '[null]'::jsonb) as atc4
    , nullif(case
        when product_context.concept_rxcui is not null
            then coalesce(product_atc.atc5, direct_ingredient_atc.atc5, ingredient_atc.atc5, clinical_drug_ingredient_atc.atc5, branded_concept_ingredient_atc.atc5)
        else coalesce(direct_ingredient_atc.atc5, ingredient_atc.atc5, clinical_drug_ingredient_atc.atc5, branded_concept_ingredient_atc.atc5)
        end, '[null]'::jsonb) as atc5
    , nullif(product_diseases.diseases, '[null]'::jsonb) as diseases
from rxnorm_concepts concept
left join concept_fields
    on concept_fields.concept_rxcui = concept.rxcui
left join inactive_ingredient_concepts
    on inactive_ingredient_concepts.rxcui = concept.rxcui
left join concept_ingredient_summary
    on concept_ingredient_summary.concept_rxcui = concept.rxcui
left join concept_ingredient_name_summary
    on concept_ingredient_name_summary.concept_rxcui = concept.rxcui
left join product_context
    on product_context.concept_rxcui = concept.rxcui
left join product_atc
    on product_atc.concept_rxcui = concept.rxcui
left join ingredient_atc
    on ingredient_atc.ingredient_rxcui = concept_ingredient_summary.selected_ingredient_rxcui
left join direct_ingredient_atc
    on direct_ingredient_atc.ingredient_rxcui = concept_ingredient_summary.selected_ingredient_rxcui
left join clinical_drug_ingredient_atc
    on clinical_drug_ingredient_atc.concept_rxcui = concept.rxcui
left join branded_concept_ingredient_atc
    on branded_concept_ingredient_atc.concept_rxcui = concept.rxcui
left join product_diseases
    on product_diseases.concept_rxcui = concept.rxcui;

create unique index rxcui_lookup_rxcui_idx
    on sagerx_dev.rxcui_lookup (rxcui);

create index rxcui_lookup_rxcui_name_idx
    on sagerx_dev.rxcui_lookup (rxcui_name);

create index rxcui_lookup_generic_name_idx
    on sagerx_dev.rxcui_lookup (generic_name);
