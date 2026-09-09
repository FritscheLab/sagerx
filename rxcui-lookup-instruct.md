# SageRX RXCUI lookup materialized view instructions

Build a PostgreSQL materialized view named `sagerx_dev.rxcui_lookup` for medication lookup and enrichment from the SageRX medication database.

## Output contract

The materialized view must contain exactly one row per distinct RxNorm RXCUI.

Required final columns:

| Column | Description |
|---|---|
| `rxcui` | RxNorm concept unique identifier. This is the unique key for the view. |
| `rxcui_name` | Preferred RxNorm name for the RXCUI. |
| `rxnorm_tty` | RxNorm TTY. Include all RxNorm TTY values available in SageRX. |
| `strength` | Strength, when available. |
| `dosage` | Dosage, when available. |
| `form` | Formulation, such as tablet, capsule, cream, solution, etc., when available. |
| `active` | Whether the RxNorm concept is active according to the SageRX/RxNorm status source. Do not filter inactive concepts out of the view. |
| `prescribable` | Whether the concept is prescribable according to the SageRX/RxNorm source. |
| `is_inactive_ingredient` | `true` if the RXCUI ever appears as an inactive ingredient in any SageRX relationship; otherwise `false`. This does not mean the concept is never active elsewhere. |
| `generic_name` | Generic medication name derived from structured ingredient relationships whenever possible. See rules below. |
| `atc3` | `jsonb` array of unique ATC level 3 objects: `[{"code":"N07B","name":"DRUGS USED IN ADDICTIVE DISORDERS","source_ingredients":[{"rxcui":"...","name":"..."}]}]`. Use `NULL` when the active ingredient set cannot be resolved safely. |
| `atc4` | `jsonb` array of unique ATC level 4 objects with the same object format as `atc3`. Use `NULL` when the active ingredient set cannot be resolved safely. |
| `diseases` | `jsonb` array of unique disease objects: `[{"disease_id":"...","disease_source":"SNOMEDCT_US","class_name":"...","disease_name":"..."}]`. Use `NULL` when no safe disease/indication mapping exists. |

Do not include intermediate or temporary columns in the final materialized view. For example, if you create helper columns only to coalesce into a final field, keep those helper columns inside CTEs and omit them from the final `SELECT`.

## Source and implementation requirements

Implement the solution in `airflow/dags/rxcui_lookup/` with:

1. A SQL file that creates the materialized view.
2. A `dag.py` file to run the SQL as part of Airflow.

The SQL file must:

1. Drop the materialized view first if it already exists.
2. Recreate `sagerx_dev.rxcui_lookup`.
3. Create indexes after the materialized view is built.

Required indexes:

```sql
CREATE UNIQUE INDEX rxcui_lookup_rxcui_idx
ON sagerx_dev.rxcui_lookup (rxcui);

CREATE INDEX rxcui_lookup_rxcui_name_idx
ON sagerx_dev.rxcui_lookup (rxcui_name);

CREATE INDEX rxcui_lookup_generic_name_idx
ON sagerx_dev.rxcui_lookup (generic_name);
```

Do not use the code in `airflow/dags/medication_brand_lookup/load_medication_brand_lookup.sql`. That was a previous attempt that became overly complicated and difficult to maintain. You may use `airflow/dags/medication_brand_lookup/dag.py` as a DAG reference.

## RxNorm TTY coverage

Include all RxNorm TTY types available in SageRX, including active and inactive/suppressed concepts.

Some TTYs are not present in the main product table but are available in auxiliary tables. These must still be included in the final materialized view. Examples include:

- `SBDC`
- `SCDC`
- `SCDG`
- `SCDFP`
- `SCDGP`
- `SBDFP`
- `SBDG`

Handle TTY-specific edge cases explicitly when needed. Do not assume all TTYs can be populated from the same source tables or with the same joins.

## Generic name rules

`generic_name` should describe only the generic medication ingredient set.

It must not contain:

- Brand names
- Strengths, such as `100 mg`
- Dose amounts
- Routes
- Forms, such as tablet, capsule, cream, solution, patch, etc.
- Quantities
- Pack information

Derive `generic_name` from structured ingredient relationships whenever possible. Do not simply copy `rxcui_name` for product-level, branded, strength-specific, dose-specific, form-specific, quantified, or pack concepts.

Rules:

1. For ingredient concepts such as `IN` and `PIN`, use the ingredient name.
2. For single-active-ingredient clinical products or products, use the active ingredient name.
3. For branded concepts such as `BN`, `SBD`, `SBDC`, `SBDG`, `SBDF`, `SBDFP`, or `BPCK`, use the underlying generic ingredient name or names, not the brand name.
4. For multi-ingredient concepts or `MIN` records, concatenate unique ingredient names with `/` in deterministic order.
5. If the ingredient set cannot be resolved safely, set `generic_name` to `NULL` rather than using a potentially misleading parsed name.

Examples:

- An `SBDC` concept often contains strength in `rxcui_name`, so `rxcui_name` must not be used directly as `generic_name`.
- A product containing trazodone plus an inactive ingredient should have `generic_name = 'trazodone'`, not a string containing the inactive ingredient, strength, formulation, or brand name.
- A true multi-active-ingredient product should have each unique generic ingredient included once and separated by `/`.

## ATC and disease enrichment rules

Resolve the active ingredient set for each RXCUI before populating `atc3`, `atc4`, or `diseases`.

ATC and disease enrichment are related but should not use identical rules:

- `atc3` and `atc4` may be populated from one or more resolved active ingredients.
- Every ATC object should include a `source_ingredients` array, even when there is only one source ingredient.
- `diseases` should be populated from ingredient-level mappings only when the RXCUI resolves to exactly one active ingredient.
- For multi-active-ingredient concepts, do not simply union ingredient-level disease/indication mappings. Populate `diseases` only when a disease/indication mapping is available for the exact combination product, exact product concept, or exact RXCUI. Otherwise set `diseases` to `NULL`.

Use these rules:

1. If a clinical product or product has exactly one ingredient, use that ingredient for ATC and disease enrichment.
2. If a clinical product or product has multiple ingredients but exactly one active ingredient and all other ingredients are inactive, use only the active ingredient for ATC and disease enrichment.
3. If a concept has multiple active ingredients and the active ingredient set is unambiguous, populate `atc3` and `atc4` by combining the unique ATC codes from all active ingredients. Do not set ATC to `NULL` solely because there are multiple active ingredients.
4. For multi-active-ingredient concepts, each ATC object must include a `source_ingredients` array containing the active ingredient RXCUI and ingredient name for each active ingredient that contributed that ATC code. If multiple active ingredients contribute the same ATC code, include the code once and include all contributing ingredients in `source_ingredients`.
5. For multi-active-ingredient concepts, set `diseases` to `NULL` unless disease/indication mappings are available for the exact combination product, exact product concept, or exact RXCUI.
6. If a concept has only inactive ingredients, set `atc3`, `atc4`, and `diseases` to `NULL`.
7. If active versus inactive ingredient status cannot be determined unambiguously, set `atc3`, `atc4`, and `diseases` to `NULL`.
8. If a brand-name concept such as `BN` can be mapped to exactly one active ingredient, populate ATC and disease enrichment from that active ingredient.
9. If a brand-name concept maps to multiple active ingredients, populate `atc3` and `atc4` from the resolved active ingredient set as described above, but do not union ingredient-level disease mappings.
10. Do not infer disease annotations by broadly aggregating across all products related to an ingredient if this would mix unrelated indications. For example, acetaminophen/Tylenol may appear in products used for different clinical purposes, so avoid joins that broaden disease mappings beyond the resolved ingredient, exact product concept, or exact RXCUI.

Use deterministic `jsonb` arrays of unique objects for `atc3`, `atc4`, and `diseases`. Sort ATC objects by code and name, sort `source_ingredients` by RXCUI/name, and sort disease objects by source, disease ID, class name, and disease name so repeated builds produce stable results.

Use `NULL` when no safe mapping exists. Use an empty JSON array only if the mapping is safe but no ATC or disease records exist.

## Recommended development/debug fields

The following fields may be useful in intermediate CTEs or development queries, but should not be included in the final materialized view:

- `resolved_ingredient_rxcui`
- `resolved_ingredient_name`
- `active_ingredient_count`
- `inactive_ingredient_count`
- `all_ingredient_count`
- `enrichment_status`
- `generic_name_source`

These fields can make debugging easier and help verify that ambiguous cases are intentionally set to `NULL`.

## Required validation tests

Run the following checks after building the materialized view. Any query that is described as returning zero rows should be treated as a failure if it returns rows.

### 1. One row per RXCUI

This query must return zero rows:

```sql
SELECT rxcui, COUNT(*) AS n
FROM sagerx_dev.rxcui_lookup
GROUP BY rxcui
HAVING COUNT(*) > 1;
```

### 2. Required base fields are populated

This query must return zero rows:

```sql
SELECT *
FROM sagerx_dev.rxcui_lookup
WHERE rxcui IS NULL
   OR rxcui_name IS NULL
   OR rxnorm_tty IS NULL;
```

### 3. Expected TTY coverage

Confirm that every RxNorm TTY represented in the source RxNorm/SageRX concept tables is also represented in `sagerx_dev.rxcui_lookup`. Adjust source table and column names as needed:

```sql
WITH source_ttys AS (
  SELECT DISTINCT tty AS rxnorm_tty
  FROM sagerx_dev.rxnorm_rxnconso
),
lookup_ttys AS (
  SELECT DISTINCT rxnorm_tty
  FROM sagerx_dev.rxcui_lookup
)
SELECT source_ttys.rxnorm_tty
FROM source_ttys
LEFT JOIN lookup_ttys USING (rxnorm_tty)
WHERE lookup_ttys.rxnorm_tty IS NULL;
```

Investigate any returned TTYs. If a source TTY is intentionally excluded, document the reason.

### 4. Known positive enrichment cases

The following RXCUIs should have populated `atc3` and `atc4`. Missing values indicate an error unless the underlying SageRX source data has changed and the reason is documented.

- `602397`
- `563877`
- `27390`
- `25480` - gabapentin, `IN`
- `10737` - trazodone, `IN`
- `856378` - `SBDC` containing trazodone and an inactive ingredient
- `411111` - `SCD` containing lorazepam only
- `128793` - vicodin, `BN`, composed of acetaminophen and hydrocodone, both active ingredients with known ATC codes
- `1310179` - vicodin pill as a `SBDG`
- `1310268` - vicodin as a `SBDC`
- `2656712` - vicodin oral tablet as a `SBDFP`
- `1310202` - vicodin oral tablet as a `SBD`
- `1819` - buprenorphine as a single ingredient `IN`, will need to retrieve ATC directly from `stg_rxnorm__atc_codes`. 

Example test:

```sql
SELECT rxcui, rxcui_name, rxnorm_tty, atc3, atc4, diseases
FROM sagerx_dev.rxcui_lookup
WHERE rxcui IN ('602397', '563877', '27390', '25480', '10737', '856378', '411111', '128793', '1310179', '1310268', '2656712', '1310202')
  AND (
    atc3 IS NULL
    OR atc4 IS NULL
  );
```

This query should return zero rows.

### 5. Known multi-active-ingredient ATC cases

Add test RXCUIs for concepts with multiple active ingredients where the active ingredient set is unambiguous. Include at least one example of each:

1. A multi-active-ingredient product.
2. A multi-active-ingredient `MIN` concept.
3. A brand-name concept mapped to multiple active ingredients.

For these cases, `atc3` and `atc4` may be populated by combining the unique ATC codes from all active ingredients. `diseases` should remain `NULL` unless there is an exact disease/indication mapping for the combination product, exact product concept, or exact RXCUI.

Example pattern:

```sql
SELECT rxcui, rxcui_name, rxnorm_tty, atc3, atc4, diseases
FROM sagerx_dev.rxcui_lookup
WHERE rxcui IN (
  -- Replace with known multi-active RXCUIs from SageRX
  'TODO_MULTI_ACTIVE_PRODUCT_RXCUI',
  'TODO_MULTI_ACTIVE_MIN_RXCUI',
  'TODO_MULTI_ACTIVE_BRAND_RXCUI'
)
AND (
  atc3 IS NULL
  OR atc4 IS NULL
  OR diseases IS NOT NULL
);
```

This query should return zero rows after the TODO RXCUIs are replaced with known examples that have ATC mappings and no exact combination-level disease mappings. If an exact combination-level disease mapping exists for a test case, remove that RXCUI from this negative disease check and document the source of the exact mapping.

### 6. Known ambiguous or inactive-only enrichment cases

Add test RXCUIs for concepts that should not receive ATC or disease enrichment because the active ingredient set is unsafe or unavailable. Include at least one example of each:

1. A product where active versus inactive ingredient status cannot be determined.
2. A concept whose only resolvable ingredient is inactive.
3. A concept whose ingredient relationships cannot be resolved reliably.

For these cases, `atc3`, `atc4`, and `diseases` should be `NULL`.

Example pattern:

```sql
SELECT rxcui, rxcui_name, rxnorm_tty, atc3, atc4, diseases
FROM sagerx_dev.rxcui_lookup
WHERE rxcui IN (
  -- Replace with known ambiguous or inactive-only RXCUIs from SageRX
  'TODO_AMBIGUOUS_ACTIVE_STATUS_RXCUI',
  'TODO_INACTIVE_ONLY_RXCUI',
  'TODO_UNRESOLVED_INGREDIENT_RELATIONSHIP_RXCUI'
)
AND (
  atc3 IS NOT NULL
  OR atc4 IS NOT NULL
  OR diseases IS NOT NULL
);
```

This query should return zero rows after the TODO RXCUIs are replaced with known examples.

### 7. Generic names should not contain strengths

This query should return zero rows or only documented false positives:

```sql
SELECT rxcui, rxcui_name, rxnorm_tty, generic_name
FROM sagerx_dev.rxcui_lookup
WHERE generic_name ~* '\m[0-9]+(\.[0-9]+)?\s*(mg|mcg|g|ml|%)\M';
```

### 8. Generic names should not contain obvious brand leakage for branded concepts

Review any returned rows:

```sql
SELECT rxcui, rxcui_name, rxnorm_tty, generic_name
FROM sagerx_dev.rxcui_lookup
WHERE rxnorm_tty IN ('BN', 'SBD', 'SBDC', 'SBDG', 'SBDF', 'SBDFP', 'BPCK')
  AND generic_name IS NOT NULL
  AND generic_name ILIKE '%' || rxcui_name || '%';
```

This is a heuristic test and may need manual review.

### 9. Multi-ingredient generic names should be unique and deterministic

For known multi-ingredient or `MIN` concepts, confirm that:

1. Ingredients are separated by `/`.
2. Each ingredient appears once.
3. Ingredient ordering is deterministic across builds.
4. Inactive ingredients are not included when the intended generic name is based only on active ingredients.

### 10. ATC JSON object shape

These queries should return zero rows:

```sql
SELECT rxcui, elem
FROM sagerx_dev.rxcui_lookup,
LATERAL jsonb_array_elements(atc3) elem
WHERE atc3 IS NOT NULL
  AND NOT (
    elem ? 'code'
    AND elem ? 'name'
    AND elem ? 'source_ingredients'
    AND jsonb_typeof(elem -> 'source_ingredients') = 'array'
  );
```

```sql
SELECT rxcui, elem
FROM sagerx_dev.rxcui_lookup,
LATERAL jsonb_array_elements(atc4) elem
WHERE atc4 IS NOT NULL
  AND NOT (
    elem ? 'code'
    AND elem ? 'name'
    AND elem ? 'source_ingredients'
    AND jsonb_typeof(elem -> 'source_ingredients') = 'array'
  );
```

These queries verify the shape of each ATC `source_ingredients` object and should also return zero rows:

```sql
SELECT rxcui, elem AS atc_object, source_elem
FROM sagerx_dev.rxcui_lookup,
LATERAL jsonb_array_elements(atc3) elem,
LATERAL jsonb_array_elements(elem -> 'source_ingredients') source_elem
WHERE atc3 IS NOT NULL
  AND NOT (source_elem ? 'rxcui' AND source_elem ? 'name');
```

```sql
SELECT rxcui, elem AS atc_object, source_elem
FROM sagerx_dev.rxcui_lookup,
LATERAL jsonb_array_elements(atc4) elem,
LATERAL jsonb_array_elements(elem -> 'source_ingredients') source_elem
WHERE atc4 IS NOT NULL
  AND NOT (source_elem ? 'rxcui' AND source_elem ? 'name');
```

### 11. Disease JSON object shape

This query should return zero rows:

```sql
SELECT rxcui, elem
FROM sagerx_dev.rxcui_lookup,
LATERAL jsonb_array_elements(diseases) elem
WHERE diseases IS NOT NULL
  AND NOT (
    elem ? 'disease_id'
    AND elem ? 'disease_source'
    AND elem ? 'class_name'
    AND elem ? 'disease_name'
  );
```

### 12. Duplicate JSON objects

These queries should return zero rows:

```sql
SELECT rxcui, elem, COUNT(*) AS n
FROM sagerx_dev.rxcui_lookup,
LATERAL jsonb_array_elements(atc3) elem
WHERE atc3 IS NOT NULL
GROUP BY rxcui, elem
HAVING COUNT(*) > 1;
```

```sql
SELECT rxcui, elem, COUNT(*) AS n
FROM sagerx_dev.rxcui_lookup,
LATERAL jsonb_array_elements(atc4) elem
WHERE atc4 IS NOT NULL
GROUP BY rxcui, elem
HAVING COUNT(*) > 1;
```

```sql
SELECT rxcui, elem, COUNT(*) AS n
FROM sagerx_dev.rxcui_lookup,
LATERAL jsonb_array_elements(diseases) elem
WHERE diseases IS NOT NULL
GROUP BY rxcui, elem
HAVING COUNT(*) > 1;
```

### 13. Index existence

Confirm that the expected indexes exist:

```sql
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'sagerx_dev'
  AND tablename = 'rxcui_lookup'
  AND indexname IN (
    'rxcui_lookup_rxcui_idx',
    'rxcui_lookup_rxcui_name_idx',
    'rxcui_lookup_generic_name_idx'
  );
```

There should be three returned rows, and `rxcui_lookup_rxcui_idx` should be unique.
