I want to build a materialized view as `sagerx_dev.rxcui_lookup` that contains the following information: 

* The unique key for each row is RXCUI
* All possible rxnorm types are included in this table
* Each row will also have the following fields:
  * The RXCUI name `rxcui_name`
  * TTY `rxnorm_tty`
  * Strength `strength`
  * Dosage `dosage`
  * Formulation (tablet, pill, cream, etc.) `form`
  * Active status `active`
  * Prescribable status `prescribable`
  * Whether the medication is ever an inactive ingredient, name this column `is_inactive_ingredient`
  * Generic medication name (be careful that this is truly the generic form of the drug only, and does not include strength, dosage, formulation, or brand name) `generic_name`
  * ATC level 3 and 4 (name the final column `atc3` and `atc4`). The field should be a list of objects, where each object contains the ATC code, and the ATC name. For example: [{"code":"N07B","name":"DRUGS USED IN ADDICTIVE DISORDERS"}].
  * A list of diseases this medication may treat or prevent. The field should be a list of objects, where each object contains the disease_id, disease_source (SNOMEDCT_US, ICD10CM, etc.), class_name, disease_name.

If a clinical product or product has a single ingredient, you can use the ATC and may treat/prevent indications for that ingredient.

If a clinical product or product has multiple ingredients, but only one of them is the active ingredient (not inactive), then use the active ingredient for ATC and may use/may prevent indications. Otherwise, leave ATC and may use/may prevent null.

Please ensure your solution works for all rxnorm TTY types. Sometimes they need to be handled separately. Examples: 

* Certain types like SBDC, SCDC, SCDG, SCDFP, SCDG, SCDGP, SBDFP, and SBDG are not included in the main product table, however information on them is present in the database in auxiliary tables, and should be used. All of these types should be included in the final materialized view/table.

* SBDC types contain a strength, so its rxcui_name cannot be used as the generic name. A generic name for the medication should contain only the generic name itself, without strength or dosage included, and definitely not brand name. Many other types are like this other than SBDC.

* Make sure brand name (BN) types that can be mapped to a single active ingredient get ATC codes and may use/may treat indications. If there are multiple ingredients, attempt to determine if only one is active, and use only the active ingredient.

* When mapping to clinical products or products in order to find diseases or ATC codes, be sure your solution is not overly broad. For example, if an ingredient maps to several products, and each product has different diseases or ATC codes, the mapping may be too broad. Tylenol / acetaminophen as an example is sometimes included in multiple products, but those products can treat vastly different things. 

* For products with multiple ingredients or MIN records, the generic name should include all of the medication names separated by `/`. Make sure these names are unique and not duplicated in the string.

Do not use the code in `airflow/dags/medication_brand_lookup/load_medication_brand_lookup.sql`. This was a previous attempt that became overly complicated and difficult to maintain. You can use the `airflow/dags/medication_brand_lookup/dag.py` dag as a reference, however. 

You do not need to include intermediate or temporary columns in the final table. For example, if you are creating columns in order to coalesce into a final column, the temporary columns should not be kept in the final materialized view.

Please implement your solution as a SQL file in `airflow/dags/rxcui_lookup/` and include a `dag.py` file as well as per above.

Include logic in the SQL file to drop the materialized view if it exists first. Also, please create indexes for rxcui, rxcui_name, and generic name.

Here are some test cases to verify your work: 

* The following rxcuis should have fully populated atc3, atc4, and diseases columns (if they do not, it is an error):
  * rxcui 602397, 563877, and 27390
  * rxcui 25480 is for gabapentin (IN)
  * rxcui 10737 is for trazodone (IN)
  * rxcui 856378 (SBDC containing trazodone and an inactive ingredient)
  * rxcui 411111 (SCD which only contains lorazepam; may need to map to SCDC -> ingredient -> ATC).

