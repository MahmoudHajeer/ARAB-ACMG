from __future__ import annotations

from typing import Final

PROJECT_ID: Final[str] = "genome-services-platform"
RAW_DATASET: Final[str] = "arab_acmg_raw"
RESULTS_DATASET: Final[str] = "arab_acmg_results"
REGISTRY_TABLE: Final[str] = "supervisor_variant_registry_v1"
REGISTRY_TABLE_REF: Final[str] = f"{PROJECT_ID}.{RESULTS_DATASET}.{REGISTRY_TABLE}"

CLINVAR_TABLE_REF: Final[str] = f"{PROJECT_ID}.{RAW_DATASET}.clinvar_raw_vcf"
GME_TABLE_REF: Final[str] = f"{PROJECT_ID}.{RAW_DATASET}.gme_hg38_raw"
GNOMAD_TABLE_REFS: Final[dict[str, str]] = {
    "genomes_chr13": f"{PROJECT_ID}.{RAW_DATASET}.gnomad_v4_1_genomes_chr13_raw",
    "genomes_chr17": f"{PROJECT_ID}.{RAW_DATASET}.gnomad_v4_1_genomes_chr17_raw",
    "exomes_chr13": f"{PROJECT_ID}.{RAW_DATASET}.gnomad_v4_1_exomes_chr13_raw",
    "exomes_chr17": f"{PROJECT_ID}.{RAW_DATASET}.gnomad_v4_1_exomes_chr17_raw",
}

# [AI-Agent: Codex]: Keep the integration SQL in one place so the build script,
# runtime API, and UI all expose the exact same logic without drift.
COMMON_REGISTRY_CTES: Final[str] = f"""
clinvar_alleles AS (
  SELECT
    REGEXP_REPLACE(chrom, r'^chr', '') AS chrom_norm,
    pos,
    ref,
    alt_value AS alt,
    CONCAT(REGEXP_REPLACE(chrom, r'^chr', ''), ':', CAST(pos AS STRING), ':', ref, ':', alt_value) AS variant_key,
    id AS clinvar_id,
    REGEXP_EXTRACT(info, r'(?:^|;)CLNSIG=([^;]+)') AS clinvar_significance,
    REGEXP_EXTRACT(info, r'(?:^|;)CLNREVSTAT=([^;]+)') AS clinvar_review_status
  FROM `{CLINVAR_TABLE_REF}`,
  UNNEST(SPLIT(alt, ',')) AS alt_value
  WHERE REGEXP_REPLACE(chrom, r'^chr', '') IN ('13', '17')
),
clinvar_wide AS (
  SELECT
    variant_key,
    ANY_VALUE(chrom_norm) AS chrom,
    ANY_VALUE(pos) AS pos,
    ANY_VALUE(ref) AS ref,
    ANY_VALUE(alt) AS alt,
    ANY_VALUE(clinvar_id) AS clinvar_id,
    ANY_VALUE(clinvar_significance) AS clinvar_significance,
    ANY_VALUE(clinvar_review_status) AS clinvar_review_status
  FROM clinvar_alleles
  GROUP BY variant_key
),
gme_wide AS (
  SELECT
    CONCAT(REGEXP_REPLACE(chrom, r'^chr', ''), ':', CAST(start AS STRING), ':', ref, ':', alt) AS variant_key,
    REGEXP_REPLACE(chrom, r'^chr', '') AS chrom,
    start AS pos,
    ref,
    alt,
    gme_af,
    gme_nwa,
    gme_nea,
    gme_ap,
    gme_israel,
    gme_sd,
    gme_tp,
    gme_ca
  FROM `{GME_TABLE_REF}`
  WHERE REGEXP_REPLACE(chrom, r'^chr', '') IN ('13', '17')
),
gnomad_union AS (
  SELECT 'genomes' AS cohort, chrom, pos, ref, alt, info FROM `{GNOMAD_TABLE_REFS['genomes_chr13']}`
  UNION ALL
  SELECT 'genomes' AS cohort, chrom, pos, ref, alt, info FROM `{GNOMAD_TABLE_REFS['genomes_chr17']}`
  UNION ALL
  SELECT 'exomes' AS cohort, chrom, pos, ref, alt, info FROM `{GNOMAD_TABLE_REFS['exomes_chr13']}`
  UNION ALL
  SELECT 'exomes' AS cohort, chrom, pos, ref, alt, info FROM `{GNOMAD_TABLE_REFS['exomes_chr17']}`
),
gnomad_alleles AS (
  SELECT
    cohort,
    REGEXP_REPLACE(chrom, r'^chr', '') AS chrom_norm,
    pos,
    ref,
    alt_value AS alt,
    alt_offset,
    info,
    CONCAT(REGEXP_REPLACE(chrom, r'^chr', ''), ':', CAST(pos AS STRING), ':', ref, ':', alt_value) AS variant_key
  FROM gnomad_union,
  UNNEST(SPLIT(alt, ',')) AS alt_value WITH OFFSET alt_offset
),
gnomad_parsed AS (
  SELECT
    cohort,
    chrom_norm AS chrom,
    pos,
    ref,
    alt,
    variant_key,
    SAFE_CAST(ac_tokens[SAFE_OFFSET(alt_offset)] AS INT64) AS ac,
    SAFE_CAST(an_token AS INT64) AS an,
    SAFE_CAST(af_tokens[SAFE_OFFSET(alt_offset)] AS FLOAT64) AS af,
    REGEXP_EXTRACT(info, r'(?:^|;)grpmax=([^;]+)') AS grpmax_population,
    SAFE_CAST(REGEXP_EXTRACT(info, r'(?:^|;)faf95=([^;]+)') AS FLOAT64) AS grpmax_faf95,
    CAST(NULL AS FLOAT64) AS depth,
    SAFE_CAST(ac_afr_tokens[SAFE_OFFSET(alt_offset)] AS INT64) AS ac_afr,
    SAFE_CAST(af_afr_tokens[SAFE_OFFSET(alt_offset)] AS FLOAT64) AS af_afr,
    SAFE_CAST(ac_nfe_tokens[SAFE_OFFSET(alt_offset)] AS INT64) AS ac_nfe,
    SAFE_CAST(af_nfe_tokens[SAFE_OFFSET(alt_offset)] AS FLOAT64) AS af_nfe,
    SAFE_CAST(ac_fin_tokens[SAFE_OFFSET(alt_offset)] AS INT64) AS ac_fin,
    SAFE_CAST(af_fin_tokens[SAFE_OFFSET(alt_offset)] AS FLOAT64) AS af_fin,
    SAFE_CAST(ac_asj_tokens[SAFE_OFFSET(alt_offset)] AS INT64) AS ac_asj,
    SAFE_CAST(af_asj_tokens[SAFE_OFFSET(alt_offset)] AS FLOAT64) AS af_asj,
    COALESCE(SAFE_CAST(an_nfe_token AS INT64), 0) + COALESCE(SAFE_CAST(an_fin_token AS INT64), 0) + COALESCE(SAFE_CAST(an_asj_token AS INT64), 0) AS an_eur_proxy,
    COALESCE(SAFE_CAST(ac_nfe_tokens[SAFE_OFFSET(alt_offset)] AS INT64), 0) + COALESCE(SAFE_CAST(ac_fin_tokens[SAFE_OFFSET(alt_offset)] AS INT64), 0) + COALESCE(SAFE_CAST(ac_asj_tokens[SAFE_OFFSET(alt_offset)] AS INT64), 0) AS ac_eur_proxy,
    SAFE_DIVIDE(
      COALESCE(SAFE_CAST(ac_nfe_tokens[SAFE_OFFSET(alt_offset)] AS INT64), 0) + COALESCE(SAFE_CAST(ac_fin_tokens[SAFE_OFFSET(alt_offset)] AS INT64), 0) + COALESCE(SAFE_CAST(ac_asj_tokens[SAFE_OFFSET(alt_offset)] AS INT64), 0),
      NULLIF(COALESCE(SAFE_CAST(an_nfe_token AS INT64), 0) + COALESCE(SAFE_CAST(an_fin_token AS INT64), 0) + COALESCE(SAFE_CAST(an_asj_token AS INT64), 0), 0)
    ) AS af_eur_proxy
  FROM (
    SELECT
      cohort,
      chrom_norm,
      pos,
      ref,
      alt,
      alt_offset,
      info,
      variant_key,
      SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC=([^;]+)'), ''), ',') AS ac_tokens,
      REGEXP_EXTRACT(info, r'(?:^|;)AN=([^;]+)') AS an_token,
      SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AF=([^;]+)'), ''), ',') AS af_tokens,
      SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC_afr=([^;]+)'), ''), ',') AS ac_afr_tokens,
      SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AF_afr=([^;]+)'), ''), ',') AS af_afr_tokens,
      SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC_nfe=([^;]+)'), ''), ',') AS ac_nfe_tokens,
      REGEXP_EXTRACT(info, r'(?:^|;)AN_nfe=([^;]+)') AS an_nfe_token,
      SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AF_nfe=([^;]+)'), ''), ',') AS af_nfe_tokens,
      SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC_fin=([^;]+)'), ''), ',') AS ac_fin_tokens,
      REGEXP_EXTRACT(info, r'(?:^|;)AN_fin=([^;]+)') AS an_fin_token,
      SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AF_fin=([^;]+)'), ''), ',') AS af_fin_tokens,
      SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC_asj=([^;]+)'), ''), ',') AS ac_asj_tokens,
      REGEXP_EXTRACT(info, r'(?:^|;)AN_asj=([^;]+)') AS an_asj_token,
      SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AF_asj=([^;]+)'), ''), ',') AS af_asj_tokens
    FROM gnomad_alleles
  )
),
gnomad_wide AS (
  SELECT
    variant_key,
    ANY_VALUE(chrom) AS chrom,
    ANY_VALUE(pos) AS pos,
    ANY_VALUE(ref) AS ref,
    ANY_VALUE(alt) AS alt,
    MAX(IF(cohort = 'genomes', TRUE, FALSE)) AS present_in_gnomad_genomes,
    MAX(IF(cohort = 'exomes', TRUE, FALSE)) AS present_in_gnomad_exomes,
    MAX(IF(cohort = 'genomes', ac, NULL)) AS gnomad_genomes_ac,
    MAX(IF(cohort = 'genomes', an, NULL)) AS gnomad_genomes_an,
    MAX(IF(cohort = 'genomes', af, NULL)) AS gnomad_genomes_af,
    MAX(IF(cohort = 'genomes', grpmax_population, NULL)) AS gnomad_genomes_grpmax,
    MAX(IF(cohort = 'genomes', grpmax_faf95, NULL)) AS gnomad_genomes_grpmax_faf95,
    MAX(IF(cohort = 'genomes', depth, NULL)) AS gnomad_genomes_depth,
    MAX(IF(cohort = 'genomes', ac_afr, NULL)) AS gnomad_genomes_ac_afr,
    MAX(IF(cohort = 'genomes', af_afr, NULL)) AS gnomad_genomes_af_afr,
    MAX(IF(cohort = 'genomes', ac_eur_proxy, NULL)) AS gnomad_genomes_ac_eur_proxy,
    MAX(IF(cohort = 'genomes', af_eur_proxy, NULL)) AS gnomad_genomes_af_eur_proxy,
    MAX(IF(cohort = 'exomes', ac, NULL)) AS gnomad_exomes_ac,
    MAX(IF(cohort = 'exomes', an, NULL)) AS gnomad_exomes_an,
    MAX(IF(cohort = 'exomes', af, NULL)) AS gnomad_exomes_af,
    MAX(IF(cohort = 'exomes', grpmax_population, NULL)) AS gnomad_exomes_grpmax,
    MAX(IF(cohort = 'exomes', grpmax_faf95, NULL)) AS gnomad_exomes_grpmax_faf95,
    MAX(IF(cohort = 'exomes', depth, NULL)) AS gnomad_exomes_depth,
    MAX(IF(cohort = 'exomes', ac_afr, NULL)) AS gnomad_exomes_ac_afr,
    MAX(IF(cohort = 'exomes', af_afr, NULL)) AS gnomad_exomes_af_afr,
    MAX(IF(cohort = 'exomes', ac_eur_proxy, NULL)) AS gnomad_exomes_ac_eur_proxy,
    MAX(IF(cohort = 'exomes', af_eur_proxy, NULL)) AS gnomad_exomes_af_eur_proxy
  FROM gnomad_parsed
  GROUP BY variant_key
),
all_keys AS (
  SELECT variant_key, chrom, pos, ref, alt FROM gnomad_wide
  UNION DISTINCT
  SELECT variant_key, chrom, pos, ref, alt FROM clinvar_wide
  UNION DISTINCT
  SELECT variant_key, chrom, pos, ref, alt FROM gme_wide
)
"""

FINAL_REGISTRY_SELECT: Final[str] = """
SELECT
  all_keys.variant_key,
  all_keys.chrom,
  all_keys.pos,
  all_keys.ref,
  all_keys.alt,
  clinvar_wide.variant_key IS NOT NULL AS present_in_clinvar,
  COALESCE(gnomad_wide.present_in_gnomad_genomes, FALSE) AS present_in_gnomad_genomes,
  COALESCE(gnomad_wide.present_in_gnomad_exomes, FALSE) AS present_in_gnomad_exomes,
  gme_wide.variant_key IS NOT NULL AS present_in_gme,
  clinvar_wide.clinvar_id,
  clinvar_wide.clinvar_significance,
  clinvar_wide.clinvar_review_status,
  gnomad_wide.gnomad_genomes_ac,
  gnomad_wide.gnomad_genomes_an,
  gnomad_wide.gnomad_genomes_af,
  gnomad_wide.gnomad_genomes_grpmax,
  gnomad_wide.gnomad_genomes_grpmax_faf95,
  gnomad_wide.gnomad_genomes_depth,
  gnomad_wide.gnomad_genomes_ac_afr,
  gnomad_wide.gnomad_genomes_af_afr,
  gnomad_wide.gnomad_genomes_ac_eur_proxy,
  gnomad_wide.gnomad_genomes_af_eur_proxy,
  gnomad_wide.gnomad_exomes_ac,
  gnomad_wide.gnomad_exomes_an,
  gnomad_wide.gnomad_exomes_af,
  gnomad_wide.gnomad_exomes_grpmax,
  gnomad_wide.gnomad_exomes_grpmax_faf95,
  gnomad_wide.gnomad_exomes_depth,
  gnomad_wide.gnomad_exomes_ac_afr,
  gnomad_wide.gnomad_exomes_af_afr,
  gnomad_wide.gnomad_exomes_ac_eur_proxy,
  gnomad_wide.gnomad_exomes_af_eur_proxy,
  gme_wide.gme_af,
  gme_wide.gme_nwa,
  gme_wide.gme_nea,
  gme_wide.gme_ap,
  gme_wide.gme_israel,
  gme_wide.gme_sd,
  gme_wide.gme_tp,
  gme_wide.gme_ca,
  IF(clinvar_wide.variant_key IS NOT NULL, 1, 0)
    + IF(COALESCE(gnomad_wide.present_in_gnomad_genomes, FALSE), 1, 0)
    + IF(COALESCE(gnomad_wide.present_in_gnomad_exomes, FALSE), 1, 0)
    + IF(gme_wide.variant_key IS NOT NULL, 1, 0) AS source_count,
  CURRENT_DATE() AS last_refresh_date
FROM all_keys
LEFT JOIN clinvar_wide USING (variant_key)
LEFT JOIN gnomad_wide USING (variant_key)
LEFT JOIN gme_wide USING (variant_key)
"""


def build_registry_sql() -> str:
    return (
        f"CREATE OR REPLACE TABLE `{REGISTRY_TABLE_REF}` AS\n"
        f"WITH\n{COMMON_REGISTRY_CTES}\n"
        f"{FINAL_REGISTRY_SELECT}\n"
        "ORDER BY chrom, pos, ref, alt"
    )


def build_registry_sample_sql(sample_percent: float = 0.2, limit: int = 50) -> str:
    return f"""
WITH sampled AS (
  SELECT *
  FROM `{REGISTRY_TABLE_REF}` TABLESAMPLE SYSTEM ({sample_percent} PERCENT)
),
numbered AS (
  SELECT ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number, *
  FROM sampled
)
SELECT *
FROM numbered
WHERE sample_row_number <= {limit}
ORDER BY sample_row_number
""".strip()


def build_registry_step_sql(step_id: str, limit: int = 50) -> str:
    if step_id == "scope":
        return f"""
SELECT 'clinvar_raw_vcf' AS source_table, COUNT(*) AS rows_in_scope
FROM `{CLINVAR_TABLE_REF}`
WHERE REGEXP_REPLACE(chrom, r'^chr', '') IN ('13', '17')
UNION ALL
SELECT 'gnomad_v4_1_genomes_chr13_raw' AS source_table, COUNT(*) AS rows_in_scope
FROM `{GNOMAD_TABLE_REFS['genomes_chr13']}`
UNION ALL
SELECT 'gnomad_v4_1_genomes_chr17_raw' AS source_table, COUNT(*) AS rows_in_scope
FROM `{GNOMAD_TABLE_REFS['genomes_chr17']}`
UNION ALL
SELECT 'gnomad_v4_1_exomes_chr13_raw' AS source_table, COUNT(*) AS rows_in_scope
FROM `{GNOMAD_TABLE_REFS['exomes_chr13']}`
UNION ALL
SELECT 'gnomad_v4_1_exomes_chr17_raw' AS source_table, COUNT(*) AS rows_in_scope
FROM `{GNOMAD_TABLE_REFS['exomes_chr17']}`
UNION ALL
SELECT 'gme_hg38_raw' AS source_table, COUNT(*) AS rows_in_scope
FROM `{GME_TABLE_REF}`
WHERE REGEXP_REPLACE(chrom, r'^chr', '') IN ('13', '17')
ORDER BY source_table
""".strip()

    if step_id == "gnomad_alleles":
        return f"""
WITH
{COMMON_REGISTRY_CTES}
SELECT
  ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number,
  cohort,
  chrom_norm AS chrom,
  pos,
  ref,
  alt,
  alt_offset,
  variant_key
FROM gnomad_alleles
QUALIFY sample_row_number <= {limit}
ORDER BY sample_row_number
""".strip()

    if step_id == "gnomad_metrics":
        return f"""
WITH
{COMMON_REGISTRY_CTES}
SELECT
  ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number,
  cohort,
  chrom,
  pos,
  ref,
  alt,
  ac,
  an,
  af,
  grpmax_population,
  grpmax_faf95,
  depth,
  ac_afr,
  af_afr,
  ac_eur_proxy,
  af_eur_proxy
FROM gnomad_parsed
QUALIFY sample_row_number <= {limit}
ORDER BY sample_row_number
""".strip()

    if step_id == "clinvar_labels":
        return f"""
WITH
{COMMON_REGISTRY_CTES}
SELECT
  ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number,
  chrom,
  pos,
  ref,
  alt,
  clinvar_id,
  clinvar_significance,
  clinvar_review_status
FROM clinvar_wide
QUALIFY sample_row_number <= {limit}
ORDER BY sample_row_number
""".strip()

    if step_id == "registry_final":
        return build_registry_sample_sql(limit=limit)

    raise KeyError(step_id)
