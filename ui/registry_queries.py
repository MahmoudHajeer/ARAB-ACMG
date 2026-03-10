from __future__ import annotations

from typing import Final

PROJECT_ID: Final[str] = "genome-services-platform"
HARMONIZED_DATASET: Final[str] = "arab_acmg_harmonized"
REGISTRY_TABLE: Final[str] = "supervisor_variant_registry_brca_v1"
REGISTRY_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.{REGISTRY_TABLE}"

GENE_WINDOWS_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.h_brca_gene_windows"
CLINVAR_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.h_brca_clinvar_variants"
GNOMAD_GENOMES_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.h_brca_gnomad_genomes_variants"
GNOMAD_EXOMES_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.h_brca_gnomad_exomes_variants"
GME_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.h_brca_gme_variants"

HARMONIZED_TABLE_REFS: Final[dict[str, str]] = {
    "h_brca_gene_windows": GENE_WINDOWS_TABLE_REF,
    "h_brca_clinvar_variants": CLINVAR_TABLE_REF,
    "h_brca_gnomad_genomes_variants": GNOMAD_GENOMES_TABLE_REF,
    "h_brca_gnomad_exomes_variants": GNOMAD_EXOMES_TABLE_REF,
    "h_brca_gme_variants": GME_TABLE_REF,
}

COMMON_REGISTRY_CTES: Final[str] = f"""
all_keys AS (
  SELECT gene_symbol, variant_key, chrom38 AS chrom, pos38 AS pos, ref_norm AS ref, alt_norm AS alt
  FROM `{CLINVAR_TABLE_REF}`
  UNION DISTINCT
  SELECT gene_symbol, variant_key, chrom38 AS chrom, pos38 AS pos, ref_norm AS ref, alt_norm AS alt
  FROM `{GNOMAD_GENOMES_TABLE_REF}`
  UNION DISTINCT
  SELECT gene_symbol, variant_key, chrom38 AS chrom, pos38 AS pos, ref_norm AS ref, alt_norm AS alt
  FROM `{GNOMAD_EXOMES_TABLE_REF}`
  UNION DISTINCT
  SELECT gene_symbol, variant_key, chrom38 AS chrom, pos38 AS pos, ref_norm AS ref, alt_norm AS alt
  FROM `{GME_TABLE_REF}`
)
"""

FINAL_REGISTRY_SELECT: Final[str] = f"""
SELECT
  all_keys.gene_symbol,
  all_keys.variant_key,
  all_keys.chrom,
  all_keys.pos,
  all_keys.ref,
  all_keys.alt,
  clinvar.variant_key IS NOT NULL AS present_in_clinvar,
  genomes.variant_key IS NOT NULL AS present_in_gnomad_genomes,
  exomes.variant_key IS NOT NULL AS present_in_gnomad_exomes,
  gme.variant_key IS NOT NULL AS present_in_gme,
  clinvar.clinvar_ids,
  clinvar.clinvar_significance_values,
  clinvar.clinvar_review_status_values,
  clinvar.clinvar_record_count,
  clinvar.gene_info_match_count AS clinvar_gene_info_match_count,
  clinvar.gene_info_mismatch_count AS clinvar_gene_info_mismatch_count,
  genomes.ac AS gnomad_genomes_ac,
  genomes.an AS gnomad_genomes_an,
  genomes.af AS gnomad_genomes_af,
  genomes.grpmax_population AS gnomad_genomes_grpmax,
  genomes.grpmax_faf95 AS gnomad_genomes_grpmax_faf95,
  genomes.depth AS gnomad_genomes_depth,
  genomes.ac_afr AS gnomad_genomes_ac_afr,
  genomes.af_afr AS gnomad_genomes_af_afr,
  genomes.ac_eur_proxy AS gnomad_genomes_ac_eur_proxy,
  genomes.af_eur_proxy AS gnomad_genomes_af_eur_proxy,
  exomes.ac AS gnomad_exomes_ac,
  exomes.an AS gnomad_exomes_an,
  exomes.af AS gnomad_exomes_af,
  exomes.grpmax_population AS gnomad_exomes_grpmax,
  exomes.grpmax_faf95 AS gnomad_exomes_grpmax_faf95,
  exomes.depth AS gnomad_exomes_depth,
  exomes.ac_afr AS gnomad_exomes_ac_afr,
  exomes.af_afr AS gnomad_exomes_af_afr,
  exomes.ac_eur_proxy AS gnomad_exomes_ac_eur_proxy,
  exomes.af_eur_proxy AS gnomad_exomes_af_eur_proxy,
  gme.gme_af,
  gme.gme_nwa,
  gme.gme_nea,
  gme.gme_ap,
  gme.gme_israel,
  gme.gme_sd,
  gme.gme_tp,
  gme.gme_ca,
  IF(clinvar.variant_key IS NOT NULL, 1, 0)
    + IF(genomes.variant_key IS NOT NULL, 1, 0)
    + IF(exomes.variant_key IS NOT NULL, 1, 0)
    + IF(gme.variant_key IS NOT NULL, 1, 0) AS source_count,
  CURRENT_DATE() AS last_refresh_date
FROM all_keys
LEFT JOIN `{CLINVAR_TABLE_REF}` AS clinvar USING (gene_symbol, variant_key)
LEFT JOIN `{GNOMAD_GENOMES_TABLE_REF}` AS genomes USING (gene_symbol, variant_key)
LEFT JOIN `{GNOMAD_EXOMES_TABLE_REF}` AS exomes USING (gene_symbol, variant_key)
LEFT JOIN `{GME_TABLE_REF}` AS gme USING (gene_symbol, variant_key)
"""


def build_registry_sql() -> str:
    return (
        f"CREATE OR REPLACE TABLE `{REGISTRY_TABLE_REF}` AS\n"
        f"WITH\n{COMMON_REGISTRY_CTES}\n"
        f"{FINAL_REGISTRY_SELECT}\n"
        "ORDER BY gene_symbol, chrom, pos, ref, alt"
    )


def build_sample_sql(table_ref: str, sample_percent: float, limit: int = 10) -> str:
    if table_ref == GENE_WINDOWS_TABLE_REF:
        return f"""
SELECT
  ROW_NUMBER() OVER (ORDER BY gene_symbol) AS sample_row_number,
  *
FROM `{table_ref}`
ORDER BY sample_row_number
LIMIT {limit}
""".strip()

    return f"""
WITH sampled AS (
  SELECT *
  FROM `{table_ref}` TABLESAMPLE SYSTEM ({sample_percent} PERCENT)
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


def build_registry_sample_sql(sample_percent: float = 12.0, limit: int = 10) -> str:
    return f"""
WITH numbered AS (
  SELECT ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number, *
  FROM `{REGISTRY_TABLE_REF}`
)
SELECT *
FROM numbered
WHERE sample_row_number <= {limit}
ORDER BY sample_row_number
""".strip()


def build_registry_step_sql(step_id: str, limit: int = 10) -> str:
    if step_id == "gene_windows":
        return build_sample_sql(GENE_WINDOWS_TABLE_REF, sample_percent=100.0, limit=limit)
    if step_id == "clinvar_brca":
        return build_sample_sql(CLINVAR_TABLE_REF, sample_percent=15.0, limit=limit)
    if step_id == "gnomad_genomes_brca":
        return build_sample_sql(GNOMAD_GENOMES_TABLE_REF, sample_percent=8.0, limit=limit)
    if step_id == "gnomad_exomes_brca":
        return build_sample_sql(GNOMAD_EXOMES_TABLE_REF, sample_percent=12.0, limit=limit)
    if step_id == "gme_brca":
        return build_sample_sql(GME_TABLE_REF, sample_percent=100.0, limit=limit)
    if step_id == "registry_final":
        return build_registry_sample_sql(limit=limit)
    raise KeyError(step_id)
