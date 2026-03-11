from __future__ import annotations

from typing import Final

PROJECT_ID: Final[str] = "genome-services-platform"
RAW_DATASET: Final[str] = "arab_acmg_raw"
HARMONIZED_DATASET: Final[str] = "arab_acmg_harmonized"

FINAL_REGISTRY_TABLE: Final[str] = "supervisor_variant_registry_brca_v1"
PRE_GME_REGISTRY_TABLE: Final[str] = "supervisor_variant_registry_brca_pre_gme_v1"
REGISTRY_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.{FINAL_REGISTRY_TABLE}"
PRE_GME_REGISTRY_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.{PRE_GME_REGISTRY_TABLE}"

GENE_WINDOWS_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.h_brca_gene_windows"
CLINVAR_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.h_brca_clinvar_variants"
GNOMAD_GENOMES_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.h_brca_gnomad_genomes_variants"
GNOMAD_EXOMES_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.h_brca_gnomad_exomes_variants"
GME_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.h_brca_gme_variants"

CLINVAR_RAW_TABLE_REF: Final[str] = f"{PROJECT_ID}.{RAW_DATASET}.clinvar_raw_vcf"
GNOMAD_GENOMES_CHR13_RAW_TABLE_REF: Final[str] = f"{PROJECT_ID}.{RAW_DATASET}.gnomad_v4_1_genomes_chr13_raw"
GNOMAD_GENOMES_CHR17_RAW_TABLE_REF: Final[str] = f"{PROJECT_ID}.{RAW_DATASET}.gnomad_v4_1_genomes_chr17_raw"
GNOMAD_EXOMES_CHR13_RAW_TABLE_REF: Final[str] = f"{PROJECT_ID}.{RAW_DATASET}.gnomad_v4_1_exomes_chr13_raw"
GNOMAD_EXOMES_CHR17_RAW_TABLE_REF: Final[str] = f"{PROJECT_ID}.{RAW_DATASET}.gnomad_v4_1_exomes_chr17_raw"
GME_RAW_TABLE_REF: Final[str] = f"{PROJECT_ID}.{RAW_DATASET}.gme_hg38_raw"

RAW_TABLE_REFS: Final[dict[str, str]] = {
    "clinvar_raw_vcf": CLINVAR_RAW_TABLE_REF,
    "gnomad_v4_1_genomes_chr13_raw": GNOMAD_GENOMES_CHR13_RAW_TABLE_REF,
    "gnomad_v4_1_genomes_chr17_raw": GNOMAD_GENOMES_CHR17_RAW_TABLE_REF,
    "gnomad_v4_1_exomes_chr13_raw": GNOMAD_EXOMES_CHR13_RAW_TABLE_REF,
    "gnomad_v4_1_exomes_chr17_raw": GNOMAD_EXOMES_CHR17_RAW_TABLE_REF,
    "gme_hg38_raw": GME_RAW_TABLE_REF,
}

HARMONIZED_TABLE_REFS: Final[dict[str, str]] = {
    "h_brca_gene_windows": GENE_WINDOWS_TABLE_REF,
    "h_brca_clinvar_variants": CLINVAR_TABLE_REF,
    "h_brca_gnomad_genomes_variants": GNOMAD_GENOMES_TABLE_REF,
    "h_brca_gnomad_exomes_variants": GNOMAD_EXOMES_TABLE_REF,
    "h_brca_gme_variants": GME_TABLE_REF,
}

PRE_GME_COMMON_REGISTRY_CTES: Final[str] = f"""
all_keys AS (
  SELECT gene_symbol, variant_key, chrom38 AS chrom, pos38 AS pos, ref_norm AS ref, alt_norm AS alt
  FROM `{CLINVAR_TABLE_REF}`
  UNION DISTINCT
  SELECT gene_symbol, variant_key, chrom38 AS chrom, pos38 AS pos, ref_norm AS ref, alt_norm AS alt
  FROM `{GNOMAD_GENOMES_TABLE_REF}`
  UNION DISTINCT
  SELECT gene_symbol, variant_key, chrom38 AS chrom, pos38 AS pos, ref_norm AS ref, alt_norm AS alt
  FROM `{GNOMAD_EXOMES_TABLE_REF}`
)
"""

PRE_GME_REGISTRY_SELECT: Final[str] = f"""
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
  IF(clinvar.variant_key IS NOT NULL, 1, 0)
    + IF(genomes.variant_key IS NOT NULL, 1, 0)
    + IF(exomes.variant_key IS NOT NULL, 1, 0) AS source_count,
  CURRENT_DATE() AS last_refresh_date
FROM all_keys
LEFT JOIN `{CLINVAR_TABLE_REF}` AS clinvar USING (gene_symbol, variant_key)
LEFT JOIN `{GNOMAD_GENOMES_TABLE_REF}` AS genomes USING (gene_symbol, variant_key)
LEFT JOIN `{GNOMAD_EXOMES_TABLE_REF}` AS exomes USING (gene_symbol, variant_key)
"""

FINAL_COMMON_REGISTRY_CTES: Final[str] = f"""
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


def build_pre_gme_registry_sql() -> str:
    return (
        f"CREATE OR REPLACE TABLE `{PRE_GME_REGISTRY_TABLE_REF}` AS\n"
        f"WITH\n{PRE_GME_COMMON_REGISTRY_CTES}\n"
        f"{PRE_GME_REGISTRY_SELECT}\n"
        "ORDER BY gene_symbol, chrom, pos, ref, alt"
    )


def build_final_registry_sql() -> str:
    return (
        f"CREATE OR REPLACE TABLE `{REGISTRY_TABLE_REF}` AS\n"
        f"WITH\n{FINAL_COMMON_REGISTRY_CTES}\n"
        f"{FINAL_REGISTRY_SELECT}\n"
        "ORDER BY gene_symbol, chrom, pos, ref, alt"
    )


def build_registry_sql() -> str:
    return build_final_registry_sql()


def build_raw_sample_sql(table_ref: str, sample_percent: float, limit: int = 10) -> str:
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


def build_pre_gme_sample_sql(limit: int = 10) -> str:
    return f"""
WITH numbered AS (
  SELECT ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number, *
  FROM `{PRE_GME_REGISTRY_TABLE_REF}`
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


def build_pre_gme_export_sql(limit: int | None = None) -> str:
    sql = f"""
SELECT
  chrom,
  pos,
  pos + LENGTH(ref) - 1 AS end_pos,
  variant_key AS export_id,
  variant_key,
  ref,
  alt,
  gene_symbol,
  present_in_clinvar,
  present_in_gnomad_genomes,
  present_in_gnomad_exomes,
  clinvar_ids,
  clinvar_significance_values,
  clinvar_review_status_values,
  clinvar_record_count,
  clinvar_gene_info_match_count,
  clinvar_gene_info_mismatch_count,
  gnomad_genomes_ac,
  gnomad_genomes_an,
  gnomad_genomes_af,
  gnomad_genomes_grpmax,
  gnomad_genomes_grpmax_faf95,
  gnomad_genomes_depth,
  gnomad_genomes_ac_afr,
  gnomad_genomes_af_afr,
  gnomad_genomes_ac_eur_proxy,
  gnomad_genomes_af_eur_proxy,
  gnomad_exomes_ac,
  gnomad_exomes_an,
  gnomad_exomes_af,
  gnomad_exomes_grpmax,
  gnomad_exomes_grpmax_faf95,
  gnomad_exomes_depth,
  gnomad_exomes_ac_afr,
  gnomad_exomes_af_afr,
  gnomad_exomes_ac_eur_proxy,
  gnomad_exomes_af_eur_proxy,
  source_count,
  last_refresh_date
FROM `{PRE_GME_REGISTRY_TABLE_REF}`
ORDER BY gene_symbol, chrom, pos, ref, alt
""".strip()
    if limit is None:
        return sql
    return f"{sql}\nLIMIT {limit}"


def build_registry_step_sql(step_id: str, limit: int = 10) -> str:
    if step_id == "gene_windows":
        return build_sample_sql(GENE_WINDOWS_TABLE_REF, sample_percent=100.0, limit=limit)
    if step_id == "clinvar_brca":
        return build_sample_sql(CLINVAR_TABLE_REF, sample_percent=15.0, limit=limit)
    if step_id == "gnomad_genomes_brca":
        return build_sample_sql(GNOMAD_GENOMES_TABLE_REF, sample_percent=8.0, limit=limit)
    if step_id == "gnomad_exomes_brca":
        return build_sample_sql(GNOMAD_EXOMES_TABLE_REF, sample_percent=12.0, limit=limit)
    if step_id == "pre_gme_registry":
        return build_pre_gme_sample_sql(limit=limit)
    if step_id == "gme_brca":
        return build_sample_sql(GME_TABLE_REF, sample_percent=100.0, limit=limit)
    if step_id == "registry_final":
        return build_registry_sample_sql(limit=limit)
    raise KeyError(step_id)
