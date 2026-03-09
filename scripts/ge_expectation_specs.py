"""Great Expectations query and suite specifications for T002 raw-layer QC.

This module is intentionally dependency-light so tests can verify the SQL and
expectation payloads without importing the Great Expectations runtime.
"""

from __future__ import annotations

from typing import Final

PROJECT_ID: Final[str] = "genome-services-platform"
RAW_DATASET: Final[str] = "arab_acmg_raw"
HARMONIZED_DATASET: Final[str] = "arab_acmg_harmonized"
CLINVAR_TABLE: Final[str] = f"{PROJECT_ID}.{RAW_DATASET}.clinvar_raw_vcf"
GNOMAD_TABLES: Final[dict[str, str]] = {
    "gnomad_v4_1_genomes_chr13_raw": f"{PROJECT_ID}.{RAW_DATASET}.gnomad_v4_1_genomes_chr13_raw",
    "gnomad_v4_1_genomes_chr17_raw": f"{PROJECT_ID}.{RAW_DATASET}.gnomad_v4_1_genomes_chr17_raw",
    "gnomad_v4_1_exomes_chr13_raw": f"{PROJECT_ID}.{RAW_DATASET}.gnomad_v4_1_exomes_chr13_raw",
    "gnomad_v4_1_exomes_chr17_raw": f"{PROJECT_ID}.{RAW_DATASET}.gnomad_v4_1_exomes_chr17_raw",
}
GNOMAD_GE_TABLES: Final[dict[str, str]] = {
    "stg_gnomad_genomes_variants": f"{PROJECT_ID}.{HARMONIZED_DATASET}.stg_gnomad_genomes_variants",
    "stg_gnomad_exomes_variants": f"{PROJECT_ID}.{HARMONIZED_DATASET}.stg_gnomad_exomes_variants",
}


CLINVAR_EXPECTATION_SUITE_NAME: Final[str] = "clinvar_raw_suite"
GNOMAD_EXPECTATION_SUITE_NAME: Final[str] = "gnomad_raw_suite"
CLINVAR_CHECKPOINT_NAME: Final[str] = "clinvar_raw_checkpoint"
GNOMAD_CHECKPOINT_NAME: Final[str] = "gnomad_raw_checkpoint"
RAW_DATASOURCE_NAME: Final[str] = "arab_acmg_raw_bigquery"
HARMONIZED_DATASOURCE_NAME: Final[str] = "arab_acmg_harmonized_bigquery"


def build_clinvar_raw_query(table_ref: str = CLINVAR_TABLE) -> str:
    return f"""
SELECT
  chrom,
  pos,
  id,
  ref,
  alt,
  qual,
  filter,
  info
FROM `{table_ref}`
""".strip()


def build_gnomad_raw_query(table_ref: str) -> str:
    return f"""
SELECT *
FROM (
WITH expanded AS (
  SELECT
    chrom,
    pos,
    ref,
    alt_value AS alt,
    alt_offset,
    info
  FROM `{table_ref}`,
  UNNEST(SPLIT(alt, ',')) AS alt_value WITH OFFSET AS alt_offset
),
parsed AS (
  SELECT
    chrom,
    pos,
    ref,
    alt,
    SAFE_CAST(SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC=([^;]+)'), ''), ',')[SAFE_OFFSET(alt_offset)] AS INT64) AS ac,
    SAFE_CAST(REGEXP_EXTRACT(info, r'(?:^|;)AN=([^;]+)') AS INT64) AS an,
    COALESCE(
      SAFE_CAST(SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AF=([^;]+)'), ''), ',')[SAFE_OFFSET(alt_offset)] AS FLOAT64),
      SAFE_DIVIDE(
        SAFE_CAST(SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC=([^;]+)'), ''), ',')[SAFE_OFFSET(alt_offset)] AS INT64),
        NULLIF(SAFE_CAST(REGEXP_EXTRACT(info, r'(?:^|;)AN=([^;]+)') AS INT64), 0)
      )
    ) AS af,
    SAFE_CAST(SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC_afr=([^;]+)'), ''), ',')[SAFE_OFFSET(alt_offset)] AS INT64) AS ac_afr,
    COALESCE(
      SAFE_CAST(SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AF_afr=([^;]+)'), ''), ',')[SAFE_OFFSET(alt_offset)] AS FLOAT64),
      SAFE_DIVIDE(
        SAFE_CAST(SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC_afr=([^;]+)'), ''), ',')[SAFE_OFFSET(alt_offset)] AS INT64),
        NULLIF(SAFE_CAST(REGEXP_EXTRACT(info, r'(?:^|;)AN_afr=([^;]+)') AS INT64), 0)
      )
    ) AS af_afr,
    SAFE_CAST(REGEXP_EXTRACT(info, r'(?:^|;)faf95=([^;]+)') AS FLOAT64) AS grpmax_faf95,
    COALESCE(SAFE_CAST(SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC_nfe=([^;]+)'), ''), ',')[SAFE_OFFSET(alt_offset)] AS INT64), 0)
      + COALESCE(SAFE_CAST(SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC_fin=([^;]+)'), ''), ',')[SAFE_OFFSET(alt_offset)] AS INT64), 0)
      + COALESCE(SAFE_CAST(SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC_asj=([^;]+)'), ''), ',')[SAFE_OFFSET(alt_offset)] AS INT64), 0) AS ac_eur_proxy,
    SAFE_DIVIDE(
      COALESCE(SAFE_CAST(SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC_nfe=([^;]+)'), ''), ',')[SAFE_OFFSET(alt_offset)] AS INT64), 0)
        + COALESCE(SAFE_CAST(SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC_fin=([^;]+)'), ''), ',')[SAFE_OFFSET(alt_offset)] AS INT64), 0)
        + COALESCE(SAFE_CAST(SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;)AC_asj=([^;]+)'), ''), ',')[SAFE_OFFSET(alt_offset)] AS INT64), 0),
      NULLIF(
        COALESCE(SAFE_CAST(REGEXP_EXTRACT(info, r'(?:^|;)AN_nfe=([^;]+)') AS INT64), 0)
          + COALESCE(SAFE_CAST(REGEXP_EXTRACT(info, r'(?:^|;)AN_fin=([^;]+)') AS INT64), 0)
          + COALESCE(SAFE_CAST(REGEXP_EXTRACT(info, r'(?:^|;)AN_asj=([^;]+)') AS INT64), 0),
        0
      )
    ) AS af_eur_proxy,
    COUNT(*) OVER (PARTITION BY chrom, pos, ref, alt) AS allele_duplicate_count,
    info
  FROM expanded
  WHERE alt IS NOT NULL
    AND alt != ''
)
SELECT *
FROM parsed
WHERE COALESCE(an, 0) > 0
) AS gnomad_qc
""".strip()


def clinvar_raw_expectations() -> list[dict]:
    return [
        {
            "type": "expect_table_columns_to_match_ordered_list",
            "kwargs": {"column_list": ["chrom", "pos", "id", "ref", "alt", "qual", "filter", "info"]},
        },
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "chrom"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "pos"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ref"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "alt"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "info"}},
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "pos", "min_value": 1},
        },
        {
            "type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 1000000},
        },
    ]


def gnomad_raw_expectations() -> list[dict]:
    return [
        {"type": "expect_column_to_exist", "kwargs": {"column": "record_key"}},
        {"type": "expect_column_to_exist", "kwargs": {"column": "chrom_raw"}},
        {"type": "expect_column_to_exist", "kwargs": {"column": "pos"}},
        {"type": "expect_column_to_exist", "kwargs": {"column": "ref"}},
        {"type": "expect_column_to_exist", "kwargs": {"column": "alt"}},
        {"type": "expect_column_to_exist", "kwargs": {"column": "ac"}},
        {"type": "expect_column_to_exist", "kwargs": {"column": "an"}},
        {"type": "expect_column_to_exist", "kwargs": {"column": "af"}},
        {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "record_key"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "chrom_raw"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "pos"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ref"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "alt"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ac"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "an"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "af"}},
        {
            "type": "expect_column_values_to_match_regex",
            "kwargs": {"column": "chrom_raw", "regex": r"^chr(13|17)$"},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "pos", "min_value": 1},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "an", "min_value": 1},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "af", "min_value": 0, "max_value": 1},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "af_afr", "min_value": 0, "max_value": 1},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "af_eur_proxy", "min_value": 0, "max_value": 1},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "grpmax_faf95", "min_value": 0, "max_value": 1},
        },
        {
            "type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 1000000},
        },
    ]
