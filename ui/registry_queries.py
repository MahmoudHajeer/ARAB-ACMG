from __future__ import annotations

import csv
from pathlib import Path
from textwrap import dedent
from typing import Final

PROJECT_ID: Final[str] = "genome-services-platform"
RAW_DATASET: Final[str] = "arab_acmg_raw"
HARMONIZED_DATASET: Final[str] = "arab_acmg_harmonized"

FINAL_REGISTRY_TABLE: Final[str] = "supervisor_variant_registry_brca_v1"
PRE_GME_REGISTRY_TABLE: Final[str] = "supervisor_variant_registry_brca_pre_gme_v1"
REGISTRY_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.{FINAL_REGISTRY_TABLE}"
PRE_GME_REGISTRY_TABLE_REF: Final[str] = f"{PROJECT_ID}.{HARMONIZED_DATASET}.{PRE_GME_REGISTRY_TABLE}"

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

UI_ROOT: Final[Path] = Path(__file__).resolve().parent
REPO_ROOT: Final[Path] = UI_ROOT.parent
GENE_WINDOW_SEED_CANDIDATES: Final[tuple[Path, ...]] = (
    REPO_ROOT / "arab_acmg_dbt" / "seeds" / "brca_gene_windows_seed.csv",
    UI_ROOT / "brca_gene_windows_seed.csv",
)


def resolve_gene_window_seed() -> Path:
    for candidate in GENE_WINDOW_SEED_CANDIDATES:
        if candidate.exists():
            return candidate
    joined = ", ".join(str(candidate) for candidate in GENE_WINDOW_SEED_CANDIDATES)
    raise FileNotFoundError(f"BRCA gene-window seed was not found in any expected location: {joined}")


def _quote(text: str) -> str:
    return text.replace("'", "\\'")


def load_gene_windows() -> list[dict[str, str]]:
    with resolve_gene_window_seed().open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def gene_windows_payload() -> list[dict[str, str]]:
    return [
        {
            "gene_symbol": row["gene_symbol"],
            "chrom38": row["chrom38"],
            "start_pos38": row["start_pos38"],
            "end_pos38": row["end_pos38"],
            "coordinate_source": row["coordinate_source"],
            "coordinate_source_url": row["coordinate_source_url"],
            "accessed_at": row["accessed_at"],
        }
        for row in load_gene_windows()
    ]


def build_gene_windows_cte() -> str:
    selects = []
    for row in load_gene_windows():
        selects.append(
            "SELECT "
            f"'{_quote(row['gene_symbol'])}' AS gene_symbol, "
            f"'{_quote(row['ensembl_gene_id'])}' AS ensembl_gene_id, "
            f"'{_quote(row['chrom38'])}' AS chrom38, "
            f"'{_quote(row['chrom_nochr'])}' AS chrom_nochr, "
            f"{int(row['start_pos38'])} AS start_pos38, "
            f"{int(row['end_pos38'])} AS end_pos38, "
            f"'{_quote(row['coordinate_source'])}' AS coordinate_source, "
            f"'{_quote(row['coordinate_source_url'])}' AS coordinate_source_url, "
            f"DATE '{row['accessed_at']}' AS accessed_at"
        )
    return "gene_windows AS (\n  " + "\n  UNION ALL\n  ".join(selects) + "\n)"


def _info_scalar(key: str) -> str:
    return f"REGEXP_EXTRACT(info, r'(?:^|;){key}=([^;]+)')"


def _info_token(key: str, alt_offset: str = 'alt_offset') -> str:
    return f"SPLIT(COALESCE(REGEXP_EXTRACT(info, r'(?:^|;){key}=([^;]+)'), ''), ',')[SAFE_OFFSET({alt_offset})]"


def _variant_type_expr(ref_expr: str, alt_expr: str) -> str:
    return dedent(
        f"""
        CASE
          WHEN LENGTH({ref_expr}) = 1 AND LENGTH({alt_expr}) = 1 THEN 'SNV'
          WHEN LENGTH({ref_expr}) = LENGTH({alt_expr}) AND LENGTH({ref_expr}) > 1 THEN 'MNV'
          WHEN LENGTH({ref_expr}) < LENGTH({alt_expr}) THEN 'INS'
          WHEN LENGTH({ref_expr}) > LENGTH({alt_expr}) THEN 'DEL'
          ELSE 'COMPLEX'
        END
        """
    ).strip()


def _clinvar_ctes() -> str:
    return dedent(
        f"""
        clinvar_source_rows AS (
          SELECT
            gene_windows.gene_symbol,
            gene_windows.chrom38 AS chrom,
            raw.pos AS pos,
            raw.ref AS ref,
            alt_value AS alt,
            CONCAT(gene_windows.chrom38, ':', CAST(raw.pos AS STRING), ':', raw.ref, ':', alt_value) AS variant_key,
            {_info_scalar('ALLELEID')} AS alleleid,
            {_info_scalar('CLNSIG')} AS clnsig,
            {_info_scalar('CLNREVSTAT')} AS clnrevstat,
            REGEXP_EXTRACT(info, r'(?:^|;)MC=[^|]+\\|([^;]+)') AS effect
          FROM `{CLINVAR_RAW_TABLE_REF}` AS raw
          JOIN gene_windows
            ON CONCAT('chr', REGEXP_REPLACE(raw.chrom, r'^chr', '')) = gene_windows.chrom38
           AND raw.pos BETWEEN gene_windows.start_pos38 AND gene_windows.end_pos38
          CROSS JOIN UNNEST(SPLIT(raw.alt, ',')) AS alt_value WITH OFFSET AS alt_offset
          WHERE raw.alt IS NOT NULL
            AND raw.alt != ''
            AND alt_value IS NOT NULL
            AND alt_value != ''
        ),
        clinvar_agg AS (
          SELECT
            gene_symbol,
            variant_key,
            ANY_VALUE(chrom) AS chrom,
            ANY_VALUE(pos) AS pos,
            ANY_VALUE(ref) AS ref,
            ANY_VALUE(alt) AS alt,
            STRING_AGG(DISTINCT alleleid, ' | ' ORDER BY alleleid) AS alleleid,
            STRING_AGG(DISTINCT clnsig, ' | ' ORDER BY clnsig) AS clnsig,
            STRING_AGG(DISTINCT clnrevstat, ' | ' ORDER BY clnrevstat) AS clnrevstat,
            STRING_AGG(DISTINCT effect, ' | ' ORDER BY effect) AS effect
          FROM clinvar_source_rows
          GROUP BY gene_symbol, variant_key
        )
        """
    ).strip()


def _gnomad_union_cte(name: str, refs: tuple[str, ...]) -> str:
    parts = [f"SELECT * FROM `{ref}`" for ref in refs]
    return f"{name} AS (\n  " + "\n  UNION ALL\n  ".join(parts) + "\n)"


def _gnomad_ctes(raw_cte_name: str, agg_name: str) -> str:
    return dedent(
        f"""
        {agg_name}_source_rows AS (
          SELECT
            gene_windows.gene_symbol,
            gene_windows.chrom38 AS chrom,
            raw.pos AS pos,
            raw.ref AS ref,
            alt_value AS alt,
            CONCAT(gene_windows.chrom38, ':', CAST(raw.pos AS STRING), ':', raw.ref, ':', alt_value) AS variant_key,
            SAFE_CAST({_info_token('AC')} AS INT64) AS ac,
            SAFE_CAST({_info_scalar('AN')} AS INT64) AS an,
            COALESCE(
              SAFE_CAST({_info_token('AF')} AS FLOAT64),
              SAFE_DIVIDE(SAFE_CAST({_info_token('AC')} AS INT64), NULLIF(SAFE_CAST({_info_scalar('AN')} AS INT64), 0))
            ) AS af,
            SAFE_CAST({_info_scalar('nhomalt')} AS INT64) AS hom,
            SAFE_CAST({_info_token('AC_mid')} AS INT64) AS ac_mid,
            SAFE_CAST({_info_scalar('AN_mid')} AS INT64) AS an_mid,
            COALESCE(
              SAFE_CAST({_info_token('AF_mid')} AS FLOAT64),
              SAFE_DIVIDE(SAFE_CAST({_info_token('AC_mid')} AS INT64), NULLIF(SAFE_CAST({_info_scalar('AN_mid')} AS INT64), 0))
            ) AS af_mid,
            SAFE_CAST({_info_scalar('nhomalt_mid')} AS INT64) AS hom_mid,
            SAFE_CAST({_info_token('AC_afr')} AS INT64) AS ac_afr,
            SAFE_CAST({_info_scalar('AN_afr')} AS INT64) AS an_afr,
            COALESCE(
              SAFE_CAST({_info_token('AF_afr')} AS FLOAT64),
              SAFE_DIVIDE(SAFE_CAST({_info_token('AC_afr')} AS INT64), NULLIF(SAFE_CAST({_info_scalar('AN_afr')} AS INT64), 0))
            ) AS af_afr,
            SAFE_CAST({_info_token('AC_nfe')} AS INT64) AS ac_nfe,
            SAFE_CAST({_info_scalar('AN_nfe')} AS INT64) AS an_nfe,
            SAFE_CAST({_info_token('AF_nfe')} AS FLOAT64) AS af_nfe,
            SAFE_CAST({_info_token('AC_fin')} AS INT64) AS ac_fin,
            SAFE_CAST({_info_scalar('AN_fin')} AS INT64) AS an_fin,
            SAFE_CAST({_info_token('AF_fin')} AS FLOAT64) AS af_fin,
            SAFE_CAST({_info_token('AC_asj')} AS INT64) AS ac_asj,
            SAFE_CAST({_info_scalar('AN_asj')} AS INT64) AS an_asj,
            SAFE_CAST({_info_token('AF_asj')} AS FLOAT64) AS af_asj,
            SAFE_CAST({_info_scalar('faf95')} AS FLOAT64) AS grpmax_faf95,
            SAFE_CAST({_info_scalar('VarDP')} AS FLOAT64) AS depth
          FROM {raw_cte_name} AS raw
          JOIN gene_windows
            ON CONCAT('chr', REGEXP_REPLACE(raw.chrom, r'^chr', '')) = gene_windows.chrom38
           AND raw.pos BETWEEN gene_windows.start_pos38 AND gene_windows.end_pos38
          CROSS JOIN UNNEST(SPLIT(raw.alt, ',')) AS alt_value WITH OFFSET AS alt_offset
          WHERE raw.alt IS NOT NULL
            AND raw.alt != ''
            AND alt_value IS NOT NULL
            AND alt_value != ''
        ),
        {agg_name} AS (
          SELECT
            gene_symbol,
            variant_key,
            ANY_VALUE(chrom) AS chrom,
            ANY_VALUE(pos) AS pos,
            ANY_VALUE(ref) AS ref,
            ANY_VALUE(alt) AS alt,
            SUM(ac) AS ac,
            SUM(an) AS an,
            SAFE_DIVIDE(SUM(ac), NULLIF(SUM(an), 0)) AS af,
            SUM(hom) AS hom,
            SUM(ac_mid) AS ac_mid,
            SUM(an_mid) AS an_mid,
            SAFE_DIVIDE(SUM(ac_mid), NULLIF(SUM(an_mid), 0)) AS af_mid,
            SUM(hom_mid) AS hom_mid,
            SUM(ac_afr) AS ac_afr,
            SUM(an_afr) AS an_afr,
            SAFE_DIVIDE(SUM(ac_afr), NULLIF(SUM(an_afr), 0)) AS af_afr,
            SUM(COALESCE(ac_nfe, 0) + COALESCE(ac_fin, 0) + COALESCE(ac_asj, 0)) AS ac_eur_proxy,
            SUM(COALESCE(an_nfe, 0) + COALESCE(an_fin, 0) + COALESCE(an_asj, 0)) AS an_eur_proxy,
            SAFE_DIVIDE(
              SUM(COALESCE(ac_nfe, 0) + COALESCE(ac_fin, 0) + COALESCE(ac_asj, 0)),
              NULLIF(SUM(COALESCE(an_nfe, 0) + COALESCE(an_fin, 0) + COALESCE(an_asj, 0)), 0)
            ) AS af_eur_proxy,
            MAX(grpmax_faf95) AS grpmax_faf95,
            MAX(depth) AS depth
          FROM {agg_name}_source_rows
          GROUP BY gene_symbol, variant_key
        )
        """
    ).strip()


def _gme_ctes() -> str:
    return dedent(
        f"""
        gme_source_rows AS (
          SELECT
            gene_windows.gene_symbol,
            gene_windows.chrom38 AS chrom,
            raw.start AS pos,
            raw.ref AS ref,
            raw.alt AS alt,
            CONCAT(gene_windows.chrom38, ':', CAST(raw.start AS STRING), ':', raw.ref, ':', raw.alt) AS variant_key,
            raw.gme_af,
            raw.gme_nwa,
            raw.gme_nea,
            raw.gme_ap,
            raw.gme_israel,
            raw.gme_sd,
            raw.gme_tp,
            raw.gme_ca
          FROM `{GME_RAW_TABLE_REF}` AS raw
          JOIN gene_windows
            ON CONCAT('chr', REGEXP_REPLACE(raw.chrom, r'^chr', '')) = gene_windows.chrom38
           AND raw.start BETWEEN gene_windows.start_pos38 AND gene_windows.end_pos38
        ),
        gme_agg AS (
          SELECT
            gene_symbol,
            variant_key,
            ANY_VALUE(chrom) AS chrom,
            ANY_VALUE(pos) AS pos,
            ANY_VALUE(ref) AS ref,
            ANY_VALUE(alt) AS alt,
            MAX(gme_af) AS gme_af,
            MAX(gme_nwa) AS gme_nwa,
            MAX(gme_nea) AS gme_nea,
            MAX(gme_ap) AS gme_ap,
            MAX(gme_israel) AS gme_israel,
            MAX(gme_sd) AS gme_sd,
            MAX(gme_tp) AS gme_tp,
            MAX(gme_ca) AS gme_ca
          FROM gme_source_rows
          GROUP BY gene_symbol, variant_key
        )
        """
    ).strip()


def _all_keys_cte(include_gme: bool) -> str:
    unions = [
        "SELECT gene_symbol, variant_key, chrom, pos, ref, alt FROM clinvar_agg",
        "SELECT gene_symbol, variant_key, chrom, pos, ref, alt FROM gnomad_genomes_agg",
        "SELECT gene_symbol, variant_key, chrom, pos, ref, alt FROM gnomad_exomes_agg",
    ]
    if include_gme:
        unions.append("SELECT gene_symbol, variant_key, chrom, pos, ref, alt FROM gme_agg")
    return "all_keys AS (\n  " + "\n  UNION DISTINCT\n  ".join(unions) + "\n)"


def _base_required_columns() -> str:
    return dedent(
        """
        all_keys.chrom AS `CHROM`,
          all_keys.pos AS `POS`,
          all_keys.pos + LENGTH(all_keys.ref) - 1 AS `END`,
          all_keys.variant_key AS `ID`,
          all_keys.ref AS `REF`,
          all_keys.alt AS `ALT`,
          {variant_type} AS `VARTYPE`,
          CAST(NULL AS STRING) AS `Repeat`,
          CAST(NULL AS STRING) AS `Segdup`,
          CAST(NULL AS STRING) AS `Blacklist`,
          all_keys.gene_symbol AS `GENE`,
          clinvar.effect AS `EFFECT`,
          CAST(NULL AS STRING) AS `HGVS_C`,
          CAST(NULL AS STRING) AS `HGVS_P`,
          CAST(NULL AS STRING) AS `PHENOTYPES_OMIM`,
          CAST(NULL AS STRING) AS `PHENOTYPES_OMIM_ID`,
          CAST(NULL AS STRING) AS `INHERITANCE_PATTERN`,
          clinvar.alleleid AS `ALLELEID`,
          clinvar.clnsig AS `CLNSIG`,
          CAST(NULL AS FLOAT64) AS `TOPMED_AF`,
          CAST(NULL AS INT64) AS `TOPMed_Hom`,
          CAST(NULL AS FLOAT64) AS `ALFA_AF`,
          CAST(NULL AS INT64) AS `ALFA_Hom`,
          SAFE_DIVIDE(COALESCE(gnomad_genomes.ac, 0) + COALESCE(gnomad_exomes.ac, 0), NULLIF(COALESCE(gnomad_genomes.an, 0) + COALESCE(gnomad_exomes.an, 0), 0)) AS `GNOMAD_ALL_AF`,
          CASE
            WHEN gnomad_genomes.hom IS NULL AND gnomad_exomes.hom IS NULL THEN NULL
            ELSE COALESCE(gnomad_genomes.hom, 0) + COALESCE(gnomad_exomes.hom, 0)
          END AS `gnomAD_all_Hom`,
          SAFE_DIVIDE(COALESCE(gnomad_genomes.ac_mid, 0) + COALESCE(gnomad_exomes.ac_mid, 0), NULLIF(COALESCE(gnomad_genomes.an_mid, 0) + COALESCE(gnomad_exomes.an_mid, 0), 0)) AS `GNOMAD_MID_AF`,
          CASE
            WHEN gnomad_genomes.hom_mid IS NULL AND gnomad_exomes.hom_mid IS NULL THEN NULL
            ELSE COALESCE(gnomad_genomes.hom_mid, 0) + COALESCE(gnomad_exomes.hom_mid, 0)
          END AS `gnomAD_mid_Hom`,
          CAST(NULL AS FLOAT64) AS `ONEKGP_AF`,
          CAST(NULL AS FLOAT64) AS `REGENERON_AF`,
          CAST(NULL AS FLOAT64) AS `TGP_AF`,
          CAST(NULL AS FLOAT64) AS `QATARI`,
          CAST(NULL AS FLOAT64) AS `JGP_AF`,
          CAST(NULL AS FLOAT64) AS `JGP_MAF`,
          CAST(NULL AS INT64) AS `JGP_Hom`,
          CAST(NULL AS INT64) AS `JGP_Het`,
          CAST(NULL AS INT64) AS `JGP_AC_Hemi`,
          CAST(NULL AS STRING) AS `SIFT_PRED`,
          CAST(NULL AS STRING) AS `POLYPHEN2_HDIV_PRED`,
          CAST(NULL AS STRING) AS `POLYPHEN2_HVAR_PRED`,
          CAST(NULL AS STRING) AS `PROVEAN_PRE`,
          clinvar.clnrevstat AS `CLNREVSTAT`,
          gnomad_genomes.ac AS `GNOMAD_GENOMES_AC`,
          gnomad_genomes.an AS `GNOMAD_GENOMES_AN`,
          gnomad_genomes.af AS `GNOMAD_GENOMES_AF`,
          gnomad_genomes.hom AS `GNOMAD_GENOMES_HOM`,
          gnomad_genomes.af_afr AS `GNOMAD_GENOMES_AF_AFR`,
          gnomad_genomes.af_eur_proxy AS `GNOMAD_GENOMES_AF_EUR_PROXY`,
          gnomad_exomes.ac AS `GNOMAD_EXOMES_AC`,
          gnomad_exomes.an AS `GNOMAD_EXOMES_AN`,
          gnomad_exomes.af AS `GNOMAD_EXOMES_AF`,
          gnomad_exomes.hom AS `GNOMAD_EXOMES_HOM`,
          gnomad_exomes.af_afr AS `GNOMAD_EXOMES_AF_AFR`,
          gnomad_exomes.af_eur_proxy AS `GNOMAD_EXOMES_AF_EUR_PROXY`,
          gnomad_genomes.depth AS `GNOMAD_GENOMES_DEPTH`,
          gnomad_exomes.depth AS `GNOMAD_EXOMES_DEPTH`,
          IF(clinvar.variant_key IS NOT NULL, 1, 0)
            + IF(gnomad_genomes.variant_key IS NOT NULL, 1, 0)
            + IF(gnomad_exomes.variant_key IS NOT NULL, 1, 0) AS `SOURCE_COUNT`
        """
    ).format(variant_type=_variant_type_expr('all_keys.ref', 'all_keys.alt')).strip()


def _base_from_clause(include_gme: bool) -> str:
    joins = dedent(
        """
        FROM all_keys
        LEFT JOIN clinvar_agg AS clinvar USING (gene_symbol, variant_key)
        LEFT JOIN gnomad_genomes_agg AS gnomad_genomes USING (gene_symbol, variant_key)
        LEFT JOIN gnomad_exomes_agg AS gnomad_exomes USING (gene_symbol, variant_key)
        """
    ).strip()
    if include_gme:
        joins += "\nLEFT JOIN gme_agg AS gme USING (gene_symbol, variant_key)"
    return joins


def _pre_gme_select() -> str:
    return "SELECT\n  " + _base_required_columns() + ",\n  'PRE_GME_REVIEW' AS `PIPELINE_STAGE`\n" + _base_from_clause(include_gme=False)


def _final_select() -> str:
    return "SELECT\n  " + _base_required_columns() + dedent(
        """
        ,
          'FINAL_WITH_GME' AS `PIPELINE_STAGE`,
          gme.gme_af AS `GME_AF`,
          gme.gme_nwa AS `GME_NWA`,
          gme.gme_nea AS `GME_NEA`,
          gme.gme_ap AS `GME_AP`,
          gme.gme_israel AS `GME_ISRAEL`,
          gme.gme_sd AS `GME_SD`,
          gme.gme_tp AS `GME_TP`,
          gme.gme_ca AS `GME_CA`
        """
    ).strip() + "\n" + _base_from_clause(include_gme=True)


def _common_ctes(include_gme: bool) -> str:
    ctes = [
        build_gene_windows_cte(),
        _clinvar_ctes(),
        _gnomad_union_cte('gnomad_genomes_raw', (GNOMAD_GENOMES_CHR13_RAW_TABLE_REF, GNOMAD_GENOMES_CHR17_RAW_TABLE_REF)),
        _gnomad_ctes('gnomad_genomes_raw', 'gnomad_genomes_agg'),
        _gnomad_union_cte('gnomad_exomes_raw', (GNOMAD_EXOMES_CHR13_RAW_TABLE_REF, GNOMAD_EXOMES_CHR17_RAW_TABLE_REF)),
        _gnomad_ctes('gnomad_exomes_raw', 'gnomad_exomes_agg'),
    ]
    if include_gme:
        ctes.append(_gme_ctes())
    ctes.append(_all_keys_cte(include_gme))
    return "WITH\n" + ",\n".join(ctes)


def build_pre_gme_registry_sql() -> str:
    return dedent(
        f"""
        CREATE OR REPLACE TABLE `{PRE_GME_REGISTRY_TABLE_REF}` AS
        {_common_ctes(include_gme=False)}
        {_pre_gme_select()}
        ORDER BY `GENE`, `CHROM`, `POS`, `REF`, `ALT`
        """
    ).strip()


def build_final_registry_sql() -> str:
    return dedent(
        f"""
        CREATE OR REPLACE TABLE `{REGISTRY_TABLE_REF}` AS
        {_common_ctes(include_gme=True)}
        {_final_select()}
        ORDER BY `GENE`, `CHROM`, `POS`, `REF`, `ALT`
        """
    ).strip()


def build_registry_sql() -> str:
    return build_final_registry_sql()


def build_raw_sample_sql(table_ref: str, sample_percent: float, limit: int = 10) -> str:
    return dedent(
        f"""
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
        """
    ).strip()


def build_export_sql(table_ref: str) -> str:
    return dedent(
        f"""
        SELECT *
        FROM `{table_ref}`
        """
    ).strip()


def build_sample_sql(table_ref: str, sample_percent: float, limit: int = 10) -> str:
    return dedent(
        f"""
        WITH numbered AS (
          SELECT ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number, *
          FROM `{table_ref}`
        )
        SELECT *
        FROM numbered
        WHERE sample_row_number <= {limit}
        ORDER BY sample_row_number
        """
    ).strip()


def build_pre_gme_sample_sql(limit: int = 10) -> str:
    return build_sample_sql(PRE_GME_REGISTRY_TABLE_REF, sample_percent=100.0, limit=limit)


def build_registry_sample_sql(sample_percent: float = 12.0, limit: int = 10) -> str:
    return build_sample_sql(REGISTRY_TABLE_REF, sample_percent=sample_percent, limit=limit)


def build_pre_gme_export_sql(limit: int | None = None) -> str:
    suffix = f"\nLIMIT {limit}" if limit is not None else ""
    return dedent(
        f"""
        SELECT *
        FROM `{PRE_GME_REGISTRY_TABLE_REF}`
        ORDER BY `GENE`, `CHROM`, `POS`, `REF`, `ALT`{suffix}
        """
    ).strip()


def build_registry_export_sql(limit: int | None = None) -> str:
    suffix = f"\nLIMIT {limit}" if limit is not None else ""
    return dedent(
        f"""
        SELECT *
        FROM `{REGISTRY_TABLE_REF}`
        ORDER BY `GENE`, `CHROM`, `POS`, `REF`, `ALT`{suffix}
        """
    ).strip()


def build_pre_gme_source_count_sql() -> str:
    return dedent(
        f"""
        {_common_ctes(include_gme=False)}
        SELECT 'clinvar' AS source_name, COUNT(*) AS row_count FROM clinvar_agg
        UNION ALL
        SELECT 'gnomad_genomes', COUNT(*) FROM gnomad_genomes_agg
        UNION ALL
        SELECT 'gnomad_exomes', COUNT(*) FROM gnomad_exomes_agg
        ORDER BY source_name
        """
    ).strip()


def build_final_source_count_sql() -> str:
    return dedent(
        f"""
        {_common_ctes(include_gme=True)}
        SELECT 'clinvar' AS source_name, COUNT(*) AS row_count FROM clinvar_agg
        UNION ALL
        SELECT 'gnomad_genomes', COUNT(*) FROM gnomad_genomes_agg
        UNION ALL
        SELECT 'gnomad_exomes', COUNT(*) FROM gnomad_exomes_agg
        UNION ALL
        SELECT 'gme', COUNT(*) FROM gme_agg
        ORDER BY source_name
        """
    ).strip()


def build_registry_step_sql(step_id: str, limit: int = 10) -> str:
    step_queries = {
        'clinvar_raw_brca': dedent(
            f"""
            {_common_ctes(include_gme=False)}
            SELECT ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number, *
            FROM clinvar_agg
            QUALIFY sample_row_number <= {limit}
            ORDER BY sample_row_number
            """
        ).strip(),
        'gnomad_genomes_raw_brca': dedent(
            f"""
            {_common_ctes(include_gme=False)}
            SELECT ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number, *
            FROM gnomad_genomes_agg
            QUALIFY sample_row_number <= {limit}
            ORDER BY sample_row_number
            """
        ).strip(),
        'gnomad_exomes_raw_brca': dedent(
            f"""
            {_common_ctes(include_gme=False)}
            SELECT ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number, *
            FROM gnomad_exomes_agg
            QUALIFY sample_row_number <= {limit}
            ORDER BY sample_row_number
            """
        ).strip(),
        'pre_gme_checkpoint': build_pre_gme_sample_sql(limit=limit),
        'gme_raw_brca': dedent(
            f"""
            {_common_ctes(include_gme=True)}
            SELECT ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number, *
            FROM gme_agg
            QUALIFY sample_row_number <= {limit}
            ORDER BY sample_row_number
            """
        ).strip(),
        'final_checkpoint': build_registry_sample_sql(limit=limit),
    }
    if step_id not in step_queries:
        raise KeyError(f'Unknown registry step: {step_id}')
    return step_queries[step_id]


def build_registry_step_export_sql(step_id: str) -> str:
    step_queries = {
        'clinvar_raw_brca': dedent(
            f"""
            {_common_ctes(include_gme=False)}
            SELECT *
            FROM clinvar_agg
            ORDER BY gene_symbol, chrom, pos, ref, alt
            """
        ).strip(),
        'gnomad_genomes_raw_brca': dedent(
            f"""
            {_common_ctes(include_gme=False)}
            SELECT *
            FROM gnomad_genomes_agg
            ORDER BY gene_symbol, chrom, pos, ref, alt
            """
        ).strip(),
        'gnomad_exomes_raw_brca': dedent(
            f"""
            {_common_ctes(include_gme=False)}
            SELECT *
            FROM gnomad_exomes_agg
            ORDER BY gene_symbol, chrom, pos, ref, alt
            """
        ).strip(),
        'pre_gme_checkpoint': build_pre_gme_export_sql(),
        'gme_raw_brca': dedent(
            f"""
            {_common_ctes(include_gme=True)}
            SELECT *
            FROM gme_agg
            ORDER BY gene_symbol, chrom, pos, ref, alt
            """
        ).strip(),
        'final_checkpoint': build_registry_export_sql(),
    }
    if step_id not in step_queries:
        raise KeyError(f'Unknown registry step: {step_id}')
    return step_queries[step_id]
