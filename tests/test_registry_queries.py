from pathlib import Path

import ui.registry_queries as registry_queries

from ui.registry_queries import (
    PRE_GME_REGISTRY_TABLE_REF,
    REGISTRY_TABLE_REF,
    build_final_source_count_sql,
    build_pre_gme_export_sql,
    build_pre_gme_registry_sql,
    build_pre_gme_sample_sql,
    build_pre_gme_source_count_sql,
    build_raw_sample_sql,
    build_registry_sample_sql,
    build_registry_sql,
    build_registry_step_sql,
    resolve_gene_window_seed,
)


def test_build_registry_sql_targets_expected_table_and_required_columns():
    sql = build_registry_sql()

    assert f"CREATE OR REPLACE TABLE `{REGISTRY_TABLE_REF}`" in sql
    assert "`CHROM`" in sql
    assert "`GNOMAD_ALL_AF`" in sql
    assert "`GME_AF`" in sql
    assert "h_brca_" not in sql
    assert "stg_gnomad" not in sql


def test_build_pre_gme_registry_sql_targets_expected_table_and_excludes_gme_extras():
    sql = build_pre_gme_registry_sql()

    assert f"CREATE OR REPLACE TABLE `{PRE_GME_REGISTRY_TABLE_REF}`" in sql
    assert "`GNOMAD_ALL_AF`" in sql
    assert "`PIPELINE_STAGE`" in sql
    assert "`GME_AF`" not in sql


def test_build_registry_sample_sql_reads_final_table_directly():
    sql = build_registry_sample_sql(sample_percent=0.2, limit=50)

    assert f"FROM `{REGISTRY_TABLE_REF}`" in sql
    assert "ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number" in sql
    assert "WHERE sample_row_number <= 50" in sql


def test_build_pre_gme_sample_sql_reads_pre_gme_table_directly():
    sql = build_pre_gme_sample_sql(limit=10)

    assert f"FROM `{PRE_GME_REGISTRY_TABLE_REF}`" in sql
    assert "TABLESAMPLE SYSTEM" not in sql


def test_build_raw_sample_sql_uses_tablesample():
    sql = build_raw_sample_sql("genome-services-platform.arab_acmg_raw.clinvar_raw_vcf", sample_percent=1.0, limit=10)

    assert "TABLESAMPLE SYSTEM (1.0 PERCENT)" in sql
    assert "WHERE sample_row_number <= 10" in sql


def test_build_registry_step_sql_targets_raw_checkpoint_pipeline_steps():
    sql = build_registry_step_sql("gnomad_genomes_raw_brca", limit=10)

    assert "gnomad_genomes_agg" in sql
    assert "h_brca_gnomad_genomes_variants" not in sql


def test_build_pre_gme_export_sql_orders_rows_for_excel_export():
    sql = build_pre_gme_export_sql(limit=25)

    assert f"FROM `{PRE_GME_REGISTRY_TABLE_REF}`" in sql
    assert "ORDER BY `GENE`, `CHROM`, `POS`, `REF`, `ALT`" in sql
    assert "LIMIT 25" in sql


def test_source_count_queries_read_raw_based_ctes():
    assert "gnomad_genomes_agg" in build_pre_gme_source_count_sql()
    assert "gme_agg" in build_final_source_count_sql()


def test_resolve_gene_window_seed_falls_back_to_ui_bundle(monkeypatch, tmp_path):
    missing_repo_seed = tmp_path / "missing.csv"
    bundled_seed = tmp_path / "bundled.csv"
    bundled_seed.write_text("gene_symbol\nBRCA1\n", encoding="utf-8")

    monkeypatch.setattr(
        registry_queries,
        "GENE_WINDOW_SEED_CANDIDATES",
        (missing_repo_seed, bundled_seed),
    )

    assert resolve_gene_window_seed() == Path(bundled_seed)
