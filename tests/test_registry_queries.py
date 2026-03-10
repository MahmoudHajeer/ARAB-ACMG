from ui.registry_queries import (
    REGISTRY_TABLE_REF,
    build_sample_sql,
    build_registry_sample_sql,
    build_registry_sql,
    build_registry_step_sql,
)


def test_build_registry_sql_targets_expected_table_and_columns():
    sql = build_registry_sql()

    assert f"CREATE OR REPLACE TABLE `{REGISTRY_TABLE_REF}`" in sql
    assert "gene_symbol" in sql
    assert "variant_key" in sql
    assert "gnomad_genomes_ac_afr" in sql
    assert "gnomad_exomes_af_eur_proxy" in sql
    assert "CURRENT_DATE() AS last_refresh_date" in sql


def test_build_registry_sample_sql_uses_tablesample_and_row_number():
    sql = build_registry_sample_sql(sample_percent=0.2, limit=50)

    assert "TABLESAMPLE SYSTEM" not in sql
    assert f"FROM `{REGISTRY_TABLE_REF}`" in sql
    assert "ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number" in sql
    assert "WHERE sample_row_number <= 50" in sql


def test_build_sample_sql_reads_gene_windows_without_tablesample():
    sql = build_sample_sql("genome-services-platform.arab_acmg_harmonized.h_brca_gene_windows", sample_percent=100, limit=10)

    assert "TABLESAMPLE SYSTEM" not in sql
    assert "ORDER BY gene_symbol" in sql
    assert "LIMIT 10" in sql


def test_build_registry_step_sql_targets_harmonized_gnomad_table():
    sql = build_registry_step_sql("gnomad_genomes_brca", limit=10)

    assert "h_brca_gnomad_genomes_variants" in sql
    assert "TABLESAMPLE SYSTEM" in sql
