from ui.registry_queries import (
    REGISTRY_TABLE_REF,
    build_registry_sample_sql,
    build_registry_sql,
    build_registry_step_sql,
)


def test_build_registry_sql_targets_expected_table_and_columns():
    sql = build_registry_sql()

    assert f"CREATE OR REPLACE TABLE `{REGISTRY_TABLE_REF}`" in sql
    assert "variant_key" in sql
    assert "gnomad_genomes_ac_afr" in sql
    assert "gnomad_exomes_af_eur_proxy" in sql
    assert "CURRENT_DATE() AS last_refresh_date" in sql


def test_build_registry_sample_sql_uses_tablesample_and_row_number():
    sql = build_registry_sample_sql(sample_percent=0.2, limit=50)

    assert "TABLESAMPLE SYSTEM (0.2 PERCENT)" in sql
    assert "ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number" in sql
    assert "WHERE sample_row_number <= 50" in sql


def test_build_registry_step_sql_includes_gnomad_metrics_proxy_fields():
    sql = build_registry_step_sql("gnomad_metrics", limit=50)

    assert "ac_eur_proxy" in sql
    assert "af_eur_proxy" in sql
    assert "grpmax_faf95" in sql
