from scripts.ge_expectation_specs import (
    CLINVAR_TABLE,
    GNOMAD_GE_TABLES,
    GNOMAD_TABLES,
    build_clinvar_raw_query,
    build_gnomad_raw_query,
    clinvar_raw_expectations,
    gnomad_raw_expectations,
)


def test_clinvar_raw_query_targets_live_raw_table():
    sql = build_clinvar_raw_query()

    assert CLINVAR_TABLE in sql
    assert "SELECT" in sql
    assert "chrom" in sql
    assert "info" in sql


def test_gnomad_raw_query_derives_duplicate_and_frequency_fields():
    sql = build_gnomad_raw_query(GNOMAD_TABLES["gnomad_v4_1_genomes_chr13_raw"])

    assert "allele_duplicate_count" in sql
    assert "AF_afr" in sql
    assert "faf95" in sql
    assert "COALESCE(an, 0) > 0" in sql


def test_clinvar_expectations_cover_columns_and_row_count():
    expectation_types = {item["type"] for item in clinvar_raw_expectations()}

    assert "expect_table_columns_to_match_ordered_list" in expectation_types
    assert "expect_table_row_count_to_be_between" in expectation_types
    assert any(item["kwargs"].get("column") == "pos" for item in clinvar_raw_expectations())


def test_gnomad_expectations_cover_af_an_and_duplicates():
    expectations = gnomad_raw_expectations()
    expectation_types = {item["type"] for item in expectations}

    assert "expect_column_values_to_be_unique" in expectation_types
    assert "expect_column_to_exist" in expectation_types
    assert "expect_table_row_count_to_be_between" in expectation_types
    assert any(item["kwargs"].get("column") == "an" for item in expectations)
    assert any(item["kwargs"].get("column") == "af_eur_proxy" for item in expectations)


def test_gnomad_ge_views_point_to_harmonized_staging_tables():
    assert GNOMAD_GE_TABLES["stg_gnomad_genomes_variants"].endswith(".stg_gnomad_genomes_variants")
    assert GNOMAD_GE_TABLES["stg_gnomad_exomes_variants"].endswith(".stg_gnomad_exomes_variants")
