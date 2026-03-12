import pandas as pd

from scripts.freeze_arab_study_sources import ExtractSpec, apply_extract_spec, slugify


def test_slugify_produces_stable_snake_case():
    assert slugify("Cancer Cohort") == "cancer_cohort"
    assert slugify("ER/PR  Status") == "er_pr_status"


def test_apply_extract_spec_filters_non_variant_rows_and_adds_locators():
    frame = pd.DataFrame(
        {
            "Mutations ": ["Negative", "BRCA2", None],
            "HGVS": ["-", "NM_000059.3:c.1A>T", "-"],
            "Chr location (hg38)": ["-", "chr13:1", "-"],
            "Pathogenicity": ["-", "Tier 3", "-"],
        }
    )
    spec = ExtractSpec(
        sheet_name="Cancer Cohort",
        output_slug="cancer_cohort_variant_rows",
        keep_columns=("Mutations", "HGVS", "Chr location (hg38)", "Pathogenicity"),
        filter_column="Mutations",
        exclude_values=("Negative",),
    )

    extracted = apply_extract_spec(frame, spec)

    assert extracted.to_dict(orient="records") == [
        {
            "mutations": "BRCA2",
            "hgvs": "NM_000059.3:c.1A>T",
            "chr_location_hg38": "chr13:1",
            "pathogenicity": "Tier 3",
            "source_sheet_name": "Cancer Cohort",
            "source_row_number": 3,
            "source_record_locator": "sheet=Cancer Cohort;row=3",
        }
    ]


def test_apply_extract_spec_keeps_only_whitelisted_columns():
    frame = pd.DataFrame(
        {
            "Carrier": ["BC_P_001"],
            "Gene": ["BRCA1"],
            "Pathogenic Variant Type": ["missense_variant"],
            " Clinvar Significance": ["Pathogenic"],
            "Clinvar Submissions": ["Pathogenic(1)"],
            "HGVS Codon Change": ["NM_007294.4:c.1A>T"],
            "HGVS Protein Change": ["NP_009225.1:p.Met1Leu"],
        }
    )
    spec = ExtractSpec(
        sheet_name="Table S5",
        output_slug="variant_carriers",
        keep_columns=(
            "Gene",
            "Pathogenic Variant Type",
            "Clinvar Significance",
            "Clinvar Submissions",
            "HGVS Codon Change",
            "HGVS Protein Change",
        ),
    )

    extracted = apply_extract_spec(frame, spec)

    assert list(extracted.columns) == [
        "gene",
        "pathogenic_variant_type",
        "clinvar_significance",
        "clinvar_submissions",
        "hgvs_codon_change",
        "hgvs_protein_change",
        "source_sheet_name",
        "source_row_number",
        "source_record_locator",
    ]
    assert "carrier" not in extracted.columns
