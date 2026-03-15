import pandas as pd

from scripts.build_brca_normalized_artifacts import (
    build_checkpoint,
    convert_table_variant,
    parse_clinvar_omim_pairs,
    uri_prefix,
    variant_type,
)


def test_variant_type_classifies_basic_alleles():
    assert variant_type("A", "G") == "SNV"
    assert variant_type("AT", "GC") == "MNV"
    assert variant_type("A", "ATG") == "INS"
    assert variant_type("ATG", "A") == "DEL"



def test_convert_table_variant_anchors_deletions_and_insertions():
    reference = {"chr13": "AACCGGTT"}

    delete_pos, delete_ref, delete_alt = convert_table_variant(reference, "chr13", 4, 5, "CG", "-")
    assert (delete_pos, delete_ref, delete_alt) == (3, "CCG", "C")

    insert_pos, insert_ref, insert_alt = convert_table_variant(reference, "chr13", 5, 5, "-", "TA")
    assert (insert_pos, insert_ref, insert_alt) == (4, "C", "CTA")



def test_parse_clinvar_omim_pairs_keeps_only_omim_backed_items():
    phenotypes, omim_ids = parse_clinvar_omim_pairs(
        "Disease_one|Disease_two|not_provided",
        "OMIM:123456,MedGen:C1|MedGen:C2|OMIM:999999",
    )

    assert phenotypes == "Disease_one | not_provided"
    assert omim_ids == "123456 | 999999"



def test_uri_prefix_preserves_gs_prefixes():
    assert uri_prefix("gs://bucket/path/file.parquet") == "gs://bucket/path/"



def test_build_checkpoint_combines_gnomad_counts_without_hiding_inputs():
    clinvar = pd.DataFrame(
        [
            {
                "variant_key": "chr13:10:A:G",
                "chrom38": "chr13",
                "pos38": 10,
                "end38": 10,
                "ref_norm": "A",
                "alt_norm": "G",
                "gene": "BRCA2",
                "vartype": "SNV",
                "alleleid": "101",
                "clnsig": "Pathogenic",
                "clnrevstat": "reviewed",
                "effect": "missense_variant",
                "phenotypes_omim": "Disease_one",
                "phenotypes_omim_id": "123456",
            }
        ]
    )
    gnomad_genomes = pd.DataFrame(
        [
            {
                "variant_key": "chr13:10:A:G",
                "chrom38": "chr13",
                "pos38": 10,
                "end38": 10,
                "ref_norm": "A",
                "alt_norm": "G",
                "gene": "BRCA2",
                "vartype": "SNV",
                "genomes_ac": 2,
                "genomes_an": 100,
                "genomes_af": 0.02,
                "genomes_hom": 1,
                "genomes_ac_mid": 1,
                "genomes_an_mid": 20,
                "genomes_af_mid": 0.05,
                "genomes_hom_mid": 0,
                "genomes_ac_afr": 0,
                "genomes_af_afr": 0.0,
                "genomes_af_eur_proxy": 0.07,
                "genomes_depth": 33.0,
            }
        ]
    )
    gnomad_exomes = pd.DataFrame(
        [
            {
                "variant_key": "chr13:10:A:G",
                "chrom38": "chr13",
                "pos38": 10,
                "end38": 10,
                "ref_norm": "A",
                "alt_norm": "G",
                "gene": "BRCA2",
                "vartype": "SNV",
                "exomes_ac": 3,
                "exomes_an": 50,
                "exomes_af": 0.06,
                "exomes_hom": 2,
                "exomes_ac_mid": 2,
                "exomes_an_mid": 10,
                "exomes_af_mid": 0.2,
                "exomes_hom_mid": 1,
                "exomes_ac_afr": 1,
                "exomes_af_afr": 0.01,
                "exomes_af_eur_proxy": 0.08,
                "exomes_depth": 41.0,
            }
        ]
    )
    shgp = pd.DataFrame(
        [
            {
                "variant_key": "chr13:10:A:G",
                "chrom38": "chr13",
                "pos38": 10,
                "end38": 10,
                "ref_norm": "A",
                "alt_norm": "G",
                "gene": "BRCA2",
                "vartype": "SNV",
                "shgp_ac": 4,
                "shgp_an": 200,
                "shgp_af": 0.02,
            }
        ]
    )

    checkpoint = build_checkpoint(
        clinvar=clinvar,
        gnomad_genomes=gnomad_genomes,
        gnomad_exomes=gnomad_exomes,
        shgp=shgp,
        gme=None,
        stage_label="pre_gme_arab_checkpoint",
    )

    row = checkpoint.iloc[0]
    assert row["GNOMAD_ALL_AF"] == 5 / 150
    assert row["gnomAD_all_Hom"] == 3
    assert row["GNOMAD_MID_AF"] == 3 / 30
    assert row["gnomAD_mid_Hom"] == 1
    assert row["SHGP_AF"] == 0.02
    assert row["GNOMAD_GENOMES_AF_AFR"] == 0.0
    assert row["GNOMAD_GENOMES_AF_EUR_PROXY"] == 0.07
    assert row["GNOMAD_EXOMES_AF_AFR"] == 0.01
    assert row["GNOMAD_EXOMES_AF_EUR_PROXY"] == 0.08
    assert row["GNOMAD_GENOMES_DEPTH"] == 33.0
    assert row["GNOMAD_EXOMES_DEPTH"] == 41.0
    assert row["PRESENT_IN_CLINVAR"] == 1
    assert row["PRESENT_IN_SHGP"] == 1
    assert row["SOURCE_COUNT"] == 4


def test_build_checkpoint_keeps_gme_context_columns_for_audit():
    checkpoint = build_checkpoint(
        clinvar=pd.DataFrame(
            [
                {
                    "variant_key": "chr17:20:C:T",
                    "chrom38": "chr17",
                    "pos38": 20,
                    "end38": 20,
                    "ref_norm": "C",
                    "alt_norm": "T",
                    "gene": "BRCA1",
                    "vartype": "SNV",
                }
            ]
        ),
        gnomad_genomes=pd.DataFrame(),
        gnomad_exomes=pd.DataFrame(),
        shgp=pd.DataFrame(),
        gme=pd.DataFrame(
            [
                {
                    "variant_key": "chr17:20:C:T",
                    "chrom38": "chr17",
                    "pos38": 20,
                    "end38": 20,
                    "ref_norm": "C",
                    "alt_norm": "T",
                    "gene": "BRCA1",
                    "vartype": "SNV",
                    "gme_af": 0.2,
                    "gme_nwa": 0.1,
                    "gme_nea": 0.2,
                    "gme_ap": 0.3,
                    "gme_israel": 0.4,
                    "gme_sd": 0.5,
                    "gme_tp": 0.6,
                    "gme_ca": 0.7,
                }
            ]
        ),
        stage_label="final_arab_checkpoint",
    )

    row = checkpoint.iloc[0]
    assert row["GME_AF"] == 0.2
    assert row["GME_ISRAEL"] == 0.4
    assert row["GME_TP"] == 0.6
    assert row["GME_CA"] == 0.7
