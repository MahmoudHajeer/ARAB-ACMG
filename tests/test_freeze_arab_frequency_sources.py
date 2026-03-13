import pandas as pd

from scripts.freeze_arab_frequency_sources import build_avdb_liftover_report, parse_hgvs_genomic37


def test_parse_hgvs_genomic37_supports_main_avdb_event_types():
    substitution = parse_hgvs_genomic37("NC_000001.10:g.94473807C>T")
    deletion = parse_hgvs_genomic37("NC_000013.10:g.23894840del")
    deletion_range = parse_hgvs_genomic37("NC_000016.9:g.226812_226816del")
    duplication = parse_hgvs_genomic37("NC_000017.10:g.19552416dup")
    insertion = parse_hgvs_genomic37("NC_000017.10:g.19552393_19552394insA")
    delins = parse_hgvs_genomic37("NC_000012.11:g.80838564delinsATATG")

    assert substitution.event_type == "substitution"
    assert substitution.start37 == 94473807
    assert substitution.source_ref == "C"
    assert substitution.source_alt == "T"
    assert deletion.event_type == "deletion"
    assert deletion_range.end37 == 226816
    assert duplication.event_type == "duplication"
    assert insertion.inserted_sequence == "A"
    assert delins.event_type == "delins"


def test_build_avdb_liftover_report_summarizes_success_and_failure():
    frame = pd.DataFrame(
        [
            {
                "gene_symbol": "BRCA1",
                "parse_status": "parsed",
                "liftover_status": "success",
                "event_type": "substitution",
                "source_row_number": 2,
                "hgvs_genomic_grch37": "NC_000017.10:g.41223094A>G",
                "liftover_notes": "ok",
            },
            {
                "gene_symbol": "BRCA2",
                "parse_status": "parsed",
                "liftover_status": "failed",
                "event_type": "deletion",
                "source_row_number": 3,
                "hgvs_genomic_grch37": "NC_000013.10:g.32900000del",
                "liftover_notes": "expected_single_mapping_got_0",
            },
            {
                "gene_symbol": "MEFV",
                "parse_status": "missing_coordinates",
                "liftover_status": "not_applicable",
                "event_type": None,
                "source_row_number": 4,
                "hgvs_genomic_grch37": None,
                "liftover_notes": "HGVS_Genomic_GRCh37 was empty or unsupported.",
            },
        ]
    )

    report = build_avdb_liftover_report(frame)

    assert report["counts"]["total_rows"] == 3
    assert report["counts"]["parse_success_rows"] == 2
    assert report["counts"]["liftover_success_rows"] == 1
    assert report["counts"]["brca_rows"] == 2
    assert report["event_type_counts"]["missing"] == 1
    assert report["use_decision"]["label"] == "reference_only"
