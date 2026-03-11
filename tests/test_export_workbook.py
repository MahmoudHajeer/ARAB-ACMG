from io import BytesIO

from openpyxl import load_workbook

from ui.export_workbook import build_pre_gme_workbook_bytes, export_header_columns, export_metadata_lines


def test_build_pre_gme_workbook_bytes_writes_metadata_header_and_rows():
    workbook_bytes = build_pre_gme_workbook_bytes(
        [
            {
                "chrom": "chr13",
                "pos": 32315086,
                "end_pos": 32315086,
                "export_id": "13:32315086:A:G",
                "variant_key": "13:32315086:A:G",
                "ref": "A",
                "alt": "G",
                "gene_symbol": "BRCA2",
                "present_in_clinvar": True,
                "present_in_gnomad_genomes": False,
                "present_in_gnomad_exomes": True,
                "clinvar_ids": "123",
                "clinvar_significance_values": "Pathogenic",
                "clinvar_review_status_values": "reviewed_by_expert_panel",
                "clinvar_record_count": 1,
                "clinvar_gene_info_match_count": 1,
                "clinvar_gene_info_mismatch_count": 0,
                "gnomad_genomes_ac": None,
                "gnomad_genomes_an": None,
                "gnomad_genomes_af": None,
                "gnomad_genomes_grpmax": None,
                "gnomad_genomes_grpmax_faf95": None,
                "gnomad_genomes_depth": None,
                "gnomad_genomes_ac_afr": None,
                "gnomad_genomes_af_afr": None,
                "gnomad_genomes_ac_eur_proxy": None,
                "gnomad_genomes_af_eur_proxy": None,
                "gnomad_exomes_ac": 4,
                "gnomad_exomes_an": 1000,
                "gnomad_exomes_af": 0.004,
                "gnomad_exomes_grpmax": "nfe",
                "gnomad_exomes_grpmax_faf95": 0.002,
                "gnomad_exomes_depth": 88,
                "gnomad_exomes_ac_afr": 0,
                "gnomad_exomes_af_afr": 0.0,
                "gnomad_exomes_ac_eur_proxy": 4,
                "gnomad_exomes_af_eur_proxy": 0.004,
                "source_count": 2,
            }
        ],
        created_at="11/03/2026 12:00",
    )

    workbook = load_workbook(BytesIO(workbook_bytes), read_only=True)
    sheet = workbook[workbook.sheetnames[0]]
    rows = list(sheet.iter_rows(values_only=True))

    assert rows[0][0] == "ARAB_ACMG_PRE_GME_PIPELINE - Created on: 11/03/2026 12:00"
    assert rows[1][0].startswith("##WORKFLOW=")
    assert rows[len(export_metadata_lines("11/03/2026 12:00"))][0] == export_header_columns()[0]
    assert rows[-1][0] == "chr13"
    assert rows[-1][8] == "TRUE"
