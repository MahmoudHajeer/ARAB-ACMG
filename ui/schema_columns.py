from __future__ import annotations

from typing import Final

REQUIRED_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("CHROM", "Canonical GRCh38 chromosome label with the chr prefix."),
    ("POS", "1-based GRCh38 start position for the allele."),
    ("END", "1-based GRCh38 end position derived as POS + len(REF) - 1."),
    ("ID", "Stable cross-source identifier used for this pipeline row."),
    ("REF", "Reference allele."),
    ("ALT", "Alternate allele."),
    ("VARTYPE", "Simple allele class derived from REF/ALT lengths (SNV, MNV, INS, DEL, COMPLEX)."),
    ("Repeat", "Repeat-region annotation placeholder. Remains NULL until a repeat-track source is added."),
    ("Segdup", "Segmental-duplication annotation placeholder. Remains NULL until a segdup track is added."),
    ("Blacklist", "Blacklist-region annotation placeholder. Remains NULL until a blacklist source is added."),
    ("GENE", "Target BRCA gene assigned from the frozen Ensembl-backed coordinate windows."),
    ("EFFECT", "Variant effect label. Currently filled only when a source-backed effect tag is available."),
    ("HGVS_C", "Coding HGVS notation placeholder. Remains NULL until transcript annotation is added."),
    ("HGVS_P", "Protein HGVS notation placeholder. Remains NULL until transcript annotation is added."),
    ("PHENOTYPES_OMIM", "OMIM phenotype label placeholder. Remains NULL until an OMIM-backed source is added."),
    ("PHENOTYPES_OMIM_ID", "OMIM phenotype identifier placeholder. Remains NULL until an OMIM-backed source is added."),
    ("INHERITANCE_PATTERN", "Inheritance placeholder. Remains NULL until a source-backed inheritance annotation is added."),
    ("ALLELEID", "ClinVar ALLELEID when the allele appears in ClinVar."),
    ("CLNSIG", "ClinVar clinical-significance label when available."),
    ("TOPMED_AF", "TOPMed allele frequency placeholder. Remains NULL until TOPMed is added."),
    ("TOPMed_Hom", "TOPMed homozygote count placeholder. Remains NULL until TOPMed is added."),
    ("ALFA_AF", "ALFA allele frequency placeholder. Remains NULL until ALFA is added."),
    ("ALFA_Hom", "ALFA homozygote count placeholder. Remains NULL until ALFA is added."),
    ("GNOMAD_ALL_AF", "Combined AF across the currently loaded gnomAD genomes and exomes cohorts."),
    ("gnomAD_all_Hom", "Combined homozygote count across the currently loaded gnomAD genomes and exomes cohorts."),
    ("GNOMAD_MID_AF", "Combined Middle Eastern AF across the currently loaded gnomAD genomes and exomes cohorts."),
    ("gnomAD_mid_Hom", "Combined Middle Eastern homozygote count across the currently loaded gnomAD genomes and exomes cohorts."),
    ("ONEKGP_AF", "1000 Genomes AF placeholder. Remains NULL until that source is added."),
    ("REGENERON_AF", "Regeneron AF placeholder. Remains NULL until that source is added."),
    ("TGP_AF", "TGP AF placeholder. Remains NULL until that source is added."),
    ("QATARI", "Qatari frequency placeholder. Remains NULL until that source is added."),
    ("JGP_AF", "JGP AF placeholder. Remains NULL until that source is added."),
    ("JGP_MAF", "JGP minor-allele frequency placeholder. Remains NULL until that source is added."),
    ("JGP_Hom", "JGP homozygote count placeholder. Remains NULL until that source is added."),
    ("JGP_Het", "JGP heterozygote count placeholder. Remains NULL until that source is added."),
    ("JGP_AC_Hemi", "JGP hemizygous alternate count placeholder. Remains NULL until that source is added."),
    ("SIFT_PRED", "SIFT prediction placeholder. Remains NULL until an annotation engine is added."),
    ("POLYPHEN2_HDIV_PRED", "PolyPhen2 HDIV prediction placeholder. Remains NULL until an annotation engine is added."),
    ("POLYPHEN2_HVAR_PRED", "PolyPhen2 HVAR prediction placeholder. Remains NULL until an annotation engine is added."),
    ("PROVEAN_PRE", "PROVEAN prediction placeholder. Column name preserved exactly as requested by the user."),
)

PRE_GME_EXTRA_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("CLNREVSTAT", "ClinVar review-status label preserved as a pipeline extra."),
    ("GNOMAD_GENOMES_AC", "gnomAD genomes allele count."),
    ("GNOMAD_GENOMES_AN", "gnomAD genomes allele number."),
    ("GNOMAD_GENOMES_AF", "gnomAD genomes allele frequency."),
    ("GNOMAD_GENOMES_HOM", "gnomAD genomes homozygote count."),
    ("GNOMAD_GENOMES_AF_AFR", "gnomAD genomes African-ancestry AF."),
    ("GNOMAD_GENOMES_AF_EUR_PROXY", "gnomAD genomes Europe-proxy AF built from NFE + FIN + ASJ."),
    ("GNOMAD_EXOMES_AC", "gnomAD exomes allele count."),
    ("GNOMAD_EXOMES_AN", "gnomAD exomes allele number."),
    ("GNOMAD_EXOMES_AF", "gnomAD exomes allele frequency."),
    ("GNOMAD_EXOMES_HOM", "gnomAD exomes homozygote count."),
    ("GNOMAD_EXOMES_AF_AFR", "gnomAD exomes African-ancestry AF."),
    ("GNOMAD_EXOMES_AF_EUR_PROXY", "gnomAD exomes Europe-proxy AF built from NFE + FIN + ASJ."),
    ("GNOMAD_GENOMES_DEPTH", "gnomAD genomes depth tag when present in raw INFO."),
    ("GNOMAD_EXOMES_DEPTH", "gnomAD exomes depth tag when present in raw INFO."),
    ("SOURCE_COUNT", "How many non-GME source streams support the allele."),
    ("PIPELINE_STAGE", "Checkpoint label for the current exported table."),
)

FINAL_GME_EXTRA_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("GME_AF", "Overall GME allele frequency."),
    ("GME_NWA", "GME North West Africa subgroup frequency."),
    ("GME_NEA", "GME North East Africa subgroup frequency."),
    ("GME_AP", "GME Arabian Peninsula subgroup frequency."),
    ("GME_ISRAEL", "GME Israel/Jewish subgroup frequency."),
    ("GME_SD", "GME Syrian Desert subgroup frequency."),
    ("GME_TP", "GME Turkish Peninsula subgroup frequency."),
    ("GME_CA", "GME Central Asia subgroup frequency."),
)


def pre_gme_columns() -> tuple[tuple[str, str], ...]:
    return REQUIRED_COLUMNS + PRE_GME_EXTRA_COLUMNS


def final_columns() -> tuple[tuple[str, str], ...]:
    return REQUIRED_COLUMNS + PRE_GME_EXTRA_COLUMNS + FINAL_GME_EXTRA_COLUMNS


def column_payload(columns: tuple[tuple[str, str], ...]) -> list[dict[str, str]]:
    required_names = {name for name, _ in REQUIRED_COLUMNS}
    return [
        {
            "name": name,
            "description": description,
            "kind": "required" if name in required_names else "extra",
        }
        for name, description in columns
    ]
