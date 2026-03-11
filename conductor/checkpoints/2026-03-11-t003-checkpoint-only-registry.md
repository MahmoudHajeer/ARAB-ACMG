# T003 Checkpoint: BRCA Checkpoint-Only Registry

## Scope
By explicit user request, the harmonized layer was reduced to two supervisor-facing checkpoint tables only:
- `genome-services-platform.arab_acmg_harmonized.supervisor_variant_registry_brca_pre_gme_v1`
- `genome-services-platform.arab_acmg_harmonized.supervisor_variant_registry_brca_v1`

All durable per-source BRCA harmonized tables/views were removed from `arab_acmg_harmonized`.

## Required Header Floor
Both checkpoint tables expose the user-mandated minimum publication-facing header:
`CHROM, POS, END, ID, REF, ALT, VARTYPE, Repeat, Segdup, Blacklist, GENE, EFFECT, HGVS_C, HGVS_P, PHENOTYPES_OMIM, PHENOTYPES_OMIM_ID, INHERITANCE_PATTERN, ALLELEID, CLNSIG, TOPMED_AF, TOPMed_Hom, ALFA_AF, ALFA_Hom, GNOMAD_ALL_AF, gnomAD_all_Hom, GNOMAD_MID_AF, gnomAD_mid_Hom, ONEKGP_AF, REGENERON_AF, TGP_AF, QATARI, JGP_AF, JGP_MAF, JGP_Hom, JGP_Het, JGP_AC_Hemi, SIFT_PRED, POLYPHEN2_HDIV_PRED, POLYPHEN2_HVAR_PRED, PROVEAN_PRE`

Rules applied:
- unsupported fields remain `NULL`
- pipeline extras appear after the required header
- extras are marked visually in the UI and Excel export

## Source of Truth
Frozen BRCA windows remain sourced from Ensembl GRCh38 and are versioned in:
- `arab_acmg_dbt/seeds/brca_gene_windows_seed.csv`
- `ui/brca_gene_windows_seed.csv` (bundled for Cloud Run runtime parity)

## Live Row Counts
- `supervisor_variant_registry_brca_pre_gme_v1`: `116,067`
- `supervisor_variant_registry_brca_v1`: `116,087`

## Cloud Run Deployment
- Service: `supervisor-ui`
- Revision: `supervisor-ui-00010-qql`
- URLs:
  - `https://supervisor-ui-142306018756.europe-west1.run.app`
  - `https://supervisor-ui-wrx363kqnq-ew.a.run.app`

## Verification
- `python3 scripts/build_supervisor_registry.py` -> pass
- `python3 scripts/verify_supervisor_registry.py` -> pass
- `python3 scripts/export_pre_gme_registry_xlsx.py` -> pass
- `python3 -m pytest -q tests` -> pass
- `node --check ui/app.js` -> pass
- `curl -s https://supervisor-ui-wrx363kqnq-ew.a.run.app/api/pre-gme` -> pass
- `curl -s https://supervisor-ui-wrx363kqnq-ew.a.run.app/api/registry` -> pass
- `curl -s 'https://supervisor-ui-wrx363kqnq-ew.a.run.app/api/pre-gme/sample?limit=10'` -> pass
- `curl -s 'https://supervisor-ui-wrx363kqnq-ew.a.run.app/api/registry/sample?limit=10'` -> pass
