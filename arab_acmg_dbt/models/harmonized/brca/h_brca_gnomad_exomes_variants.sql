{{ config(materialized='table', schema='arab_acmg_harmonized', tags=['t003', 'brca', 'gnomad', 'exomes']) }}

with gene_windows as (
    select *
    from {{ ref('h_brca_gene_windows') }}
),
source_rows as (
    -- [AI-Agent: Codex]: Exome rows are extracted with the same BRCA windows as genomes.
    -- The interval does not change; only the cohort and coverage pattern change.
    select
        gene_windows.gene_symbol,
        gene_windows.ensembl_gene_id,
        'gnomad' as source_name,
        'exomes' as cohort,
        'GRCh38' as source_build,
        'not_needed' as liftover_status,
        'success' as norm_status,
        gnomad.source_version,
        gnomad.snapshot_date,
        gnomad.record_key as source_record_key,
        gnomad.variant_key,
        gene_windows.chrom38,
        gnomad.pos as pos38,
        gnomad.ref as ref_norm,
        gnomad.alt as alt_norm,
        gnomad.ac,
        gnomad.an,
        gnomad.af,
        gnomad.grpmax_population,
        gnomad.grpmax_faf95,
        gnomad.depth,
        gnomad.ac_afr,
        gnomad.af_afr,
        gnomad.ac_eur_proxy,
        gnomad.af_eur_proxy
    from {{ ref('stg_gnomad_exomes_variants') }} as gnomad
    join gene_windows
      on gnomad.chrom_norm = gene_windows.chrom_nochr
     and gnomad.pos between gene_windows.start_pos38 and gene_windows.end_pos38
),
aggregated as (
    select
        gene_symbol,
        ensembl_gene_id,
        source_name,
        cohort,
        source_build,
        liftover_status,
        norm_status,
        any_value(source_version) as source_version,
        any_value(snapshot_date) as snapshot_date,
        variant_key,
        any_value(chrom38) as chrom38,
        any_value(pos38) as pos38,
        any_value(ref_norm) as ref_norm,
        any_value(alt_norm) as alt_norm,
        count(*) as source_row_count,
        max(ac) as ac,
        max(an) as an,
        max(af) as af,
        any_value(grpmax_population) as grpmax_population,
        max(grpmax_faf95) as grpmax_faf95,
        max(depth) as depth,
        max(ac_afr) as ac_afr,
        max(af_afr) as af_afr,
        max(ac_eur_proxy) as ac_eur_proxy,
        max(af_eur_proxy) as af_eur_proxy
    from source_rows
    group by
        gene_symbol,
        ensembl_gene_id,
        source_name,
        cohort,
        source_build,
        liftover_status,
        norm_status,
        variant_key
)
select
    *,
    'coordinate_window' as extraction_rule
from aggregated
