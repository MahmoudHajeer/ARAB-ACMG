{{ config(materialized='table', schema='arab_acmg_harmonized', tags=['t003', 'brca', 'gme']) }}

with gene_windows as (
    select *
    from {{ ref('h_brca_gene_windows') }}
),
source_rows as (
    -- [AI-Agent: Codex]: The frozen GME file is already an hg38 summary table, so BRCA
    -- extraction is a coordinate slice on the provided start position.
    select
        gene_windows.gene_symbol,
        gene_windows.ensembl_gene_id,
        'gme' as source_name,
        'GRCh38' as source_build,
        'not_needed' as liftover_status,
        'success' as norm_status,
        '20161025-hg38' as source_version,
        date '2026-03-08' as snapshot_date,
        concat('gme_hg38_raw:', regexp_replace(gme.chrom, r'^chr', ''), ':', cast(gme.start as string), ':', gme.ref, ':', gme.alt) as source_record_key,
        concat(regexp_replace(gme.chrom, r'^chr', ''), ':', cast(gme.start as string), ':', gme.ref, ':', gme.alt) as variant_key,
        gene_windows.chrom38,
        gme.start as pos38,
        gme.ref as ref_norm,
        gme.alt as alt_norm,
        gme.gme_af,
        gme.gme_nwa,
        gme.gme_nea,
        gme.gme_ap,
        gme.gme_israel,
        gme.gme_sd,
        gme.gme_tp,
        gme.gme_ca
    from {{ source('arab_raw', 'gme_hg38_raw') }} as gme
    join gene_windows
      on regexp_replace(gme.chrom, r'^chr', '') = gene_windows.chrom_nochr
     and gme.start between gene_windows.start_pos38 and gene_windows.end_pos38
),
aggregated as (
    select
        gene_symbol,
        ensembl_gene_id,
        source_name,
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
        max(gme_af) as gme_af,
        max(gme_nwa) as gme_nwa,
        max(gme_nea) as gme_nea,
        max(gme_ap) as gme_ap,
        max(gme_israel) as gme_israel,
        max(gme_sd) as gme_sd,
        max(gme_tp) as gme_tp,
        max(gme_ca) as gme_ca
    from source_rows
    group by
        gene_symbol,
        ensembl_gene_id,
        source_name,
        source_build,
        liftover_status,
        norm_status,
        variant_key
)
select
    *,
    'coordinate_window' as extraction_rule
from aggregated
