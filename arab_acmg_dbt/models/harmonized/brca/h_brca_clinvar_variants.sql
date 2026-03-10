{{ config(materialized='table', schema='arab_acmg_harmonized', tags=['t003', 'brca', 'clinvar']) }}

with gene_windows as (
    select *
    from {{ ref('h_brca_gene_windows') }}
),
source_rows as (
    -- [AI-Agent: Codex]: ClinVar already arrives on GRCh38 in this workspace, so the
    -- harmonization step only needs coordinate scoping plus an audit check against GENEINFO.
    select
        gene_windows.gene_symbol,
        gene_windows.ensembl_gene_id,
        'clinvar' as source_name,
        'GRCh38' as source_build,
        'not_needed' as liftover_status,
        'success' as norm_status,
        clinvar.source_version,
        clinvar.snapshot_date,
        clinvar.record_key as source_record_key,
        clinvar.variant_key,
        gene_windows.chrom38,
        clinvar.pos as pos38,
        clinvar.ref as ref_norm,
        clinvar.alt as alt_norm,
        clinvar.clinvar_id,
        clinvar.clinvar_significance,
        clinvar.clinvar_review_status,
        clinvar.gene_info,
        regexp_contains(
            coalesce(clinvar.gene_info, ''),
            concat(r'(^|\|)', gene_windows.gene_symbol, ':')
        ) as gene_info_matches_target,
        clinvar.info_raw
    from {{ ref('stg_clinvar_variants') }} as clinvar
    join gene_windows
      on clinvar.chrom_norm = gene_windows.chrom_nochr
     and clinvar.pos between gene_windows.start_pos38 and gene_windows.end_pos38
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
        count(*) as clinvar_record_count,
        string_agg(distinct cast(clinvar_id as string), ' | ' order by cast(clinvar_id as string)) as clinvar_ids,
        string_agg(distinct clinvar_significance, ' | ' order by clinvar_significance) as clinvar_significance_values,
        string_agg(distinct clinvar_review_status, ' | ' order by clinvar_review_status) as clinvar_review_status_values,
        countif(gene_info_matches_target) as gene_info_match_count,
        countif(not gene_info_matches_target) as gene_info_mismatch_count,
        string_agg(distinct source_record_key, ' | ' order by source_record_key) as source_record_keys
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
    case
        when gene_info_mismatch_count = 0 then 'coordinate_window_and_gene_info'
        else 'coordinate_window_primary_gene_info_review_needed'
    end as extraction_rule
from aggregated
