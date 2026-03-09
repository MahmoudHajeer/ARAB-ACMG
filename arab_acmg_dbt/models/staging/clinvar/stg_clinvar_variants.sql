{{ config(materialized='view', schema='arab_acmg_harmonized', tags=['t002', 'phase2', 'clinvar']) }}

with source_rows as (
    select
        'clinvar' as source_name,
        'lastmod-20260302' as source_version,
        date '2026-03-03' as snapshot_date,
        'clinvar_raw_vcf' as source_table,
        chrom as chrom_raw,
        regexp_replace(chrom, r'^chr', '') as chrom_norm,
        pos,
        id as clinvar_id,
        ref,
        alt,
        qual,
        filter,
        info
    from {{ source('clinvar_raw', 'clinvar_raw_vcf') }}
),
expanded as (
    select
        source_name,
        source_version,
        snapshot_date,
        source_table,
        chrom_raw,
        chrom_norm,
        pos,
        clinvar_id,
        ref,
        alt_value as alt,
        alt_offset,
        qual,
        filter,
        info
    from source_rows,
    unnest(split(alt, ',')) as alt_value with offset as alt_offset
)
select
    concat(source_table, ':', chrom_norm, ':', cast(pos as string), ':', ref, ':', alt) as record_key,
    concat(chrom_norm, ':', cast(pos as string), ':', ref, ':', alt) as variant_key,
    source_name,
    source_version,
    snapshot_date,
    source_table,
    chrom_raw,
    chrom_norm,
    pos,
    clinvar_id,
    ref,
    alt,
    alt_offset,
    qual,
    filter,
    {{ info_scalar('info', 'ALLELEID') }} as allele_id,
    {{ info_scalar('info', 'CLNSIG') }} as clinvar_significance,
    {{ info_scalar('info', 'CLNREVSTAT') }} as clinvar_review_status,
    {{ info_scalar('info', 'CLNDN') }} as condition_name,
    {{ info_scalar('info', 'GENEINFO') }} as gene_info,
    {{ info_scalar('info', 'ORIGIN') }} as origin_code,
    info as info_raw
from expanded
where alt is not null
  and alt not in ('', '.', '*')
