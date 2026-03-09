{{ config(materialized='view', schema='arab_acmg_harmonized', tags=['t002', 'phase3', 'gnomad', 'genomes']) }}

with raw_union as (
    select
        'gnomad_v4.1' as source_name,
        '4.1' as source_version,
        date '2026-03-03' as snapshot_date,
        'genomes' as cohort,
        'gnomad_v4_1_genomes_chr13_raw' as source_table,
        chrom as chrom_raw,
        pos,
        ref,
        alt,
        qual,
        filter,
        info
    from {{ source('gnomad_raw', 'gnomad_v4_1_genomes_chr13_raw') }}

    union all

    select
        'gnomad_v4.1' as source_name,
        '4.1' as source_version,
        date '2026-03-03' as snapshot_date,
        'genomes' as cohort,
        'gnomad_v4_1_genomes_chr17_raw' as source_table,
        chrom as chrom_raw,
        pos,
        ref,
        alt,
        qual,
        filter,
        info
    from {{ source('gnomad_raw', 'gnomad_v4_1_genomes_chr17_raw') }}
),
expanded as (
    select
        source_name,
        source_version,
        snapshot_date,
        cohort,
        source_table,
        chrom_raw,
        regexp_replace(chrom_raw, r'^chr', '') as chrom_norm,
        pos,
        ref,
        alt_value as alt,
        alt_offset,
        qual,
        filter,
        info
    from raw_union,
    unnest(split(alt, ',')) as alt_value with offset as alt_offset
)
,
parsed as (
    select
        concat(chrom_norm, ':', cast(pos as string), ':', ref, ':', alt) as variant_key,
        source_name,
        source_version,
        snapshot_date,
        cohort,
        source_table,
        chrom_raw,
        chrom_norm,
        pos,
        ref,
        alt,
        alt_offset,
        qual,
        filter,
        safe_cast({{ info_token('info', 'AC', 'alt_offset') }} as int64) as ac,
        safe_cast({{ info_scalar('info', 'AN') }} as int64) as an,
        coalesce(
            safe_cast({{ info_token('info', 'AF', 'alt_offset') }} as float64),
            safe_divide(
                safe_cast({{ info_token('info', 'AC', 'alt_offset') }} as int64),
                nullif(safe_cast({{ info_scalar('info', 'AN') }} as int64), 0)
            )
        ) as af,
        {{ info_scalar('info', 'grpmax') }} as grpmax_population,
        safe_cast({{ info_scalar('info', 'faf95') }} as float64) as grpmax_faf95,
        safe_cast({{ info_scalar('info', 'VarDP') }} as float64) as depth,
        safe_cast({{ info_token('info', 'AC_afr', 'alt_offset') }} as int64) as ac_afr,
        safe_cast({{ info_scalar('info', 'AN_afr') }} as int64) as an_afr,
        coalesce(
            safe_cast({{ info_token('info', 'AF_afr', 'alt_offset') }} as float64),
            safe_divide(
                safe_cast({{ info_token('info', 'AC_afr', 'alt_offset') }} as int64),
                nullif(safe_cast({{ info_scalar('info', 'AN_afr') }} as int64), 0)
            )
        ) as af_afr,
        safe_cast({{ info_token('info', 'AC_nfe', 'alt_offset') }} as int64) as ac_nfe,
        safe_cast({{ info_scalar('info', 'AN_nfe') }} as int64) as an_nfe,
        safe_cast({{ info_token('info', 'AF_nfe', 'alt_offset') }} as float64) as af_nfe,
        safe_cast({{ info_token('info', 'AC_fin', 'alt_offset') }} as int64) as ac_fin,
        safe_cast({{ info_scalar('info', 'AN_fin') }} as int64) as an_fin,
        safe_cast({{ info_token('info', 'AF_fin', 'alt_offset') }} as float64) as af_fin,
        safe_cast({{ info_token('info', 'AC_asj', 'alt_offset') }} as int64) as ac_asj,
        safe_cast({{ info_scalar('info', 'AN_asj') }} as int64) as an_asj,
        safe_cast({{ info_token('info', 'AF_asj', 'alt_offset') }} as float64) as af_asj,
        coalesce(safe_cast({{ info_token('info', 'AC_nfe', 'alt_offset') }} as int64), 0)
          + coalesce(safe_cast({{ info_token('info', 'AC_fin', 'alt_offset') }} as int64), 0)
          + coalesce(safe_cast({{ info_token('info', 'AC_asj', 'alt_offset') }} as int64), 0) as ac_eur_proxy,
        coalesce(safe_cast({{ info_scalar('info', 'AN_nfe') }} as int64), 0)
          + coalesce(safe_cast({{ info_scalar('info', 'AN_fin') }} as int64), 0)
          + coalesce(safe_cast({{ info_scalar('info', 'AN_asj') }} as int64), 0) as an_eur_proxy,
        safe_divide(
            coalesce(safe_cast({{ info_token('info', 'AC_nfe', 'alt_offset') }} as int64), 0)
              + coalesce(safe_cast({{ info_token('info', 'AC_fin', 'alt_offset') }} as int64), 0)
              + coalesce(safe_cast({{ info_token('info', 'AC_asj', 'alt_offset') }} as int64), 0),
            nullif(
                coalesce(safe_cast({{ info_scalar('info', 'AN_nfe') }} as int64), 0)
                  + coalesce(safe_cast({{ info_scalar('info', 'AN_fin') }} as int64), 0)
                  + coalesce(safe_cast({{ info_scalar('info', 'AN_asj') }} as int64), 0),
                0
            )
        ) as af_eur_proxy,
        info as info_raw
    from expanded
    where alt is not null
      and alt != ''
),
final as (
    select
        concat(
            source_table,
            ':',
            variant_key,
            ':',
            cast(
                row_number() over (
                    partition by source_table, variant_key
                    order by info_raw, qual, filter
                ) - 1 as string
            )
        ) as record_key,
        parsed.*
    from parsed
    where coalesce(an, 0) > 0
)
select *
from final
