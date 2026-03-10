{{ config(materialized='table', schema='arab_acmg_harmonized', tags=['t003', 'brca', 'reference']) }}

-- [AI-Agent: Codex]: Read BRCA windows from a frozen seed artifact instead of embedding
-- ad-hoc constants in SQL, so the target definition is versioned and source-backed.
select
    cast(gene_symbol as string) as gene_symbol,
    cast(ensembl_gene_id as string) as ensembl_gene_id,
    cast(chrom38 as string) as chrom38,
    cast(chrom_nochr as string) as chrom_nochr,
    cast(start_pos38 as int64) as start_pos38,
    cast(end_pos38 as int64) as end_pos38,
    cast(coordinate_source as string) as coordinate_source,
    cast(coordinate_source_url as string) as coordinate_source_url,
    cast(accessed_at as date) as accessed_at,
    cast(coordinate_note as string) as coordinate_note
from {{ ref('brca_gene_windows_seed') }}
