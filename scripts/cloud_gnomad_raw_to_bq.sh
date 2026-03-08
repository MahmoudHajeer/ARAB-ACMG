#!/usr/bin/env bash
set -euo pipefail

# [AI-Agent: Codex]: Chunked cloud-side gnomAD raw loader for a single source.
# [AI-Agent: Codex]: Each run handles exactly one {cohort, chromosome} pair and
# writes one raw BigQuery table, preserving row content as-is.

: "${PROJECT_ID:?PROJECT_ID is required}"
: "${BUCKET:?BUCKET is required}"
: "${DATASET:?DATASET is required}"
: "${SNAPSHOT_DATE:?SNAPSHOT_DATE is required}"
: "${COHORT:?COHORT is required (genomes|exomes)}"
: "${CHROM:?CHROM is required (chr13|chr17)}"

WINDOW_BP="${WINDOW_BP:-10000000}"
CLEAN_WORKING="${CLEAN_WORKING:-false}"

TAB=$'\t'
SCHEMA='chrom:STRING,pos:INT64,id:STRING,ref:STRING,alt:STRING,qual:STRING,filter:STRING,info:STRING'
WORK_ROOT="/workspace/gnomad_chunk_job_${COHORT}_${CHROM}"
mkdir -p "${WORK_ROOT}"

declare -A CHROM_END=(
  [chr13]=114364328
  [chr17]=83257441
)

declare -A SOURCE_FILE=(
  [genomes,chr13]='gnomad.genomes.v4.1.sites.chr13.vcf.bgz'
  [genomes,chr17]='gnomad.genomes.v4.1.sites.chr17.vcf.bgz'
  [exomes,chr13]='gnomad.exomes.v4.1.sites.chr13.vcf.bgz'
  [exomes,chr17]='gnomad.exomes.v4.1.sites.chr17.vcf.bgz'
)

validate_inputs() {
  if [[ "${COHORT}" != "genomes" && "${COHORT}" != "exomes" ]]; then
    echo "Unsupported COHORT=${COHORT}. Expected genomes or exomes." >&2
    exit 1
  fi
  if [[ "${CHROM}" != "chr13" && "${CHROM}" != "chr17" ]]; then
    echo "Unsupported CHROM=${CHROM}. Expected chr13 or chr17." >&2
    exit 1
  fi
}

source_uri() {
  local key="${COHORT},${CHROM}"
  local file_name="${SOURCE_FILE[${key}]}"
  printf 'gs://%s/raw/sources/gnomad_v4.1/release=4.1/cohort=%s/chrom=%s/snapshot_date=%s/%s' \
    "${BUCKET}" "${COHORT}" "${CHROM}" "${SNAPSHOT_DATE}" "${file_name}"
}

chunk_prefix() {
  printf 'gs://%s/raw/working/gnomad_v4.1/release=4.1/cohort=%s/chrom=%s/snapshot_date=%s/chunks_%sbp' \
    "${BUCKET}" "${COHORT}" "${CHROM}" "${SNAPSHOT_DATE}" "${WINDOW_BP}"
}

prepare_chunks_for_source() {
  local key="${COHORT},${CHROM}"
  local file_name="${SOURCE_FILE[${key}]}"
  local src_uri
  local idx_uri
  local local_dir
  local local_vcf
  local local_tbi

  src_uri="$(source_uri)"
  idx_uri="${src_uri}.tbi"
  local_dir="${WORK_ROOT}/${COHORT}_${CHROM}"
  local_vcf="${local_dir}/${file_name}"
  local_tbi="${local_vcf}.tbi"

  mkdir -p "${local_dir}"

  # [AI-Agent: Codex]: Step 1: Download untouched raw source snapshot + index.
  echo "[cloud-job] Downloading raw snapshot ${src_uri}"
  gsutil cp "${src_uri}" "${local_vcf}"
  gsutil cp "${idx_uri}" "${local_tbi}"

  # [AI-Agent: Codex]: Step 2: Count VCF header lines once for BigQuery load.
  local header_rows
  header_rows="$(bcftools view -h "${local_vcf}" | wc -l | awk '{print $1}')"
  printf '%s\n' "${header_rows}" > "${WORK_ROOT}/header_rows.txt"
  echo "[cloud-job] Header rows ${COHORT} ${CHROM}: ${header_rows}"

  # [AI-Agent: Codex]: Step 3: Chunk by genomic windows to bypass BQ 4GiB
  # limit on non-splittable compressed CSV objects.
  local chrom_end="${CHROM_END[${CHROM}]}"
  local part=1
  local chunk_uri
  local part_num
  local start
  local end

  for ((start=1; start<=chrom_end; start+=WINDOW_BP)); do
    end=$((start + WINDOW_BP - 1))
    if ((end > chrom_end)); then
      end="${chrom_end}"
    fi

    part_num="$(printf '%03d' "${part}")"
    chunk_uri="$(chunk_prefix)/part_${part_num}_${start}_${end}.vcf.bgz"

    if gsutil -q stat "${chunk_uri}"; then
      echo "[cloud-job] Reusing chunk ${chunk_uri}"
    else
      echo "[cloud-job] Building chunk ${COHORT} ${CHROM} ${start}-${end}"
      bcftools view -r "${CHROM}:${start}-${end}" -Oz -o "${local_dir}/chunk.vcf.bgz" "${local_vcf}"
      gsutil cp "${local_dir}/chunk.vcf.bgz" "${chunk_uri}"
    fi

    part=$((part + 1))
  done

  rm -rf "${local_dir}"
}

load_single_table() {
  # [AI-Agent: Codex]: Step 4: Load chunks into one dedicated raw table.
  local table_id="${DATASET}.gnomad_v4_1_${COHORT}_${CHROM}_raw"
  local table_ref="${PROJECT_ID}:${table_id}"
  local header_rows
  local chunk_glob

  bq --project_id="${PROJECT_ID}" rm -f -t "${table_ref}" || true

  if [[ ! -f "${WORK_ROOT}/header_rows.txt" ]]; then
    echo "[cloud-job] Missing header rows metadata for ${COHORT} ${CHROM}" >&2
    exit 1
  fi
  header_rows="$(cat "${WORK_ROOT}/header_rows.txt")"
  chunk_glob="$(chunk_prefix)/part_*.vcf.bgz"

  echo "[cloud-job] Loading ${table_ref} from ${chunk_glob}"
  bq --project_id="${PROJECT_ID}" load \
    --replace \
    --source_format=CSV \
    --field_delimiter="${TAB}" \
    --skip_leading_rows="${header_rows}" \
    "${table_id}" \
    "${chunk_glob}" \
    "${SCHEMA}"

  bq --project_id="${PROJECT_ID}" query --nouse_legacy_sql \
    "SELECT '${table_id}' AS table_name, COUNT(*) AS row_count FROM \`${PROJECT_ID}.${table_id}\`"
}

validate_inputs
prepare_chunks_for_source
load_single_table

# [AI-Agent: Codex]: Step 5 (optional): remove per-job working chunks.
if [[ "${CLEAN_WORKING}" == "true" ]]; then
  echo "[cloud-job] Cleaning working chunks for ${COHORT} ${CHROM}"
  gsutil -m rm -r "$(chunk_prefix)/**" || true
fi

echo "[cloud-job] gnomAD raw BigQuery load completed for ${COHORT} ${CHROM}."
