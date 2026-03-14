"""Build frozen BRCA normalized artifacts from the raw source packages.

This script is the first real T003 normalization pass. It keeps the workflow
explicit and cheap:
1. Read the frozen source manifests so every output is tied to a known source package.
2. Fetch only the BRCA1/BRCA2 windows from the public VCF sources.
3. Convert table-style Arab frequency sources into minimal VCF rows with explicit raw locators.
4. Normalize every ready source with `bcftools norm` against a local GRCh38 chr13/chr17 reference.
5. Persist per-source normalized Parquet snapshots, a normalization report, and two checkpoint artifacts:
   - pre-GME Arab checkpoint: ClinVar + gnomAD + SHGP
   - final Arab checkpoint: pre-GME + GME
6. Refresh the frozen supervisor UI bundle so the review surface stays static.
7. Re-compose the supervisor bundle so the legacy baseline stays separate from the Arab extension review.

The script does not use BigQuery and it does not hide intermediate logic behind
runtime queries. Every displayed number comes from the artifacts written here.
"""

from __future__ import annotations

import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Iterable

import pandas as pd
from google.cloud import storage

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
UI_DIR: Final[Path] = ROOT / "ui"
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
SNAPSHOT_DATE: Final[str] = dt.date.today().isoformat()
RUN_ID: Final[str] = f"brca-normalize-{dt.datetime.now(dt.UTC).strftime('%Y%m%dT%H%M%SZ')}"
TIMESTAMP_UTC: Final[str] = dt.datetime.now(dt.UTC).isoformat()

BRCA_WINDOWS: Final[dict[str, dict[str, int | str]]] = {
    "BRCA1": {"chrom": "chr17", "start": 43044295, "end": 43170245},
    "BRCA2": {"chrom": "chr13", "start": 32315086, "end": 32400268},
}

REQUIRED_COLUMNS: Final[list[str]] = [
    "CHROM",
    "POS",
    "END",
    "ID",
    "REF",
    "ALT",
    "VARTYPE",
    "Repeat",
    "Segdup",
    "Blacklist",
    "GENE",
    "EFFECT",
    "HGVS_C",
    "HGVS_P",
    "PHENOTYPES_OMIM",
    "PHENOTYPES_OMIM_ID",
    "INHERITANCE_PATTERN",
    "ALLELEID",
    "CLNSIG",
    "TOPMED_AF",
    "TOPMed_Hom",
    "ALFA_AF",
    "ALFA_Hom",
    "GNOMAD_ALL_AF",
    "gnomAD_all_Hom",
    "GNOMAD_MID_AF",
    "gnomAD_mid_Hom",
    "ONEKGP_AF",
    "REGENERON_AF",
    "TGP_AF",
    "QATARI",
    "JGP_AF",
    "JGP_MAF",
    "JGP_Hom",
    "JGP_Het",
    "JGP_AC_Hemi",
    "SIFT_PRED",
    "POLYPHEN2_HDIV_PRED",
    "POLYPHEN2_HVAR_PRED",
    "PROVEAN_PRE",
]

EXTRA_COLUMNS: Final[list[str]] = [
    "VARIANT_KEY",
    "CLNREVSTAT",
    "GNOMAD_GENOMES_AC",
    "GNOMAD_GENOMES_AN",
    "GNOMAD_GENOMES_AF",
    "GNOMAD_GENOMES_HOM",
    "GNOMAD_EXOMES_AC",
    "GNOMAD_EXOMES_AN",
    "GNOMAD_EXOMES_AF",
    "GNOMAD_EXOMES_HOM",
    "SHGP_AC",
    "SHGP_AN",
    "SHGP_AF",
    "GME_AF",
    "GME_NWA",
    "GME_NEA",
    "GME_AP",
    "GME_SD",
    "PRESENT_IN_CLINVAR",
    "PRESENT_IN_GNOMAD_GENOMES",
    "PRESENT_IN_GNOMAD_EXOMES",
    "PRESENT_IN_SHGP",
    "PRESENT_IN_GME",
    "SOURCE_COUNT",
    "PIPELINE_STAGE",
]

PUBLIC_FINAL_CSV_OBJECT: Final[str] = (
    f"frozen/results/checkpoint=supervisor_variant_registry_brca_arab_v2/snapshot_date={SNAPSHOT_DATE}/"
    "supervisor_variant_registry_brca_arab_v2.csv"
)

REFERENCE_URLS: Final[dict[str, str]] = {
    "chr13": "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/chromosomes/chr13.fa.gz",
    "chr17": "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/chromosomes/chr17.fa.gz",
}

CLINVAR_MANIFEST_URI: Final[str] = (
    "gs://mahmoud-arab-acmg-research-data/raw/sources/clinvar/lastmod-20260302/"
    "snapshot_date=2026-03-03/manifest.json"
)
CLINVAR_PUBLIC_URL: Final[str] = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz"
GNOMAD_SOURCE_URIS: Final[dict[str, dict[str, str]]] = {
    "genomes_chr13": {
        "public_url": "https://storage.googleapis.com/gcp-public-data--gnomad/release/4.1/vcf/genomes/gnomad.genomes.v4.1.sites.chr13.vcf.bgz",
        "raw_uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=genomes/chrom=chr13/snapshot_date=2026-03-03/gnomad.genomes.v4.1.sites.chr13.vcf.bgz",
    },
    "genomes_chr17": {
        "public_url": "https://storage.googleapis.com/gcp-public-data--gnomad/release/4.1/vcf/genomes/gnomad.genomes.v4.1.sites.chr17.vcf.bgz",
        "raw_uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=genomes/chrom=chr17/snapshot_date=2026-03-03/gnomad.genomes.v4.1.sites.chr17.vcf.bgz",
    },
    "exomes_chr13": {
        "public_url": "https://storage.googleapis.com/gcp-public-data--gnomad/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr13.vcf.bgz",
        "raw_uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=exomes/chrom=chr13/snapshot_date=2026-03-03/gnomad.exomes.v4.1.sites.chr13.vcf.bgz",
    },
    "exomes_chr17": {
        "public_url": "https://storage.googleapis.com/gcp-public-data--gnomad/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr17.vcf.bgz",
        "raw_uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=exomes/chrom=chr17/snapshot_date=2026-03-03/gnomad.exomes.v4.1.sites.chr17.vcf.bgz",
    },
}
SHGP_MANIFEST_URI: Final[str] = (
    "gs://mahmoud-arab-acmg-research-data/raw/sources/shgp_saudi_af/version=figshare-28059686-v1/"
    "snapshot_date=2026-03-13/manifest.json"
)
SHGP_LOCAL_FILE: Final[Path] = Path("/Users/macbookpro/Desktop/storage/raw/shgp/Saudi_Arabian_Allele_Frequencies.txt")
GME_MANIFEST_URI: Final[str] = (
    "gs://mahmoud-arab-acmg-research-data/raw/sources/gme/release=20161025-hg38/build=hg38/"
    "snapshot_date=2026-03-08/manifest.json"
)
GME_LOCAL_FILE: Final[Path] = Path("/Users/macbookpro/Desktop/storage/raw/gme/hg38_gme.txt.gz")

OLD_REC_TAG: Final[str] = "OLD_REC"
QUERY_FORMATS: Final[dict[str, str]] = {
    "clinvar": "%CHROM\t%POS\t%ID\t%REF\t%ALT\t%INFO/OLD_REC\t%INFO/ALLELEID\t%INFO/CLNSIG\t%INFO/CLNREVSTAT\t%INFO/GENEINFO\t%INFO/MC\t%INFO/CLNVC\t%INFO/CLNHGVS\t%INFO/CLNDN\t%INFO/CLNDISDB\n",
    "gnomad": "%CHROM\t%POS\t%ID\t%REF\t%ALT\t%INFO/OLD_REC\t%INFO/AC\t%INFO/AN\t%INFO/AF\t%INFO/nhomalt\t%INFO/AC_mid\t%INFO/AN_mid\t%INFO/AF_mid\t%INFO/nhomalt_mid\t%INFO/AC_afr\t%INFO/AF_afr\n",
    "shgp": "%CHROM\t%POS\t%ID\t%REF\t%ALT\t%INFO/OLD_REC\t%INFO/SRC_ROW\t%INFO/SRC_LOC\t%INFO/SHGP_AC\t%INFO/SHGP_AN\t%INFO/SHGP_AF\n",
    "gme": "%CHROM\t%POS\t%ID\t%REF\t%ALT\t%INFO/OLD_REC\t%INFO/SRC_ROW\t%INFO/SRC_LOC\t%INFO/GME_AF\t%INFO/GME_NWA\t%INFO/GME_NEA\t%INFO/GME_AP\t%INFO/GME_ISRAEL\t%INFO/GME_SD\t%INFO/GME_TP\t%INFO/GME_CA\n",
}


@dataclass(frozen=True)
class SourceArtifact:
    key: str
    display_name: str
    source_kind: str
    source_version: str
    source_build: str
    snapshot_date: str
    upstream_url: str
    source_artifact_uri: str
    source_artifact_sha256: str
    manifest_uri: str
    row_count: int | None
    notes: str


@dataclass(frozen=True)
class WorkflowArtifact:
    key: str
    title: str
    stage: str
    storage_uri: str
    row_count: int
    local_parquet: Path
    local_manifest: Path
    sample: dict[str, Any]
    columns: list[dict[str, str]]
    summary: str
    notes: list[str]


def run(cmd: list[str], *, stdout_path: Path | None = None) -> str:
    result = subprocess.run(cmd, check=True, capture_output=stdout_path is None, text=stdout_path is None)
    if stdout_path is not None:
        stdout_path.write_bytes(result.stdout if isinstance(result.stdout, bytes) else b"")
        return ""
    return result.stdout


def run_pipe(command: str) -> None:
    subprocess.run(["bash", "-lc", command], check=True)


def command_output(cmd: list[str]) -> str:
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout.strip()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_dump(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def parse_gs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Unsupported GCS URI: {uri}")
    bucket, object_name = uri[5:].split("/", 1)
    return bucket, object_name


def download_gcs_json(storage_client: storage.Client, uri: str) -> dict[str, Any]:
    bucket_name, object_name = parse_gs_uri(uri)
    blob = storage_client.bucket(bucket_name).blob(object_name)
    return json.loads(blob.download_as_text())


def upload_file(
    storage_client: storage.Client,
    local_path: Path,
    object_name: str,
    *,
    content_type: str,
    make_public: bool = False,
) -> str:
    blob = storage_client.bucket(BUCKET_NAME).blob(object_name)
    blob.upload_from_filename(str(local_path), content_type=content_type)
    if make_public:
        blob.make_public()
    return f"gs://{BUCKET_NAME}/{object_name}"


def upload_text(storage_client: storage.Client, object_name: str, content: str, *, content_type: str) -> str:
    blob = storage_client.bucket(BUCKET_NAME).blob(object_name)
    blob.upload_from_string(content.encode("utf-8"), content_type=content_type)
    return f"gs://{BUCKET_NAME}/{object_name}"


def public_object_url(object_name: str) -> str:
    return f"https://storage.googleapis.com/{BUCKET_NAME}/{object_name}"


def variant_type(ref: str, alt: str) -> str:
    if len(ref) == len(alt) == 1:
        return "SNV"
    if len(ref) == len(alt) and len(ref) > 1:
        return "MNV"
    if len(ref) < len(alt):
        return "INS"
    if len(ref) > len(alt):
        return "DEL"
    return "COMPLEX"


def infer_gene(chrom: str, pos: int) -> str | None:
    for gene, window in BRCA_WINDOWS.items():
        if chrom == window["chrom"] and int(window["start"]) <= pos <= int(window["end"]):
            return gene
    return None


def build_header_glossary(required: Iterable[tuple[str, str]], extras: Iterable[tuple[str, str]]) -> list[dict[str, str]]:
    required_names = {name for name, _ in required}
    rows = []
    for name, description in list(required) + list(extras):
        rows.append({
            "name": name,
            "description": description,
            "kind": "required" if name in required_names else "extra",
        })
    return rows


def compact_rows(frame: pd.DataFrame, *, limit: int = 10) -> dict[str, Any]:
    preview = frame.head(limit).copy()
    preview.insert(0, "sample_row_number", range(1, len(preview) + 1))
    preview = preview.where(pd.notnull(preview), None)
    return {
        "columns": list(preview.columns),
        "rows": preview.to_dict(orient="records"),
        "mode": "frozen bundle",
        "frozen_at": SNAPSHOT_DATE,
        "query_sql": "Frozen artifact preview from the approved BRCA normalization bundle; no live analytical query is executed at review time.",
    }


def curl_head(url: str) -> dict[str, str]:
    output = command_output(["curl", "-I", "-sS", url])
    headers: dict[str, str] = {}
    for line in output.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip().lower()] = value.strip()
    return headers


def download_reference_fasta(workdir: Path) -> tuple[Path, dict[str, Any]]:
    parts: list[Path] = []
    reference_notes: list[dict[str, str]] = []
    for chrom, url in REFERENCE_URLS.items():
        gz_path = workdir / f"{chrom}.fa.gz"
        fa_path = workdir / f"{chrom}.fa"
        run_pipe(f"curl -sSL {url!s} -o {gz_path!s}")
        run_pipe(f"gzip -dc {gz_path!s} > {fa_path!s}")
        parts.append(fa_path)
        head = curl_head(url)
        reference_notes.append(
            {
                "chrom": chrom,
                "url": url,
                "last_modified": head.get("last-modified", "not recorded"),
                "etag": head.get("etag", "not recorded"),
            }
        )
    combined = workdir / "brca_reference_chr13_chr17.fa"
    with combined.open("wb") as output:
        for part in parts:
            output.write(part.read_bytes())
    run(["samtools", "faidx", str(combined)])
    return combined, {"reference_sources": reference_notes, "sha256": sha256_file(combined)}


def load_reference_sequences(fasta_path: Path) -> dict[str, str]:
    sequences: dict[str, list[str]] = {}
    current: str | None = None
    for line in fasta_path.read_text(encoding="utf-8").splitlines():
        if line.startswith(">"):
            current = line[1:].split()[0]
            sequences[current] = []
            continue
        if current is None:
            continue
        sequences[current].append(line.strip())
    return {chrom: "".join(parts) for chrom, parts in sequences.items()}


def previous_base(reference: dict[str, str], chrom: str, pos_1based: int) -> str:
    return reference[chrom][pos_1based - 2]


def convert_table_variant(reference: dict[str, str], chrom: str, start: int, end: int, ref: str, alt: str) -> tuple[int, str, str]:
    if ref == "-":
        anchor = previous_base(reference, chrom, start)
        return start - 1, anchor, anchor + alt
    if alt == "-":
        anchor = previous_base(reference, chrom, start)
        return start - 1, anchor + ref, anchor
    return start, ref, alt


def parse_info_pairs(text: str | None) -> dict[str, str]:
    if not text or text == ".":
        return {}
    pairs: dict[str, str] = {}
    for part in text.split(";"):
        if not part:
            continue
        if "=" not in part:
            pairs[part] = ""
            continue
        key, value = part.split("=", 1)
        pairs[key] = value
    return pairs


def parse_clinvar_omim_pairs(clndn: str | None, clndisdb: str | None) -> tuple[str | None, str | None]:
    if not clndn or not clndisdb:
        return None, None
    phenotype_values = [item.strip() for item in clndn.split("|")]
    db_values = [item.strip() for item in clndisdb.split("|")]
    omim_names: list[str] = []
    omim_ids: list[str] = []
    for phenotype, db_text in zip(phenotype_values, db_values):
        ids = [token.split(":", 1)[1] for token in db_text.split(",") if token.startswith("OMIM:")]
        if not ids:
            continue
        omim_names.append(phenotype)
        omim_ids.extend(ids)
    return (
        " | ".join(sorted(dict.fromkeys(name for name in omim_names if name and name != "."))) or None,
        " | ".join(sorted(dict.fromkeys(omim_ids))) or None,
    )


def parse_geneinfo(value: str | None) -> str | None:
    if not value or value == ".":
        return None
    first = value.split("|")[0]
    return first.split(":", 1)[0] if ":" in first else first


def parse_effect(value: str | None) -> str | None:
    if not value or value == ".":
        return None
    effects = []
    for part in value.split(","):
        if "|" in part:
            effects.append(part.split("|", 1)[1])
        else:
            effects.append(part)
    unique = [item for item in dict.fromkeys(effect.strip() for effect in effects if effect.strip() and effect.strip() != ".")]
    return " | ".join(unique) or None


def artifact_sha_from_manifest(manifest: dict[str, Any]) -> str:
    for key in ("sha256", "local_sha256"):
        value = manifest.get(key)
        if value:
            return str(value)
    md5_value = manifest.get("local_md5") or manifest.get("upstream_md5")
    if md5_value:
        return f"sha256_not_available;md5={md5_value}"
    return "sha256_not_available"


def parse_source_artifact(manifest: dict[str, Any], *, key: str, display_name: str, source_kind: str, source_build: str, manifest_uri: str) -> SourceArtifact:
    return SourceArtifact(
        key=key,
        display_name=display_name,
        source_kind=source_kind,
        source_version=str(manifest.get("source_version", "unknown")),
        source_build=source_build,
        snapshot_date=str(manifest.get("snapshot_date", SNAPSHOT_DATE)),
        upstream_url=str(manifest.get("upstream_url", "not recorded")),
        source_artifact_uri=str(manifest.get("gcs_uri", "not recorded")),
        source_artifact_sha256=artifact_sha_from_manifest(manifest),
        manifest_uri=manifest_uri,
        row_count=int(manifest["row_count"]) if manifest.get("row_count") not in (None, "", -1) else None,
        notes=str(manifest.get("notes", "")),
    )


def count_vcf_records(vcf_path: Path) -> int:
    count = 0
    with vcf_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.startswith("#"):
                count += 1
    return count


def write_table_vcf(
    *,
    source_key: str,
    source_rows: list[dict[str, Any]],
    reference: dict[str, str],
    output_path: Path,
    extra_header_lines: list[str],
    info_builder: callable,
) -> dict[str, int]:
    contig_lengths = {chrom: len(sequence) for chrom, sequence in reference.items()}
    with output_path.open("w", encoding="utf-8") as handle:
        handle.write("##fileformat=VCFv4.2\n")
        handle.write(f"##source={source_key}\n")
        for chrom, length in contig_lengths.items():
            handle.write(f"##contig=<ID={chrom},length={length}>\n")
        handle.write('##INFO=<ID=SRC_ROW,Number=1,Type=Integer,Description="Original source row number after header rows">\n')
        handle.write('##INFO=<ID=SRC_LOC,Number=1,Type=String,Description="Stable source record locator">\n')
        handle.write('##INFO=<ID=SRC_START,Number=1,Type=Integer,Description="Original source start coordinate">\n')
        handle.write('##INFO=<ID=SRC_END,Number=1,Type=Integer,Description="Original source end coordinate">\n')
        handle.write('##INFO=<ID=SRC_REF,Number=1,Type=String,Description="Original source ref token before VCF conversion">\n')
        handle.write('##INFO=<ID=SRC_ALT,Number=1,Type=String,Description="Original source alt token before VCF conversion">\n')
        for line in extra_header_lines:
            handle.write(f"{line}\n")
        handle.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")

        for row in source_rows:
            pos, ref_vcf, alt_vcf = convert_table_variant(
                reference,
                str(row["chrom38"]),
                int(row["start"]),
                int(row["end"]),
                str(row["ref"]),
                str(row["alt"]),
            )
            safe_locator = str(row["source_record_locator"]).replace(";", "|").replace(" ", "_")
            info_parts = [
                f"SRC_ROW={int(row['source_row_number'])}",
                f"SRC_LOC={safe_locator}",
                f"SRC_START={int(row['start'])}",
                f"SRC_END={int(row['end'])}",
                f"SRC_REF={row['ref']}",
                f"SRC_ALT={row['alt']}",
            ]
            info_parts.extend(info_builder(row))
            handle.write(
                "\t".join(
                    [
                        str(row["chrom38"]),
                        str(pos),
                        str(row["source_id"]),
                        ref_vcf,
                        alt_vcf,
                        ".",
                        ".",
                        ";".join(info_parts),
                    ]
                )
                + "\n"
            )
    return {"input_rows": len(source_rows)}


def query_vcf_to_frame(vcf_path: Path, query_format: str, columns: list[str]) -> pd.DataFrame:
    output = command_output(["bcftools", "query", "-f", query_format, str(vcf_path)])
    if not output:
        return pd.DataFrame(columns=columns)
    return pd.read_csv(io.StringIO(output), sep="\t", names=columns, dtype=str).fillna("")


def uri_prefix(uri: str) -> str:
    return uri.rsplit("/", 1)[0] + "/"


def normalize_public_vcf_source(
    *,
    temp_dir: Path,
    source: SourceArtifact,
    public_url: str,
    rename_map_path: Path | None,
    regions: list[str],
    reference_fasta: Path,
    query_format: str,
    query_columns: list[str],
    parser: callable,
) -> tuple[pd.DataFrame, dict[str, Any], pd.DataFrame]:
    raw_vcf = temp_dir / f"{source.key}_raw_subset.vcf"
    working_vcf = temp_dir / f"{source.key}_working.vcf"
    normalized_vcf = temp_dir / f"{source.key}_normalized.vcf"

    run(["bcftools", "view", "-r", ",".join(regions), "-Ov", "-o", str(raw_vcf), public_url])
    if rename_map_path is not None:
        run(["bcftools", "annotate", "--rename-chrs", str(rename_map_path), "-Ov", "-o", str(working_vcf), str(raw_vcf)])
    else:
        working_vcf.write_text(raw_vcf.read_text(encoding="utf-8"), encoding="utf-8")
    run(["bcftools", "norm", "-f", str(reference_fasta), "-m", "-any", "--old-rec-tag", OLD_REC_TAG, "-Ov", "-o", str(normalized_vcf), str(working_vcf)])

    before_count = count_vcf_records(working_vcf)
    raw_preview = query_vcf_to_frame(raw_vcf, "%CHROM\t%POS\t%ID\t%REF\t%ALT\n", ["CHROM", "POS", "ID", "REF", "ALT"])
    queried = query_vcf_to_frame(normalized_vcf, query_format, query_columns)
    frame = parser(queried, source)
    duplicate_rows = int(frame.duplicated(subset=["variant_key"]).sum()) if not frame.empty else 0
    report = {
        "source_key": source.key,
        "display_name": source.display_name,
        "source_artifact_uri": source.source_artifact_uri,
        "source_artifact_sha256": source.source_artifact_sha256,
        "public_extraction_url": public_url,
        "region_filters": regions,
        "source_rows_before_normalization": before_count,
        "normalized_rows": int(len(frame)),
        "duplicate_variant_keys": duplicate_rows,
        "normalization_tool": "bcftools norm",
        "normalization_tool_version": command_output(["bcftools", "--version"]).splitlines()[0],
        "notes": [
            "BRCA windows were fetched directly from the provider VCF to avoid localizing the full source archive.",
            f"Per-row provenance still points back to the frozen raw source artifact: {source.source_artifact_uri}",
        ],
    }
    return frame, report, raw_preview


def build_shgp_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    data_row = 0
    with SHGP_LOCAL_FILE.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) != 6:
                continue
            data_row += 1
            chrom, pos_text, ref, alt, an_text, ac_text = parts
            pos = int(pos_text)
            gene = infer_gene(chrom, pos)
            if gene is None:
                continue
            rows.append(
                {
                    "chrom38": chrom,
                    "start": pos,
                    "end": pos + len(ref.replace("-", "")) - 1 if ref != "-" else pos,
                    "ref": ref,
                    "alt": alt,
                    "source_row_number": data_row,
                    "source_record_locator": f"table=Saudi_Arabian_Allele_Frequencies.txt;row={data_row}",
                    "source_id": f"SHGP:{data_row}",
                    "shgp_ac": int(ac_text),
                    "shgp_an": int(an_text),
                    "shgp_af": float(ac_text) / float(an_text) if float(an_text) else None,
                    "gene": gene,
                }
            )
    return rows


def build_gme_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    data_row = 0
    with gzip.open(GME_LOCAL_FILE, "rt", encoding="utf-8") as handle:
        header = next(handle)
        for line in handle:
            data_row += 1
            chrom, start_text, end_text, ref, alt, gme_af, gme_nwa, gme_nea, gme_ap, gme_israel, gme_sd, gme_tp, gme_ca = line.rstrip("\n").split("\t")
            chrom38 = f"chr{chrom}"
            start = int(start_text)
            gene = infer_gene(chrom38, start)
            if gene is None:
                continue
            rows.append(
                {
                    "chrom38": chrom38,
                    "start": start,
                    "end": int(end_text),
                    "ref": ref,
                    "alt": alt,
                    "source_row_number": data_row,
                    "source_record_locator": f"table=hg38_gme.txt.gz;row={data_row}",
                    "source_id": f"GME:{data_row}",
                    "gme_af": float(gme_af),
                    "gme_nwa": float(gme_nwa),
                    "gme_nea": float(gme_nea),
                    "gme_ap": float(gme_ap),
                    "gme_israel": float(gme_israel),
                    "gme_sd": float(gme_sd),
                    "gme_tp": float(gme_tp),
                    "gme_ca": float(gme_ca),
                    "gene": gene,
                }
            )
    return rows


def normalize_table_source(
    *,
    temp_dir: Path,
    source: SourceArtifact,
    source_rows: list[dict[str, Any]],
    reference: dict[str, str],
    reference_fasta: Path,
    extra_header_lines: list[str],
    info_builder: callable,
    query_format: str,
    query_columns: list[str],
    parser: callable,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    raw_vcf = temp_dir / f"{source.key}_raw_table_subset.vcf"
    normalized_vcf = temp_dir / f"{source.key}_normalized.vcf"
    write_table_vcf(
        source_key=source.key,
        source_rows=source_rows,
        reference=reference,
        output_path=raw_vcf,
        extra_header_lines=extra_header_lines,
        info_builder=info_builder,
    )
    run(["bcftools", "norm", "-f", str(reference_fasta), "-m", "-any", "--old-rec-tag", OLD_REC_TAG, "-Ov", "-o", str(normalized_vcf), str(raw_vcf)])
    queried = query_vcf_to_frame(normalized_vcf, query_format, query_columns)
    frame = parser(queried, source)
    duplicate_rows = int(frame.duplicated(subset=["variant_key"]).sum()) if not frame.empty else 0
    report = {
        "source_key": source.key,
        "display_name": source.display_name,
        "source_artifact_uri": source.source_artifact_uri,
        "source_artifact_sha256": source.source_artifact_sha256,
        "source_rows_before_normalization": len(source_rows),
        "normalized_rows": int(len(frame)),
        "duplicate_variant_keys": duplicate_rows,
        "normalization_tool": "bcftools norm",
        "normalization_tool_version": command_output(["bcftools", "--version"]).splitlines()[0],
        "notes": [
            "The table-style source was converted to a minimal VCF with explicit SRC_* provenance tags before normalization.",
            "Deletion rows that used '-' were converted to legal VCF form by anchoring on the previous GRCh38 reference base.",
        ],
    }
    return frame, report


def base_metadata_frame(frame: pd.DataFrame, source: SourceArtifact) -> pd.DataFrame:
    if frame.empty:
        return frame
    enriched = frame.copy()
    enriched["source"] = source.key
    enriched["source_version"] = source.source_version
    enriched["snapshot_date"] = source.snapshot_date
    enriched["source_build"] = source.source_build
    enriched["source_artifact_uri"] = source.source_artifact_uri
    enriched["source_artifact_sha256"] = source.source_artifact_sha256
    enriched["source_sheet_name"] = None
    enriched["parse_status"] = "parsed"
    enriched["liftover_status"] = "not_needed"
    enriched["liftover_tool"] = None
    enriched["liftover_tool_version"] = None
    enriched["liftover_notes"] = None
    enriched["norm_status"] = "success"
    enriched["norm_tool"] = "bcftools norm"
    enriched["norm_tool_version"] = command_output(["bcftools", "--version"]).splitlines()[0]
    enriched["norm_notes"] = None
    enriched["transform_run_id"] = RUN_ID
    enriched["transform_timestamp_utc"] = TIMESTAMP_UTC
    return enriched


def parse_clinvar_frame(raw: pd.DataFrame, source: SourceArtifact) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()
    frame = raw.rename(
        columns={
            "CHROM": "chrom38",
            "POS": "pos38",
            "ID": "source_id",
            "REF": "ref_norm",
            "ALT": "alt_norm",
            "OLD_REC": "raw_record",
            "ALLELEID": "alleleid",
            "CLNSIG": "clnsig",
            "CLNREVSTAT": "clnrevstat",
            "GENEINFO": "geneinfo",
            "MC": "mc",
            "CLNVC": "clnvc",
            "CLNHGVS": "clnhgvs",
            "CLNDN": "clndn",
            "CLNDISDB": "clndisdb",
        }
    )
    frame["pos38"] = frame["pos38"].astype(int)
    frame["end38"] = frame["pos38"] + frame["ref_norm"].str.len() - 1
    frame["gene"] = [infer_gene(chrom, pos) for chrom, pos in zip(frame["chrom38"], frame["pos38"])]
    frame["variant_key"] = frame.apply(lambda row: f"{row['chrom38']}:{row['pos38']}:{row['ref_norm']}:{row['alt_norm']}", axis=1)
    frame["vartype"] = [variant_type(ref, alt) for ref, alt in zip(frame["ref_norm"], frame["alt_norm"])]
    omim_pairs = [parse_clinvar_omim_pairs(clndn, clndisdb) for clndn, clndisdb in zip(frame["clndn"], frame["clndisdb"])]
    frame["phenotypes_omim"] = [pair[0] for pair in omim_pairs]
    frame["phenotypes_omim_id"] = [pair[1] for pair in omim_pairs]
    frame["effect"] = [parse_effect(value) for value in frame["mc"]]
    frame["gene_symbol_source"] = [parse_geneinfo(value) for value in frame["geneinfo"]]
    frame["source_row_number"] = None
    frame["source_record_locator"] = frame["raw_record"].map(lambda value: f"vcf_record={value}" if value and value != "." else "vcf_record=as_emitted")
    return base_metadata_frame(frame, source)


def parse_gnomad_frame(raw: pd.DataFrame, source: SourceArtifact, *, cohort: str) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()
    frame = raw.rename(
        columns={
            "CHROM": "chrom38",
            "POS": "pos38",
            "ID": "source_id",
            "REF": "ref_norm",
            "ALT": "alt_norm",
            "OLD_REC": "raw_record",
            "AC": "ac",
            "AN": "an",
            "AF": "af",
            "nhomalt": "hom",
            "AC_mid": "ac_mid",
            "AN_mid": "an_mid",
            "AF_mid": "af_mid",
            "nhomalt_mid": "hom_mid",
            "AC_afr": "ac_afr",
            "AF_afr": "af_afr",
        }
    )
    frame["pos38"] = frame["pos38"].astype(int)
    frame["end38"] = frame["pos38"] + frame["ref_norm"].str.len() - 1
    frame["gene"] = [infer_gene(chrom, pos) for chrom, pos in zip(frame["chrom38"], frame["pos38"])]
    frame["variant_key"] = frame.apply(lambda row: f"{row['chrom38']}:{row['pos38']}:{row['ref_norm']}:{row['alt_norm']}", axis=1)
    frame["vartype"] = [variant_type(ref, alt) for ref, alt in zip(frame["ref_norm"], frame["alt_norm"])]
    for column in ["ac", "an", "hom", "ac_mid", "an_mid", "hom_mid", "ac_afr"]:
        frame[column] = pd.to_numeric(frame[column].replace("", 0), errors="coerce").fillna(0).astype(int)
    for column in ["af", "af_mid", "af_afr"]:
        frame[column] = pd.to_numeric(frame[column].replace("", 0), errors="coerce").fillna(0.0)
    frame["cohort"] = cohort
    frame["source_row_number"] = None
    frame["source_record_locator"] = frame["raw_record"].map(lambda value: f"vcf_record={value}" if value and value != "." else "vcf_record=as_emitted")
    return base_metadata_frame(frame, source)


def parse_shgp_frame(raw: pd.DataFrame, source: SourceArtifact) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()
    frame = raw.rename(
        columns={
            "CHROM": "chrom38",
            "POS": "pos38",
            "ID": "source_id",
            "REF": "ref_norm",
            "ALT": "alt_norm",
            "OLD_REC": "raw_record",
            "SRC_ROW": "source_row_number",
            "SRC_LOC": "source_record_locator",
            "SHGP_AC": "shgp_ac",
            "SHGP_AN": "shgp_an",
            "SHGP_AF": "shgp_af",
        }
    )
    frame["pos38"] = frame["pos38"].astype(int)
    frame["end38"] = frame["pos38"] + frame["ref_norm"].str.len() - 1
    frame["gene"] = [infer_gene(chrom, pos) for chrom, pos in zip(frame["chrom38"], frame["pos38"])]
    frame["variant_key"] = frame.apply(lambda row: f"{row['chrom38']}:{row['pos38']}:{row['ref_norm']}:{row['alt_norm']}", axis=1)
    frame["vartype"] = [variant_type(ref, alt) for ref, alt in zip(frame["ref_norm"], frame["alt_norm"])]
    frame["source_row_number"] = frame["source_row_number"].astype(int)
    frame["shgp_ac"] = pd.to_numeric(frame["shgp_ac"], errors="coerce").fillna(0).astype(int)
    frame["shgp_an"] = pd.to_numeric(frame["shgp_an"], errors="coerce").fillna(0).astype(int)
    frame["shgp_af"] = pd.to_numeric(frame["shgp_af"], errors="coerce")
    return base_metadata_frame(frame, source)


def parse_gme_frame(raw: pd.DataFrame, source: SourceArtifact) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()
    frame = raw.rename(
        columns={
            "CHROM": "chrom38",
            "POS": "pos38",
            "ID": "source_id",
            "REF": "ref_norm",
            "ALT": "alt_norm",
            "OLD_REC": "raw_record",
            "SRC_ROW": "source_row_number",
            "SRC_LOC": "source_record_locator",
            "GME_AF": "gme_af",
            "GME_NWA": "gme_nwa",
            "GME_NEA": "gme_nea",
            "GME_AP": "gme_ap",
            "GME_ISRAEL": "gme_israel",
            "GME_SD": "gme_sd",
            "GME_TP": "gme_tp",
            "GME_CA": "gme_ca",
        }
    )
    frame["pos38"] = frame["pos38"].astype(int)
    frame["end38"] = frame["pos38"] + frame["ref_norm"].str.len() - 1
    frame["gene"] = [infer_gene(chrom, pos) for chrom, pos in zip(frame["chrom38"], frame["pos38"])]
    frame["variant_key"] = frame.apply(lambda row: f"{row['chrom38']}:{row['pos38']}:{row['ref_norm']}:{row['alt_norm']}", axis=1)
    frame["vartype"] = [variant_type(ref, alt) for ref, alt in zip(frame["ref_norm"], frame["alt_norm"])]
    frame["source_row_number"] = frame["source_row_number"].astype(int)
    for column in ["gme_af", "gme_nwa", "gme_nea", "gme_ap", "gme_israel", "gme_sd", "gme_tp", "gme_ca"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return base_metadata_frame(frame, source)


def aggregate_clinvar(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["variant_key"])
    return (
        frame.groupby("variant_key", dropna=False)
        .agg(
            chrom38=("chrom38", "first"),
            pos38=("pos38", "first"),
            end38=("end38", "first"),
            ref_norm=("ref_norm", "first"),
            alt_norm=("alt_norm", "first"),
            gene=("gene", "first"),
            vartype=("vartype", "first"),
            alleleid=("alleleid", lambda values: " | ".join(sorted({v for v in values if v and v != "."})) or None),
            clnsig=("clnsig", lambda values: " | ".join(sorted({v for v in values if v and v != "."})) or None),
            clnrevstat=("clnrevstat", lambda values: " | ".join(sorted({v for v in values if v and v != "."})) or None),
            effect=("effect", lambda values: " | ".join(sorted({v for v in values if v and v != "."})) or None),
            phenotypes_omim=("phenotypes_omim", lambda values: " | ".join(sorted({v for v in values if v})) or None),
            phenotypes_omim_id=("phenotypes_omim_id", lambda values: " | ".join(sorted({v for v in values if v})) or None),
        )
        .reset_index()
    )


def aggregate_gnomad(frame: pd.DataFrame, *, prefix: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["variant_key"])
    grouped = (
        frame.groupby("variant_key", dropna=False)
        .agg(
            chrom38=("chrom38", "first"),
            pos38=("pos38", "first"),
            end38=("end38", "first"),
            ref_norm=("ref_norm", "first"),
            alt_norm=("alt_norm", "first"),
            gene=("gene", "first"),
            vartype=("vartype", "first"),
            ac=("ac", "first"),
            an=("an", "first"),
            af=("af", "first"),
            hom=("hom", "first"),
            ac_mid=("ac_mid", "first"),
            an_mid=("an_mid", "first"),
            af_mid=("af_mid", "first"),
            hom_mid=("hom_mid", "first"),
            ac_afr=("ac_afr", "first"),
            af_afr=("af_afr", "first"),
        )
        .reset_index()
    )
    rename_map = {column: f"{prefix}_{column}" for column in ["ac", "an", "af", "hom", "ac_mid", "an_mid", "af_mid", "hom_mid", "ac_afr", "af_afr"]}
    return grouped.rename(columns=rename_map)


def aggregate_shgp(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["variant_key"])
    return (
        frame.groupby("variant_key", dropna=False)
        .agg(
            chrom38=("chrom38", "first"),
            pos38=("pos38", "first"),
            end38=("end38", "first"),
            ref_norm=("ref_norm", "first"),
            alt_norm=("alt_norm", "first"),
            gene=("gene", "first"),
            vartype=("vartype", "first"),
            shgp_ac=("shgp_ac", "first"),
            shgp_an=("shgp_an", "first"),
            shgp_af=("shgp_af", "first"),
        )
        .reset_index()
    )


def aggregate_gme(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["variant_key"])
    return (
        frame.groupby("variant_key", dropna=False)
        .agg(
            chrom38=("chrom38", "first"),
            pos38=("pos38", "first"),
            end38=("end38", "first"),
            ref_norm=("ref_norm", "first"),
            alt_norm=("alt_norm", "first"),
            gene=("gene", "first"),
            vartype=("vartype", "first"),
            gme_af=("gme_af", "first"),
            gme_nwa=("gme_nwa", "first"),
            gme_nea=("gme_nea", "first"),
            gme_ap=("gme_ap", "first"),
            gme_israel=("gme_israel", "first"),
            gme_sd=("gme_sd", "first"),
            gme_tp=("gme_tp", "first"),
            gme_ca=("gme_ca", "first"),
        )
        .reset_index()
    )


def null_series(size: int) -> list[None]:
    return [None] * size


def build_checkpoint(
    *,
    clinvar: pd.DataFrame,
    gnomad_genomes: pd.DataFrame,
    gnomad_exomes: pd.DataFrame,
    shgp: pd.DataFrame,
    gme: pd.DataFrame | None,
    stage_label: str,
) -> pd.DataFrame:
    sources = [frame for frame in [clinvar, gnomad_genomes, gnomad_exomes, shgp, gme] if frame is not None and not frame.empty]
    keys = pd.concat([frame[["variant_key", "chrom38", "pos38", "end38", "ref_norm", "alt_norm", "gene", "vartype"]] for frame in sources], ignore_index=True)
    keys = keys.drop_duplicates(subset=["variant_key"]).sort_values(["gene", "chrom38", "pos38", "ref_norm", "alt_norm"]).reset_index(drop=True)

    merged = keys.copy()
    for frame in [clinvar, gnomad_genomes, gnomad_exomes, shgp]:
        if not frame.empty:
            merged = merged.merge(frame.drop(columns=[c for c in ["chrom38", "pos38", "end38", "ref_norm", "alt_norm", "gene", "vartype"] if c in frame.columns]), on="variant_key", how="left")
    if gme is not None and not gme.empty:
        merged = merged.merge(gme.drop(columns=[c for c in ["chrom38", "pos38", "end38", "ref_norm", "alt_norm", "gene", "vartype"] if c in gme.columns]), on="variant_key", how="left")

    all_ac = merged[[column for column in ["genomes_ac", "exomes_ac"] if column in merged.columns]].fillna(0).sum(axis=1)
    all_an = merged[[column for column in ["genomes_an", "exomes_an"] if column in merged.columns]].fillna(0).sum(axis=1)
    all_hom = merged[[column for column in ["genomes_hom", "exomes_hom"] if column in merged.columns]].fillna(0).sum(axis=1)
    mid_ac = merged[[column for column in ["genomes_ac_mid", "exomes_ac_mid"] if column in merged.columns]].fillna(0).sum(axis=1)
    mid_an = merged[[column for column in ["genomes_an_mid", "exomes_an_mid"] if column in merged.columns]].fillna(0).sum(axis=1)
    mid_hom = merged[[column for column in ["genomes_hom_mid", "exomes_hom_mid"] if column in merged.columns]].fillna(0).sum(axis=1)

    result = pd.DataFrame(
        {
            "CHROM": merged["chrom38"],
            "POS": merged["pos38"],
            "END": merged["end38"],
            "ID": merged["variant_key"],
            "REF": merged["ref_norm"],
            "ALT": merged["alt_norm"],
            "VARTYPE": merged["vartype"],
            "Repeat": null_series(len(merged)),
            "Segdup": null_series(len(merged)),
            "Blacklist": null_series(len(merged)),
            "GENE": merged["gene"],
            "EFFECT": merged.get("effect"),
            "HGVS_C": null_series(len(merged)),
            "HGVS_P": null_series(len(merged)),
            "PHENOTYPES_OMIM": merged.get("phenotypes_omim"),
            "PHENOTYPES_OMIM_ID": merged.get("phenotypes_omim_id"),
            "INHERITANCE_PATTERN": null_series(len(merged)),
            "ALLELEID": merged.get("alleleid"),
            "CLNSIG": merged.get("clnsig"),
            "TOPMED_AF": null_series(len(merged)),
            "TOPMed_Hom": null_series(len(merged)),
            "ALFA_AF": null_series(len(merged)),
            "ALFA_Hom": null_series(len(merged)),
            "GNOMAD_ALL_AF": all_ac.divide(all_an.where(all_an != 0)).where(all_an != 0),
            "gnomAD_all_Hom": all_hom.where(all_hom != 0),
            "GNOMAD_MID_AF": mid_ac.divide(mid_an.where(mid_an != 0)).where(mid_an != 0),
            "gnomAD_mid_Hom": mid_hom.where(mid_hom != 0),
            "ONEKGP_AF": null_series(len(merged)),
            "REGENERON_AF": null_series(len(merged)),
            "TGP_AF": null_series(len(merged)),
            "QATARI": null_series(len(merged)),
            "JGP_AF": null_series(len(merged)),
            "JGP_MAF": null_series(len(merged)),
            "JGP_Hom": null_series(len(merged)),
            "JGP_Het": null_series(len(merged)),
            "JGP_AC_Hemi": null_series(len(merged)),
            "SIFT_PRED": null_series(len(merged)),
            "POLYPHEN2_HDIV_PRED": null_series(len(merged)),
            "POLYPHEN2_HVAR_PRED": null_series(len(merged)),
            "PROVEAN_PRE": null_series(len(merged)),
            "VARIANT_KEY": merged["variant_key"],
            "CLNREVSTAT": merged.get("clnrevstat"),
            "GNOMAD_GENOMES_AC": merged.get("genomes_ac"),
            "GNOMAD_GENOMES_AN": merged.get("genomes_an"),
            "GNOMAD_GENOMES_AF": merged.get("genomes_af"),
            "GNOMAD_GENOMES_HOM": merged.get("genomes_hom"),
            "GNOMAD_EXOMES_AC": merged.get("exomes_ac"),
            "GNOMAD_EXOMES_AN": merged.get("exomes_an"),
            "GNOMAD_EXOMES_AF": merged.get("exomes_af"),
            "GNOMAD_EXOMES_HOM": merged.get("exomes_hom"),
            "SHGP_AC": merged.get("shgp_ac"),
            "SHGP_AN": merged.get("shgp_an"),
            "SHGP_AF": merged.get("shgp_af"),
            "GME_AF": merged.get("gme_af") if gme is not None else null_series(len(merged)),
            "GME_NWA": merged.get("gme_nwa") if gme is not None else null_series(len(merged)),
            "GME_NEA": merged.get("gme_nea") if gme is not None else null_series(len(merged)),
            "GME_AP": merged.get("gme_ap") if gme is not None else null_series(len(merged)),
            "GME_SD": merged.get("gme_sd") if gme is not None else null_series(len(merged)),
        }
    )
    result["PRESENT_IN_CLINVAR"] = merged.get("alleleid").notna().astype(int)
    result["PRESENT_IN_GNOMAD_GENOMES"] = merged.get("genomes_af").notna().astype(int)
    result["PRESENT_IN_GNOMAD_EXOMES"] = merged.get("exomes_af").notna().astype(int)
    result["PRESENT_IN_SHGP"] = merged.get("shgp_af").notna().astype(int)
    result["PRESENT_IN_GME"] = merged.get("gme_af").notna().astype(int) if "gme_af" in merged else 0
    result["SOURCE_COUNT"] = result[["PRESENT_IN_CLINVAR", "PRESENT_IN_GNOMAD_GENOMES", "PRESENT_IN_GNOMAD_EXOMES", "PRESENT_IN_SHGP", "PRESENT_IN_GME"]].sum(axis=1)
    result["PIPELINE_STAGE"] = stage_label
    return result.where(pd.notnull(result), None)


def save_parquet(frame: pd.DataFrame, path: Path) -> None:
    frame.to_parquet(path, index=False)


def save_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False)


def source_columns() -> list[dict[str, str]]:
    return [
        {"name": "variant_key", "description": "Canonical GRCh38 key after normalization.", "kind": "required"},
        {"name": "chrom38", "description": "Canonical chromosome with chr prefix.", "kind": "required"},
        {"name": "pos38", "description": "Canonical 1-based start after normalization.", "kind": "required"},
        {"name": "end38", "description": "Canonical end after normalization.", "kind": "required"},
        {"name": "ref_norm", "description": "Normalized reference allele.", "kind": "required"},
        {"name": "alt_norm", "description": "Normalized alternate allele.", "kind": "required"},
        {"name": "source_artifact_uri", "description": "Frozen raw source package used for this row.", "kind": "required"},
        {"name": "source_record_locator", "description": "Stable raw-row locator or VCF origin pointer.", "kind": "required"},
        {"name": "parse_status", "description": "Parsing status under the T003 contract.", "kind": "required"},
        {"name": "liftover_status", "description": "Liftover status under the T003 contract.", "kind": "required"},
        {"name": "norm_status", "description": "Normalization status under the T003 contract.", "kind": "required"},
    ]


def required_and_extra_glossary() -> list[dict[str, str]]:
    required = [
        ("CHROM", "Canonical GRCh38 chromosome label with chr prefix."),
        ("POS", "1-based canonical start position."),
        ("END", "Canonical end position after normalization."),
        ("ID", "Stable pipeline identifier; here it is the canonical variant key."),
        ("REF", "Normalized reference allele."),
        ("ALT", "Normalized alternate allele."),
        ("VARTYPE", "Variant class derived from the normalized REF/ALT pair."),
        ("Repeat", "Reserved placeholder until a repeat-track source is added."),
        ("Segdup", "Reserved placeholder until a segmental-duplication source is added."),
        ("Blacklist", "Reserved placeholder until a blacklist-region source is added."),
        ("GENE", "BRCA1 or BRCA2 assigned from the frozen Ensembl-backed windows."),
        ("EFFECT", "Effect label only when a source-backed value exists."),
        ("HGVS_C", "Reserved until transcript annotation is added."),
        ("HGVS_P", "Reserved until transcript annotation is added."),
        ("PHENOTYPES_OMIM", "ClinVar-derived OMIM phenotype names when OMIM identifiers are present."),
        ("PHENOTYPES_OMIM_ID", "ClinVar-derived OMIM identifiers when present."),
        ("INHERITANCE_PATTERN", "Reserved until an inheritance source is added."),
        ("ALLELEID", "ClinVar ALLELEID."),
        ("CLNSIG", "ClinVar clinical significance."),
        ("TOPMED_AF", "Reserved until TOPMed is added."),
        ("TOPMed_Hom", "Reserved until TOPMed is added."),
        ("ALFA_AF", "Reserved until ALFA is added."),
        ("ALFA_Hom", "Reserved until ALFA is added."),
        ("GNOMAD_ALL_AF", "Combined AF from the normalized gnomAD genomes + exomes cohorts used in this project."),
        ("gnomAD_all_Hom", "Combined homozygote count from the normalized gnomAD genomes + exomes cohorts used in this project."),
        ("GNOMAD_MID_AF", "Combined Middle Eastern AF from the normalized gnomAD genomes + exomes cohorts used in this project."),
        ("gnomAD_mid_Hom", "Combined Middle Eastern homozygote count from the normalized gnomAD genomes + exomes cohorts used in this project."),
        ("ONEKGP_AF", "Reserved until a 1000 Genomes source is added."),
        ("REGENERON_AF", "Reserved until a Regeneron source is added."),
        ("TGP_AF", "Reserved until a TGP source is added."),
        ("QATARI", "Reserved until a Qatari source is added."),
        ("JGP_AF", "Reserved until a JGP source is added."),
        ("JGP_MAF", "Reserved until a JGP source is added."),
        ("JGP_Hom", "Reserved until a JGP source is added."),
        ("JGP_Het", "Reserved until a JGP source is added."),
        ("JGP_AC_Hemi", "Reserved until a JGP source is added."),
        ("SIFT_PRED", "Reserved until a transcript annotation engine is added."),
        ("POLYPHEN2_HDIV_PRED", "Reserved until a transcript annotation engine is added."),
        ("POLYPHEN2_HVAR_PRED", "Reserved until a transcript annotation engine is added."),
        ("PROVEAN_PRE", "Reserved until a transcript annotation engine is added."),
    ]
    extras = [
        ("VARIANT_KEY", "Explicit canonical key kept as a pipeline extra for joins and audits."),
        ("CLNREVSTAT", "ClinVar review status."),
        ("GNOMAD_GENOMES_AC", "Normalized gnomAD genomes allele count."),
        ("GNOMAD_GENOMES_AN", "Normalized gnomAD genomes allele number."),
        ("GNOMAD_GENOMES_AF", "Normalized gnomAD genomes allele frequency."),
        ("GNOMAD_GENOMES_HOM", "Normalized gnomAD genomes homozygote count."),
        ("GNOMAD_EXOMES_AC", "Normalized gnomAD exomes allele count."),
        ("GNOMAD_EXOMES_AN", "Normalized gnomAD exomes allele number."),
        ("GNOMAD_EXOMES_AF", "Normalized gnomAD exomes allele frequency."),
        ("GNOMAD_EXOMES_HOM", "Normalized gnomAD exomes homozygote count."),
        ("SHGP_AC", "Saudi frequency-table alternate allele count."),
        ("SHGP_AN", "Saudi frequency-table total allele count."),
        ("SHGP_AF", "Saudi frequency-table allele frequency."),
        ("GME_AF", "Overall GME allele frequency."),
        ("GME_NWA", "GME North West Africa subgroup frequency."),
        ("GME_NEA", "GME North East Africa subgroup frequency."),
        ("GME_AP", "GME Arabian Peninsula subgroup frequency."),
        ("GME_SD", "GME Syrian Desert subgroup frequency."),
        ("PRESENT_IN_CLINVAR", "1 when the canonical allele is present in ClinVar."),
        ("PRESENT_IN_GNOMAD_GENOMES", "1 when the canonical allele is present in the normalized gnomAD genomes cohort."),
        ("PRESENT_IN_GNOMAD_EXOMES", "1 when the canonical allele is present in the normalized gnomAD exomes cohort."),
        ("PRESENT_IN_SHGP", "1 when the canonical allele is present in SHGP."),
        ("PRESENT_IN_GME", "1 when the canonical allele is present in GME."),
        ("SOURCE_COUNT", "How many source streams support the canonical allele in this checkpoint."),
        ("PIPELINE_STAGE", "Checkpoint label used for the frozen artifact."),
    ]
    return build_header_glossary(required, extras)


def build_source_manifest(source: SourceArtifact, frame: pd.DataFrame, report_uri: str, parquet_uri: str, sample: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_key": source.key,
        "display_name": source.display_name,
        "snapshot_date": SNAPSHOT_DATE,
        "source_artifact_uri": source.source_artifact_uri,
        "source_artifact_sha256": source.source_artifact_sha256,
        "normalized_parquet_uri": parquet_uri,
        "normalization_report_uri": report_uri,
        "row_count": int(len(frame)),
        "variant_key_count": int(frame["variant_key"].nunique()) if not frame.empty else 0,
        "sample_preview": sample,
        "transform_run_id": RUN_ID,
        "transform_timestamp_utc": TIMESTAMP_UTC,
    }


def build_review_bundle(
    *,
    raw_cards: list[dict[str, Any]],
    normalized_cards: list[dict[str, Any]],
    pre_gme_artifact: WorkflowArtifact,
    final_artifact: WorkflowArtifact,
    normalization_report_uri: str,
    checkpoint_report_uri: str,
    final_csv_public_url: str,
) -> dict[str, Any]:
    return {
        "generated_at": TIMESTAMP_UTC,
        "frozen_at": SNAPSHOT_DATE,
        "cost_mode": "static_review_bundle",
        "workflow": {
            "pages": [
                {"id": "overview", "title": "Overview", "summary": "Track progress and workflow navigation for the supervisor."},
                {"id": "raw", "title": "Raw Sources", "summary": "Frozen untouched source packages and BRCA-window raw previews before any normalization."},
                {"id": "harmonization", "title": "Normalization", "summary": "Per-source BRCA normalization artifacts with explicit provenance and no live queries."},
                {"id": "pre-gme", "title": "Pre-GME Review", "summary": "Arab-aware BRCA checkpoint from ClinVar + gnomAD + SHGP before GME is added."},
                {"id": "final", "title": "Final Registry", "summary": "Arab-aware BRCA checkpoint after the supporting GME layer is added."},
                {"id": "access", "title": "Controlled Access", "summary": "Official acquisition paths for restricted Arab datasets still outside the active workflow."},
            ],
            "harmonization_steps": [
                {
                    "id": "clinvar_normalized_brca",
                    "title": "Step 1: Normalize ClinVar BRCA rows",
                    "simple": "Extract the BRCA windows from the GRCh38 ClinVar VCF, prefix chromosomes with chr, then normalize alleles with bcftools.",
                    "technical": "The normalized rows keep ALLELEID, CLNSIG, CLNREVSTAT, MC-derived effect labels, and raw-record lineage through the OLD_REC tag.",
                },
                {
                    "id": "gnomad_genomes_normalized_brca",
                    "title": "Step 2: Normalize gnomAD genomes BRCA rows",
                    "simple": "Extract chr13 and chr17 BRCA windows from gnomAD genomes, then normalize biallelic alleles with bcftools.",
                    "technical": "The artifact preserves AC, AN, AF, nhomalt, AFR, and MID metrics per normalized allele.",
                },
                {
                    "id": "gnomad_exomes_normalized_brca",
                    "title": "Step 3: Normalize gnomAD exomes BRCA rows",
                    "simple": "Extract chr13 and chr17 BRCA windows from gnomAD exomes, then normalize biallelic alleles with bcftools.",
                    "technical": "Exomes stay separate from genomes in the per-source artifact so later combined metrics remain auditable.",
                },
                {
                    "id": "shgp_normalized_brca",
                    "title": "Step 4: Normalize SHGP BRCA rows",
                    "simple": "Filter the Saudi frequency table to BRCA windows, convert rows to minimal VCF, then normalize with bcftools.",
                    "technical": "Each row keeps its original table row locator and the derived SHGP AF still ties back to AC and AN.",
                },
                {
                    "id": "pre_gme_checkpoint",
                    "title": "Step 5: Build the Arab pre-GME checkpoint",
                    "simple": "Join the normalized ClinVar, gnomAD, and SHGP artifacts on the canonical variant key.",
                    "technical": "The required publication-facing header stays first and every combined gnomAD metric is derived from explicit per-cohort counts.",
                },
            ],
            "final_steps": [
                {
                    "id": "gme_normalized_brca",
                    "title": "Step 6: Normalize GME BRCA rows",
                    "simple": "Filter GME to BRCA windows, convert summary rows into minimal VCF, then normalize with bcftools.",
                    "technical": "Deletion rows that use '-' are anchored on the previous GRCh38 base so the VCF representation remains valid and explicit.",
                },
                {
                    "id": "final_checkpoint",
                    "title": "Step 7: Build the final Arab checkpoint",
                    "simple": "Add the supporting GME layer on top of the Arab pre-GME checkpoint without changing the required header order.",
                    "technical": "Only the Arab-relevant GME fields are carried forward as extras, so context-only subgroup columns do not clutter the final registry.",
                },
            ],
        },
        "raw_datasets": {"datasets": raw_cards},
        "datasets": {"datasets": normalized_cards},
        "pre_gme": {
            "title": pre_gme_artifact.title,
            "table_ref": pre_gme_artifact.storage_uri,
            "row_count": pre_gme_artifact.row_count,
            "scope_note": "This checkpoint is the Arab-aware BRCA table before GME is added. It combines normalized ClinVar, gnomAD, and SHGP rows only.",
            "accuracy_notes": [
                "The required publication-facing header remains the minimum schema and unsupported fields stay NULL.",
                "Combined gnomAD fields are calculated from explicit genomes/exomes counts carried alongside the final row.",
                "SHGP is now part of the pre-GME checkpoint because it is a primary Arab frequency source, not an afterthought.",
            ],
            "scientific_notes": [
                "This table is built from frozen normalized Parquet artifacts only; there is no live BigQuery rebuild behind the UI.",
                "Every row inherits the canonical variant key from the normalized source artifacts, and each source can still be reviewed independently on the Harmonization page.",
            ],
            "scientific_metrics": {
                "source_row_counts": [
                    {"source_name": "clinvar", "row_count": int(pre_gme_artifact.sample.get("source_counts", {}).get("clinvar", 0))},
                    {"source_name": "gnomad_genomes", "row_count": int(pre_gme_artifact.sample.get("source_counts", {}).get("gnomad_genomes", 0))},
                    {"source_name": "gnomad_exomes", "row_count": int(pre_gme_artifact.sample.get("source_counts", {}).get("gnomad_exomes", 0))},
                    {"source_name": "shgp", "row_count": int(pre_gme_artifact.sample.get("source_counts", {}).get("shgp", 0))},
                ],
                "frozen_at": SNAPSHOT_DATE,
            },
            "columns": pre_gme_artifact.columns,
            "build_sql": "Build logic is executed in scripts/build_brca_normalized_artifacts.py. The checkpoint joins canonical variant keys from the normalized ClinVar, gnomAD genomes, gnomAD exomes, and SHGP Parquet artifacts.",
            "export_metadata_preview": [
                f"Created at: {TIMESTAMP_UTC}",
                f"Run id: {RUN_ID}",
                f"Artifact: {pre_gme_artifact.storage_uri}",
            ],
            "export_header_columns": REQUIRED_COLUMNS,
            "sample": pre_gme_artifact.sample,
        },
        "registry": {
            "title": final_artifact.title,
            "table_ref": final_artifact.storage_uri,
            "row_count": final_artifact.row_count,
            "scope_note": "This final checkpoint keeps the Arab-aware pre-GME table intact, then adds the supporting GME layer as explicit extras.",
            "accuracy_notes": [
                "Context-only GME subgroup columns were intentionally left out of the final checkpoint to keep the Arab analysis focused.",
                "The only added GME extras are the overall AF and the Arab-relevant subgroup frequencies (NWA, NEA, AP, SD).",
            ],
            "scientific_notes": [
                "The final CSV download points to the frozen GCS export of this checkpoint, not a live query.",
                "The same canonical variant key and per-source presence flags remain available for audit and downstream joins.",
            ],
            "scientific_metrics": {
                "source_row_counts": [
                    {"source_name": "clinvar", "row_count": int(final_artifact.sample.get("source_counts", {}).get("clinvar", 0))},
                    {"source_name": "gnomad_genomes", "row_count": int(final_artifact.sample.get("source_counts", {}).get("gnomad_genomes", 0))},
                    {"source_name": "gnomad_exomes", "row_count": int(final_artifact.sample.get("source_counts", {}).get("gnomad_exomes", 0))},
                    {"source_name": "shgp", "row_count": int(final_artifact.sample.get("source_counts", {}).get("shgp", 0))},
                    {"source_name": "gme", "row_count": int(final_artifact.sample.get("source_counts", {}).get("gme", 0))},
                ],
                "frozen_at": SNAPSHOT_DATE,
            },
            "columns": final_artifact.columns,
            "build_sql": "Build logic is executed in scripts/build_brca_normalized_artifacts.py. The final checkpoint starts from the frozen Arab pre-GME checkpoint and left-joins normalized GME BRCA rows by canonical variant key.",
            "csv_download_url": final_csv_public_url,
            "sample": final_artifact.sample,
        },
        "step_samples": {card["key"]: card["sample"] for card in normalized_cards} | {
            "pre_gme_checkpoint": pre_gme_artifact.sample,
            "final_checkpoint": final_artifact.sample,
        },
        "artifacts": {
            "bucket": BUCKET_NAME,
            "normalization_report_uri": normalization_report_uri,
            "checkpoint_report_uri": checkpoint_report_uri,
            "final_csv_uri": f"gs://{BUCKET_NAME}/{PUBLIC_FINAL_CSV_OBJECT}",
            "final_csv_public_url": final_csv_public_url,
            "bundle_uri": f"gs://{BUCKET_NAME}/frozen/review_bundle/snapshot_date={SNAPSHOT_DATE}/review_bundle.json",
        },
    }


def raw_card(*, key: str, title: str, source: SourceArtifact, sample_frame: pd.DataFrame, summary: str, notes: list[str]) -> dict[str, Any]:
    return {
        "key": key,
        "title": title,
        "table_ref": source.source_artifact_uri,
        "storage_ref": source.source_artifact_uri,
        "row_count": source.row_count if source.row_count is not None else int(len(sample_frame)),
        "simple_summary": summary,
        "notes": notes,
        "columns": [
            {"name": name, "description": desc, "kind": "extra"}
            for name, desc in [(column, f"Frozen preview column `{column}` from the untouched source package.") for column in sample_frame.columns]
        ],
        "sample": compact_rows(sample_frame),
        "download_url": None,
    }


def normalized_card(*, artifact: WorkflowArtifact) -> dict[str, Any]:
    return {
        "key": artifact.key,
        "title": artifact.title,
        "table_ref": artifact.storage_uri,
        "storage_ref": artifact.storage_uri,
        "row_count": artifact.row_count,
        "simple_summary": artifact.summary,
        "notes": artifact.notes,
        "columns": artifact.columns,
        "sample": artifact.sample,
        "download_url": None,
    }


def build_source_review_json(
    *,
    sources: list[dict[str, Any]],
    decision_summary: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "generated_at": TIMESTAMP_UTC,
        "workflow_categories": [
            {
                "id": "raw_freeze",
                "title": "Stage 1: Freeze the raw source package",
                "purpose": "Preserve the exact upstream bytes before any filtering, lifting, or normalization.",
                "evidence_types": ["raw GCS prefix", "manifest checksum", "upstream URL"],
                "output": "A raw source-of-truth artifact in GCS.",
            },
            {
                "id": "normalization_entry",
                "title": "Stage 2: Normalize only coordinate-ready rows",
                "purpose": "Convert BRCA-ready rows into a canonical GRCh38 representation with an explicit audit trail.",
                "evidence_types": ["OLD_REC or SRC_* lineage", "bcftools version", "canonical variant key"],
                "output": "A frozen normalized Parquet artifact per source.",
            },
            {
                "id": "checkpoint_build",
                "title": "Stage 3: Build the Arab checkpoints",
                "purpose": "Join normalized sources into first the pre-GME checkpoint and then the final GME-extended checkpoint.",
                "evidence_types": ["per-source presence flags", "source_count", "frozen checkpoint manifests"],
                "output": "A pre-GME Arab checkpoint and a final Arab checkpoint in GCS.",
            },
        ],
        "decision_summary": decision_summary,
        "sources": sources,
    }


def source_review_entry(
    *,
    source: SourceArtifact,
    project_fit: str,
    project_fit_label: str,
    project_fit_summary: str,
    project_fit_note: str,
    category: str,
    coordinate_readiness: str,
    liftover_decision: str,
    normalization_decision: str,
    review_status: str,
    next_action: str,
    sample: dict[str, Any],
    workflow_position: dict[str, Any],
    notes: list[str],
    artifact_links: list[dict[str, str]],
    row_count: int | None,
) -> dict[str, Any]:
    return {
        "source_key": source.key,
        "display_name": source.display_name,
        "category": category,
        "source_kind": source.source_kind,
        "source_build": source.source_build,
        "coordinate_readiness": coordinate_readiness,
        "liftover_decision": liftover_decision,
        "normalization_decision": normalization_decision,
        "brca_relevance": "Direct" if project_fit not in {"blocked", "reference_only"} else "Indirect",
        "review_status": review_status,
        "project_fit": project_fit,
        "project_fit_note": project_fit_note,
        "use_tier": project_fit,
        "use_tier_label": project_fit_label,
        "use_tier_summary": project_fit_summary,
        "snapshot_date": source.snapshot_date,
        "source_version": source.source_version,
        "upstream_url": source.upstream_url,
        "raw_vault_prefix": uri_prefix(source.source_artifact_uri),
        "raw_manifest_uri": source.manifest_uri,
        "row_count": row_count,
        "notes": notes,
        "artifact_links": artifact_links,
        "workflow_position": workflow_position,
        "next_action": next_action,
        "sample": sample,
    }


def build_decision_summary(sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    labels = {
        "adopted_100": ("Adopted 100%", "Core input"),
        "adopted_secondary": ("Supporting source", "Used as supporting evidence"),
        "reference_only": ("Reference only", "Kept for context and audit"),
        "demo_only": ("Demo only", "Useful for walkthroughs, not a core evidence stream"),
        "blocked": ("Blocked", "Frozen only until scientific gaps are resolved"),
    }
    grouped: dict[str, list[str]] = {}
    for source in sources:
        grouped.setdefault(str(source["use_tier"]), []).append(str(source["display_name"]))
    summary = []
    for key in ["adopted_100", "adopted_secondary", "reference_only", "demo_only", "blocked"]:
        members = sorted(grouped.get(key, []))
        if not members:
            continue
        label, short = labels[key]
        summary.append({"tier": key, "label": label, "summary": short, "count": len(members), "members": members})
    return summary


def write_ui_files(review_bundle: dict[str, Any], source_review: dict[str, Any]) -> None:
    json_dump(UI_DIR / "review_bundle.json", review_bundle)
    json_dump(UI_DIR / "source_review.json", source_review)


def update_overview_state() -> None:
    subprocess.run(["python3", "scripts/update_ui_overview_state.py"], check=True, cwd=ROOT)


def artifact_prefix(name: str) -> str:
    return f"frozen/harmonized/checkpoint={name}/snapshot_date={SNAPSHOT_DATE}"


def main() -> None:
    storage_client = storage.Client(project=PROJECT_ID)
    clinvar_manifest = download_gcs_json(storage_client, CLINVAR_MANIFEST_URI)
    shgp_manifest = download_gcs_json(storage_client, SHGP_MANIFEST_URI)
    gme_manifest = download_gcs_json(storage_client, GME_MANIFEST_URI)

    clinvar_source = parse_source_artifact(clinvar_manifest, key="clinvar", display_name="ClinVar GRCh38 VCF", source_kind="VCF", source_build="GRCh38", manifest_uri=CLINVAR_MANIFEST_URI)
    shgp_source = parse_source_artifact(shgp_manifest, key="shgp_saudi_af", display_name="SHGP Saudi allele-frequency table", source_kind="TSV frequency table", source_build="GRCh38", manifest_uri=SHGP_MANIFEST_URI)
    gme_source = parse_source_artifact(gme_manifest, key="gme_hg38", display_name="GME hg38 summary table", source_kind="Summary table", source_build="GRCh38", manifest_uri=GME_MANIFEST_URI)

    gnomad_sources = {
        key: SourceArtifact(
            key=("gnomad_genomes" if key.startswith("genomes") else "gnomad_exomes"),
            display_name=("gnomAD v4.1 genomes (chr13 + chr17)" if key.startswith("genomes") else "gnomAD v4.1 exomes (chr13 + chr17)"),
            source_kind="VCF",
            source_version="4.1",
            source_build="GRCh38",
            snapshot_date="2026-03-03",
            upstream_url=value["public_url"],
            source_artifact_uri=value["raw_uri"],
            source_artifact_sha256="sha256_not_available;server_side_copy_manifest_only",
            manifest_uri=value["raw_uri"].rsplit("/", 1)[0] + "/manifest.json",
            row_count=None,
            notes="gnomAD raw archive preserved by server-side copy; raw object md5/crc32c exist but sha256 was not computed at ingest time.",
        )
        for key, value in GNOMAD_SOURCE_URIS.items()
    }

    with tempfile.TemporaryDirectory(prefix="arab_acmg_t003_") as tmpdir:
        temp_dir = Path(tmpdir)
        reference_fasta, reference_meta = download_reference_fasta(temp_dir)
        reference = load_reference_sequences(reference_fasta)
        rename_map = temp_dir / "clinvar_chr_map.txt"
        rename_map.write_text("13\tchr13\n17\tchr17\n", encoding="utf-8")

        clinvar_frame, clinvar_report, clinvar_raw_preview = normalize_public_vcf_source(
            temp_dir=temp_dir,
            source=clinvar_source,
            public_url=CLINVAR_PUBLIC_URL,
            rename_map_path=rename_map,
            regions=[f"13:{BRCA_WINDOWS['BRCA2']['start']}-{BRCA_WINDOWS['BRCA2']['end']}", f"17:{BRCA_WINDOWS['BRCA1']['start']}-{BRCA_WINDOWS['BRCA1']['end']}"],
            reference_fasta=reference_fasta,
            query_format=QUERY_FORMATS["clinvar"],
            query_columns=["CHROM", "POS", "ID", "REF", "ALT", "OLD_REC", "ALLELEID", "CLNSIG", "CLNREVSTAT", "GENEINFO", "MC", "CLNVC", "CLNHGVS", "CLNDN", "CLNDISDB"],
            parser=parse_clinvar_frame,
        )

        gnomad_genomes_frames: list[pd.DataFrame] = []
        gnomad_genomes_reports: list[dict[str, Any]] = []
        for chrom_key in ["genomes_chr13", "genomes_chr17"]:
            chrom = "chr13" if chrom_key.endswith("chr13") else "chr17"
            gene = "BRCA2" if chrom == "chr13" else "BRCA1"
            frame, report, raw_preview = normalize_public_vcf_source(
                temp_dir=temp_dir,
                source=gnomad_sources[chrom_key],
                public_url=GNOMAD_SOURCE_URIS[chrom_key]["public_url"],
                rename_map_path=None,
                regions=[f"{chrom}:{BRCA_WINDOWS[gene]['start']}-{BRCA_WINDOWS[gene]['end']}"],
                reference_fasta=reference_fasta,
                query_format=QUERY_FORMATS["gnomad"],
                query_columns=["CHROM", "POS", "ID", "REF", "ALT", "OLD_REC", "AC", "AN", "AF", "nhomalt", "AC_mid", "AN_mid", "AF_mid", "nhomalt_mid", "AC_afr", "AF_afr"],
                parser=lambda raw, source_obj, cohort="genomes": parse_gnomad_frame(raw, source_obj, cohort=cohort),
            )
            gnomad_genomes_frames.append(frame)
            gnomad_genomes_reports.append(report)
            if chrom_key == "genomes_chr13":
                gnomad_genomes_raw_preview = raw_preview
            else:
                gnomad_genomes_raw_preview = pd.concat([gnomad_genomes_raw_preview, raw_preview], ignore_index=True)
        gnomad_genomes_frame = pd.concat(gnomad_genomes_frames, ignore_index=True)

        gnomad_exomes_frames: list[pd.DataFrame] = []
        gnomad_exomes_reports: list[dict[str, Any]] = []
        for chrom_key in ["exomes_chr13", "exomes_chr17"]:
            chrom = "chr13" if chrom_key.endswith("chr13") else "chr17"
            gene = "BRCA2" if chrom == "chr13" else "BRCA1"
            frame, report, raw_preview = normalize_public_vcf_source(
                temp_dir=temp_dir,
                source=gnomad_sources[chrom_key],
                public_url=GNOMAD_SOURCE_URIS[chrom_key]["public_url"],
                rename_map_path=None,
                regions=[f"{chrom}:{BRCA_WINDOWS[gene]['start']}-{BRCA_WINDOWS[gene]['end']}"],
                reference_fasta=reference_fasta,
                query_format=QUERY_FORMATS["gnomad"],
                query_columns=["CHROM", "POS", "ID", "REF", "ALT", "OLD_REC", "AC", "AN", "AF", "nhomalt", "AC_mid", "AN_mid", "AF_mid", "nhomalt_mid", "AC_afr", "AF_afr"],
                parser=lambda raw, source_obj, cohort="exomes": parse_gnomad_frame(raw, source_obj, cohort=cohort),
            )
            gnomad_exomes_frames.append(frame)
            gnomad_exomes_reports.append(report)
            if chrom_key == "exomes_chr13":
                gnomad_exomes_raw_preview = raw_preview
            else:
                gnomad_exomes_raw_preview = pd.concat([gnomad_exomes_raw_preview, raw_preview], ignore_index=True)
        gnomad_exomes_frame = pd.concat(gnomad_exomes_frames, ignore_index=True)

        shgp_rows = build_shgp_rows()
        shgp_frame, shgp_report = normalize_table_source(
            temp_dir=temp_dir,
            source=shgp_source,
            source_rows=shgp_rows,
            reference=reference,
            reference_fasta=reference_fasta,
            extra_header_lines=[
                '##INFO=<ID=SHGP_AC,Number=1,Type=Integer,Description="Saudi alternate allele count">',
                '##INFO=<ID=SHGP_AN,Number=1,Type=Integer,Description="Saudi allele number">',
                '##INFO=<ID=SHGP_AF,Number=1,Type=Float,Description="Saudi allele frequency">',
            ],
            info_builder=lambda row: [f"SHGP_AC={row['shgp_ac']}", f"SHGP_AN={row['shgp_an']}", f"SHGP_AF={row['shgp_af']:.10f}"],
            query_format=QUERY_FORMATS["shgp"],
            query_columns=["CHROM", "POS", "ID", "REF", "ALT", "OLD_REC", "SRC_ROW", "SRC_LOC", "SHGP_AC", "SHGP_AN", "SHGP_AF"],
            parser=parse_shgp_frame,
        )

        gme_rows = build_gme_rows()
        gme_frame, gme_report = normalize_table_source(
            temp_dir=temp_dir,
            source=gme_source,
            source_rows=gme_rows,
            reference=reference,
            reference_fasta=reference_fasta,
            extra_header_lines=[
                '##INFO=<ID=GME_AF,Number=1,Type=Float,Description="Overall GME allele frequency">',
                '##INFO=<ID=GME_NWA,Number=1,Type=Float,Description="GME North West Africa frequency">',
                '##INFO=<ID=GME_NEA,Number=1,Type=Float,Description="GME North East Africa frequency">',
                '##INFO=<ID=GME_AP,Number=1,Type=Float,Description="GME Arabian Peninsula frequency">',
                '##INFO=<ID=GME_ISRAEL,Number=1,Type=Float,Description="GME Israel/Jewish subgroup frequency">',
                '##INFO=<ID=GME_SD,Number=1,Type=Float,Description="GME Syrian Desert frequency">',
                '##INFO=<ID=GME_TP,Number=1,Type=Float,Description="GME Turkish Peninsula frequency">',
                '##INFO=<ID=GME_CA,Number=1,Type=Float,Description="GME Central Asia frequency">',
            ],
            info_builder=lambda row: [
                f"GME_AF={row['gme_af']:.10f}",
                f"GME_NWA={row['gme_nwa']:.10f}",
                f"GME_NEA={row['gme_nea']:.10f}",
                f"GME_AP={row['gme_ap']:.10f}",
                f"GME_ISRAEL={row['gme_israel']:.10f}",
                f"GME_SD={row['gme_sd']:.10f}",
                f"GME_TP={row['gme_tp']:.10f}",
                f"GME_CA={row['gme_ca']:.10f}",
            ],
            query_format=QUERY_FORMATS["gme"],
            query_columns=["CHROM", "POS", "ID", "REF", "ALT", "OLD_REC", "SRC_ROW", "SRC_LOC", "GME_AF", "GME_NWA", "GME_NEA", "GME_AP", "GME_ISRAEL", "GME_SD", "GME_TP", "GME_CA"],
            parser=parse_gme_frame,
        )

        artifact_specs = [
            (clinvar_source, clinvar_frame, clinvar_report, "clinvar_normalized_brca_v1", "Normalized BRCA ClinVar artifact", "Normalize the ClinVar BRCA window with explicit raw-record lineage."),
            (gnomad_sources["genomes_chr13"], gnomad_genomes_frame, {"children": gnomad_genomes_reports}, "gnomad_genomes_normalized_brca_v1", "Normalized BRCA gnomAD genomes artifact", "Normalize the gnomAD genomes BRCA windows and keep genomes separate from exomes."),
            (gnomad_sources["exomes_chr13"], gnomad_exomes_frame, {"children": gnomad_exomes_reports}, "gnomad_exomes_normalized_brca_v1", "Normalized BRCA gnomAD exomes artifact", "Normalize the gnomAD exomes BRCA windows and keep exomes separate from genomes."),
            (shgp_source, shgp_frame, shgp_report, "shgp_normalized_brca_v1", "Normalized BRCA SHGP artifact", "Normalize the Saudi BRCA rows after explicit table-to-VCF conversion."),
            (gme_source, gme_frame, gme_report, "gme_normalized_brca_v1", "Normalized BRCA GME artifact", "Normalize the GME BRCA rows after explicit table-to-VCF conversion."),
        ]

        normalized_workflow_artifacts: list[WorkflowArtifact] = []
        normalization_report = {
            "generated_at": TIMESTAMP_UTC,
            "snapshot_date": SNAPSHOT_DATE,
            "run_id": RUN_ID,
            "reference": reference_meta,
            "source_reports": [],
        }

        for source_obj, frame, report, artifact_key, title, summary in artifact_specs:
            prefix = artifact_prefix(artifact_key)
            parquet_path = temp_dir / f"{artifact_key}.parquet"
            manifest_path = temp_dir / f"{artifact_key}_manifest.json"
            save_parquet(frame, parquet_path)
            parquet_uri = upload_file(storage_client, parquet_path, f"{prefix}/{artifact_key}.parquet", content_type="application/octet-stream")
            report_path = temp_dir / f"{artifact_key}_report.json"
            json_dump(report_path, report)
            report_uri = upload_file(storage_client, report_path, f"{prefix}/{artifact_key}_report.json", content_type="application/json")
            sample_columns = [column for column in ["variant_key", "gene", "chrom38", "pos38", "ref_norm", "alt_norm", "source_record_locator", "source_artifact_uri"] if column in frame.columns]
            sample = compact_rows(frame[sample_columns] if sample_columns else frame)
            sample["source_counts"] = {}
            manifest = build_source_manifest(source_obj, frame, report_uri, parquet_uri, sample)
            json_dump(manifest_path, manifest)
            manifest_uri = upload_file(storage_client, manifest_path, f"{prefix}/{artifact_key}_manifest.json", content_type="application/json")
            normalization_report["source_reports"].append({**report, "parquet_uri": parquet_uri, "manifest_uri": manifest_uri})
            normalized_workflow_artifacts.append(
                WorkflowArtifact(
                    key=artifact_key.replace("_v1", ""),
                    title=title,
                    stage="normalized_source",
                    storage_uri=parquet_uri,
                    row_count=int(len(frame)),
                    local_parquet=parquet_path,
                    local_manifest=manifest_path,
                    sample=sample,
                    columns=source_columns(),
                    summary=summary,
                    notes=[
                        f"Source artifact: {source_obj.source_artifact_uri}",
                        f"Normalization report: {report_uri}",
                    ],
                )
            )

        clinvar_agg = aggregate_clinvar(clinvar_frame)
        gnomad_genomes_agg = aggregate_gnomad(gnomad_genomes_frame, prefix="genomes")
        gnomad_exomes_agg = aggregate_gnomad(gnomad_exomes_frame, prefix="exomes")
        shgp_agg = aggregate_shgp(shgp_frame)
        gme_agg = aggregate_gme(gme_frame)

        pre_gme_checkpoint = build_checkpoint(
            clinvar=clinvar_agg,
            gnomad_genomes=gnomad_genomes_agg,
            gnomad_exomes=gnomad_exomes_agg,
            shgp=shgp_agg,
            gme=None,
            stage_label="pre_gme_arab_checkpoint",
        )
        final_checkpoint = build_checkpoint(
            clinvar=clinvar_agg,
            gnomad_genomes=gnomad_genomes_agg,
            gnomad_exomes=gnomad_exomes_agg,
            shgp=shgp_agg,
            gme=gme_agg,
            stage_label="final_arab_checkpoint",
        )

        checkpoint_report = {
            "generated_at": TIMESTAMP_UTC,
            "run_id": RUN_ID,
            "snapshot_date": SNAPSHOT_DATE,
            "pre_gme_row_count": int(len(pre_gme_checkpoint)),
            "final_row_count": int(len(final_checkpoint)),
            "pre_gme_variant_keys": int(pre_gme_checkpoint["VARIANT_KEY"].nunique()),
            "final_variant_keys": int(final_checkpoint["VARIANT_KEY"].nunique()),
            "source_counts": {
                "clinvar": int(clinvar_agg.shape[0]),
                "gnomad_genomes": int(gnomad_genomes_agg.shape[0]),
                "gnomad_exomes": int(gnomad_exomes_agg.shape[0]),
                "shgp": int(shgp_agg.shape[0]),
                "gme": int(gme_agg.shape[0]),
            },
            "notes": [
                "GNOMAD_ALL_AF and GNOMAD_MID_AF are derived metrics built from explicit genomes/exomes counts inside this project checkpoint; they are not copied from a single upstream field.",
                "The final checkpoint intentionally carries only the Arab-relevant GME extras (AF, NWA, NEA, AP, SD).",
                "Reserved publication-facing fields remain NULL until a documented source is added rather than being guessed.",
            ],
        }

        pre_key = "supervisor_variant_registry_brca_arab_pre_gme_v2"
        final_key = "supervisor_variant_registry_brca_arab_v2"
        pre_prefix = artifact_prefix(pre_key)
        final_prefix = artifact_prefix(final_key)
        pre_parquet = temp_dir / f"{pre_key}.parquet"
        final_parquet = temp_dir / f"{final_key}.parquet"
        final_csv = temp_dir / f"{final_key}.csv"
        checkpoint_report_path = temp_dir / "brca_checkpoint_report.json"
        save_parquet(pre_gme_checkpoint, pre_parquet)
        save_parquet(final_checkpoint, final_parquet)
        save_csv(final_checkpoint, final_csv)
        json_dump(checkpoint_report_path, checkpoint_report)

        pre_parquet_uri = upload_file(storage_client, pre_parquet, f"{pre_prefix}/{pre_key}.parquet", content_type="application/octet-stream")
        final_parquet_uri = upload_file(storage_client, final_parquet, f"{final_prefix}/{final_key}.parquet", content_type="application/octet-stream")
        final_csv_uri = upload_file(storage_client, final_csv, PUBLIC_FINAL_CSV_OBJECT, content_type="text/csv", make_public=True)
        checkpoint_report_uri = upload_file(storage_client, checkpoint_report_path, f"{final_prefix}/{final_key}_report.json", content_type="application/json")

        normalization_report_path = temp_dir / "brca_normalization_report.json"
        json_dump(normalization_report_path, normalization_report)
        normalization_report_uri = upload_file(storage_client, normalization_report_path, f"frozen/harmonized/normalization_report/snapshot_date={SNAPSHOT_DATE}/brca_normalization_report.json", content_type="application/json")

        pre_sample = compact_rows(pre_gme_checkpoint[[column for column in ["CHROM", "POS", "REF", "ALT", "GENE", "VARIANT_KEY", "CLNSIG", "GNOMAD_ALL_AF", "SHGP_AF"] if column in pre_gme_checkpoint.columns]])
        pre_sample["source_counts"] = {
            "clinvar": int(pre_gme_checkpoint["PRESENT_IN_CLINVAR"].sum()),
            "gnomad_genomes": int(pre_gme_checkpoint["PRESENT_IN_GNOMAD_GENOMES"].sum()),
            "gnomad_exomes": int(pre_gme_checkpoint["PRESENT_IN_GNOMAD_EXOMES"].sum()),
            "shgp": int(pre_gme_checkpoint["PRESENT_IN_SHGP"].sum()),
        }
        final_sample = compact_rows(final_checkpoint[[column for column in ["CHROM", "POS", "REF", "ALT", "GENE", "VARIANT_KEY", "CLNSIG", "GNOMAD_ALL_AF", "SHGP_AF", "GME_AF"] if column in final_checkpoint.columns]])
        final_sample["source_counts"] = {
            "clinvar": int(final_checkpoint["PRESENT_IN_CLINVAR"].sum()),
            "gnomad_genomes": int(final_checkpoint["PRESENT_IN_GNOMAD_GENOMES"].sum()),
            "gnomad_exomes": int(final_checkpoint["PRESENT_IN_GNOMAD_EXOMES"].sum()),
            "shgp": int(final_checkpoint["PRESENT_IN_SHGP"].sum()),
            "gme": int(final_checkpoint["PRESENT_IN_GME"].sum()),
        }

        pre_artifact = WorkflowArtifact(
            key="pre_gme_checkpoint",
            title=pre_key,
            stage="checkpoint",
            storage_uri=pre_parquet_uri,
            row_count=int(len(pre_gme_checkpoint)),
            local_parquet=pre_parquet,
            local_manifest=checkpoint_report_path,
            sample=pre_sample,
            columns=required_and_extra_glossary(),
            summary="Arab-aware checkpoint before GME: normalized ClinVar + gnomAD + SHGP.",
            notes=[f"Checkpoint report: {checkpoint_report_uri}", f"Normalization report: {normalization_report_uri}"],
        )
        final_artifact = WorkflowArtifact(
            key="final_checkpoint",
            title=final_key,
            stage="checkpoint",
            storage_uri=final_parquet_uri,
            row_count=int(len(final_checkpoint)),
            local_parquet=final_parquet,
            local_manifest=checkpoint_report_path,
            sample=final_sample,
            columns=required_and_extra_glossary(),
            summary="Final Arab-aware checkpoint after adding the supporting GME layer.",
            notes=[f"Checkpoint report: {checkpoint_report_uri}", f"Normalization report: {normalization_report_uri}", f"Final CSV: {final_csv_uri}"],
        )

        raw_cards = [
            raw_card(
                key="clinvar_raw_brca_window",
                title="ClinVar raw BRCA-window preview",
                source=clinvar_source,
                sample_frame=clinvar_raw_preview,
                summary="Untouched ClinVar evidence constrained to the BRCA windows before normalization-derived checkpoint aggregation.",
                notes=["Preview rows are sourced from the untouched ClinVar BRCA-window extraction before chromosome renaming or allele normalization.", f"Raw manifest: {CLINVAR_MANIFEST_URI}"],
            ),
            raw_card(
                key="gnomad_genomes_raw_brca_window",
                title="gnomAD genomes raw BRCA-window preview",
                source=gnomad_sources["genomes_chr13"],
                sample_frame=gnomad_genomes_raw_preview,
                summary="Untouched gnomAD genomes evidence constrained to the BRCA windows before checkpoint aggregation.",
                notes=["The source remains logically split across chr13 and chr17, but the preview is unified for review.", f"Raw source prefixes: {GNOMAD_SOURCE_URIS['genomes_chr13']['raw_uri']} and {GNOMAD_SOURCE_URIS['genomes_chr17']['raw_uri']}"],
            ),
            raw_card(
                key="gnomad_exomes_raw_brca_window",
                title="gnomAD exomes raw BRCA-window preview",
                source=gnomad_sources["exomes_chr13"],
                sample_frame=gnomad_exomes_raw_preview,
                summary="Untouched gnomAD exomes evidence constrained to the BRCA windows before checkpoint aggregation.",
                notes=["The source remains logically split across chr13 and chr17, but the preview is unified for review.", f"Raw source prefixes: {GNOMAD_SOURCE_URIS['exomes_chr13']['raw_uri']} and {GNOMAD_SOURCE_URIS['exomes_chr17']['raw_uri']}"],
            ),
            raw_card(
                key="shgp_raw_brca_window",
                title="SHGP raw BRCA-window preview",
                source=shgp_source,
                sample_frame=pd.DataFrame(shgp_rows)[["chrom38", "start", "end", "ref", "alt", "source_record_locator", "shgp_ac", "shgp_an", "shgp_af"]].rename(columns={"chrom38": "CHROM", "start": "POS", "end": "END", "ref": "REF", "alt": "ALT", "source_record_locator": "RAW_LOCATOR", "shgp_ac": "AC", "shgp_an": "AN", "shgp_af": "AF"}),
                summary="Untouched SHGP rows inside the BRCA windows before table-to-VCF conversion.",
                notes=["This is the first primary Arab frequency source now carried into the checkpoint workflow.", f"Raw manifest: {SHGP_MANIFEST_URI}"],
            ),
            raw_card(
                key="gme_raw_brca_window",
                title="GME raw BRCA-window preview",
                source=gme_source,
                sample_frame=pd.DataFrame(gme_rows)[["chrom38", "start", "end", "ref", "alt", "source_record_locator", "gme_af", "gme_nwa", "gme_nea", "gme_ap", "gme_sd"]].rename(columns={"chrom38": "CHROM", "start": "POS", "end": "END", "ref": "REF", "alt": "ALT", "source_record_locator": "RAW_LOCATOR", "gme_af": "GME_AF", "gme_nwa": "GME_NWA", "gme_nea": "GME_NEA", "gme_ap": "GME_AP", "gme_sd": "GME_SD"}),
                summary="Untouched GME rows inside the BRCA windows before table-to-VCF conversion.",
                notes=["GME remains a supporting source and is added only after the Arab pre-GME checkpoint is already reviewable.", f"Raw manifest: {GME_MANIFEST_URI}"],
            ),
        ]

        normalized_cards = [normalized_card(artifact=artifact) for artifact in normalized_workflow_artifacts]
        review_bundle = build_review_bundle(
            raw_cards=raw_cards,
            normalized_cards=normalized_cards,
            pre_gme_artifact=pre_artifact,
            final_artifact=final_artifact,
            normalization_report_uri=normalization_report_uri,
            checkpoint_report_uri=checkpoint_report_uri,
            final_csv_public_url=public_object_url(PUBLIC_FINAL_CSV_OBJECT),
        )

        source_entries = [
            source_review_entry(
                source=clinvar_source,
                project_fit="adopted_100",
                project_fit_label="Adopted 100%",
                project_fit_summary="Core input",
                project_fit_note="Primary clinical truth source and normalization anchor.",
                category="Global clinical classification anchor",
                coordinate_readiness="Genomic coordinates ready",
                liftover_decision="not_needed",
                normalization_decision="Completed in the current BRCA normalization pass with bcftools.",
                review_status="ready",
                next_action="Keep ClinVar locked as a core source for the next downstream master-dataset assembly.",
                sample=compact_rows(clinvar_frame[["variant_key", "gene", "chrom38", "pos38", "ref_norm", "alt_norm", "alleleid", "clnsig"]]),
                workflow_position={
                    "raw_stage": "Frozen raw BRCA-window preview is visible on the Raw page.",
                    "brca_stage": "Normalized in the current T003 pass with explicit OLD_REC lineage.",
                    "final_stage": "Included in both the Arab pre-GME checkpoint and the final Arab checkpoint.",
                    "included_in_current_final": True,
                },
                notes=[
                    "ClinVar rows were extracted from the frozen BRCA windows only, then normalized with bcftools against a GRCh38 chr13/chr17 reference.",
                    f"Normalization report: {normalization_report_uri}",
                ],
                artifact_links=[
                    {"label": "Raw manifest", "url": CLINVAR_MANIFEST_URI},
                    {"label": "Normalized Parquet", "url": normalized_workflow_artifacts[0].storage_uri},
                    {"label": "Normalization report", "url": normalization_report_uri},
                ],
                row_count=int(len(clinvar_frame)),
            ),
            source_review_entry(
                source=gnomad_sources["genomes_chr13"],
                project_fit="adopted_100",
                project_fit_label="Adopted 100%",
                project_fit_summary="Core input",
                project_fit_note="Primary global genome frequency baseline.",
                category="Global genome frequency baseline",
                coordinate_readiness="Genomic coordinates ready",
                liftover_decision="not_needed",
                normalization_decision="Completed in the current BRCA normalization pass with bcftools.",
                review_status="ready",
                next_action="Keep genomes separate from exomes downstream so combined frequency metrics stay auditable.",
                sample=compact_rows(gnomad_genomes_frame[["variant_key", "gene", "chrom38", "pos38", "ref_norm", "alt_norm", "af", "af_mid"]]),
                workflow_position={
                    "raw_stage": "Frozen raw BRCA-window preview is visible on the Raw page.",
                    "brca_stage": "Normalized in the current T003 pass with per-cohort BRCA artifacts.",
                    "final_stage": "Included in both the Arab pre-GME checkpoint and the final Arab checkpoint.",
                    "included_in_current_final": True,
                },
                notes=[
                    "Genome rows remain an independent normalized artifact and are not hidden inside a combined gnomAD blob.",
                    f"Normalization report: {normalization_report_uri}",
                ],
                artifact_links=[
                    {"label": "Normalized Parquet", "url": normalized_workflow_artifacts[1].storage_uri},
                    {"label": "Normalization report", "url": normalization_report_uri},
                ],
                row_count=int(len(gnomad_genomes_frame)),
            ),
            source_review_entry(
                source=gnomad_sources["exomes_chr13"],
                project_fit="adopted_100",
                project_fit_label="Adopted 100%",
                project_fit_summary="Core input",
                project_fit_note="Primary global exome frequency baseline.",
                category="Global exome frequency baseline",
                coordinate_readiness="Genomic coordinates ready",
                liftover_decision="not_needed",
                normalization_decision="Completed in the current BRCA normalization pass with bcftools.",
                review_status="ready",
                next_action="Keep exomes separate from genomes downstream so combined frequency metrics stay auditable.",
                sample=compact_rows(gnomad_exomes_frame[["variant_key", "gene", "chrom38", "pos38", "ref_norm", "alt_norm", "af", "af_mid"]]),
                workflow_position={
                    "raw_stage": "Frozen raw BRCA-window preview is visible on the Raw page.",
                    "brca_stage": "Normalized in the current T003 pass with per-cohort BRCA artifacts.",
                    "final_stage": "Included in both the Arab pre-GME checkpoint and the final Arab checkpoint.",
                    "included_in_current_final": True,
                },
                notes=[
                    "Exome rows remain an independent normalized artifact and are not hidden inside a combined gnomAD blob.",
                    f"Normalization report: {normalization_report_uri}",
                ],
                artifact_links=[
                    {"label": "Normalized Parquet", "url": normalized_workflow_artifacts[2].storage_uri},
                    {"label": "Normalization report", "url": normalization_report_uri},
                ],
                row_count=int(len(gnomad_exomes_frame)),
            ),
            source_review_entry(
                source=shgp_source,
                project_fit="adopted_100",
                project_fit_label="Adopted 100%",
                project_fit_summary="Core input",
                project_fit_note="Primary Arab population-frequency source for the current workflow.",
                category="Arab population-frequency baseline",
                coordinate_readiness="Genomic coordinates ready",
                liftover_decision="not_needed",
                normalization_decision="Completed in the current BRCA normalization pass after explicit table-to-VCF conversion.",
                review_status="ready",
                next_action="Carry SHGP forward as the main Arab frequency comparator in the next downstream modeling stage.",
                sample=compact_rows(shgp_frame[["variant_key", "gene", "chrom38", "pos38", "ref_norm", "alt_norm", "shgp_ac", "shgp_an", "shgp_af", "source_record_locator"]]),
                workflow_position={
                    "raw_stage": "Frozen raw BRCA-window preview is visible on the Raw page.",
                    "brca_stage": "Converted to minimal VCF, normalized with bcftools, and kept with explicit table row lineage.",
                    "final_stage": "Included in both the Arab pre-GME checkpoint and the final Arab checkpoint.",
                    "included_in_current_final": True,
                },
                notes=[
                    "The Saudi table is now fully inside the active checkpoint workflow instead of being only a future candidate.",
                    f"Normalization report: {normalization_report_uri}",
                ],
                artifact_links=[
                    {"label": "Raw manifest", "url": SHGP_MANIFEST_URI},
                    {"label": "Normalized Parquet", "url": normalized_workflow_artifacts[3].storage_uri},
                    {"label": "Normalization report", "url": normalization_report_uri},
                ],
                row_count=int(len(shgp_frame)),
            ),
            source_review_entry(
                source=gme_source,
                project_fit="adopted_secondary",
                project_fit_label="Supporting source",
                project_fit_summary="Used as supporting evidence",
                project_fit_note="Supporting Arab/MENA frequency source added after the pre-GME checkpoint is already reviewable.",
                category="Arab / Middle Eastern summary-frequency layer",
                coordinate_readiness="Coordinates present but table-style, not VCF-style",
                liftover_decision="not_needed",
                normalization_decision="Completed in the current BRCA normalization pass after explicit table-to-VCF conversion.",
                review_status="partial",
                next_action="Keep GME as a supporting Arab layer and continue to prefer SHGP for the main Arab baseline.",
                sample=compact_rows(gme_frame[["variant_key", "gene", "chrom38", "pos38", "ref_norm", "alt_norm", "gme_af", "gme_ap", "source_record_locator"]]),
                workflow_position={
                    "raw_stage": "Frozen raw BRCA-window preview is visible on the Raw page.",
                    "brca_stage": "Converted to minimal VCF, normalized with bcftools, and kept with explicit table row lineage.",
                    "final_stage": "Included only in the final Arab checkpoint after the pre-GME checkpoint is already frozen.",
                    "included_in_current_final": True,
                },
                notes=[
                    "Only Arab-relevant GME subgroup fields are carried to the final checkpoint; context-only columns are left out deliberately.",
                    f"Normalization report: {normalization_report_uri}",
                ],
                artifact_links=[
                    {"label": "Raw manifest", "url": GME_MANIFEST_URI},
                    {"label": "Normalized Parquet", "url": normalized_workflow_artifacts[4].storage_uri},
                    {"label": "Normalization report", "url": normalization_report_uri},
                ],
                row_count=int(len(gme_frame)),
            ),
            {
                "source_key": "avdb_uae",
                "display_name": "AVDB Emirati workbook",
                "category": "Arab curated clinical-frequency workbook",
                "source_kind": "Workbook",
                "source_build": "GRCh37 workbook with GRCh38 liftover checkpoint",
                "coordinate_readiness": "Liftover checkpoint exists, but the current release has zero BRCA rows.",
                "liftover_decision": "required_and_completed",
                "normalization_decision": "Do not enter the current BRCA normalization checkpoint because BRCA rows are absent.",
                "brca_relevance": "Indirect",
                "review_status": "partial",
                "project_fit": "reference_only",
                "project_fit_note": "Scientifically valid as lifted reference evidence, but not part of the current BRCA checkpoint inputs.",
                "use_tier": "reference_only",
                "use_tier_label": "Reference only",
                "use_tier_summary": "Kept for context and audit",
                "snapshot_date": "2026-03-13",
                "source_version": "workbook-created-2025-06-27",
                "upstream_url": "https://avdb-arabgenome.ae/downloads",
                "raw_vault_prefix": "gs://mahmoud-arab-acmg-research-data/raw/sources/avdb_uae/version=workbook-created-2025-06-27/build=GRCh37/snapshot_date=2026-03-13/",
                "raw_manifest_uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/avdb_uae/version=workbook-created-2025-06-27/build=GRCh37/snapshot_date=2026-03-13/manifest.json",
                "row_count": 801,
                "notes": ["Liftover remains valid and frozen, but AVDB does not affect the current BRCA checkpoint because the workbook currently contributes zero BRCA rows."],
                "artifact_links": [],
                "workflow_position": {
                    "raw_stage": "Frozen workbook and liftover evidence stay visible for audit.",
                    "brca_stage": "Not used in the current BRCA normalization pass because BRCA rows are absent.",
                    "final_stage": "Excluded from both current checkpoints.",
                    "included_in_current_final": False,
                },
                "next_action": "Keep as reference-only unless a later non-BRCA Emirati analysis justifies full normalization.",
                "sample": None,
            },
            {
                "source_key": "uae_brca_pmc12011969",
                "display_name": "UAE BRCA supplement (PMC12011969)",
                "category": "Arab BRCA cohort supplement",
                "source_kind": "Supplementary workbook",
                "source_build": "GRCh38 claimed at column level",
                "coordinate_readiness": "Mutation-positive rows still need row-level coordinate parsing and allele validation.",
                "liftover_decision": "not_needed for valid hg38 rows",
                "normalization_decision": "Not yet entered because the allele parsing step is still incomplete.",
                "brca_relevance": "Direct",
                "review_status": "partial",
                "project_fit": "demo_only",
                "project_fit_note": "Useful for scientific walkthroughs and later case-context review, not a core baseline.",
                "use_tier": "demo_only",
                "use_tier_label": "Demo only",
                "use_tier_summary": "Useful for walkthroughs, not a core evidence stream",
                "snapshot_date": "2026-03-12",
                "source_version": "pmc12011969-moesm1",
                "upstream_url": "https://pmc.ncbi.nlm.nih.gov/articles/PMC12011969/",
                "raw_vault_prefix": "gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=uae_brca_pmc12011969/",
                "raw_manifest_uri": "",
                "row_count": 83,
                "notes": ["Still outside the active checkpoint because the genomic allele parser for this supplement is not finished yet."],
                "artifact_links": [],
                "workflow_position": {
                    "raw_stage": "Frozen de-identified extract stays visible for audit.",
                    "brca_stage": "Awaiting row-level coordinate parsing before normalization can start.",
                    "final_stage": "Excluded from both current checkpoints.",
                    "included_in_current_final": False,
                },
                "next_action": "Complete the explicit allele parser before reconsidering entry into the checkpoint workflow.",
                "sample": None,
            },
            {
                "source_key": "saudi_breast_cancer_pmc10474689",
                "display_name": "Saudi breast cancer supplement (PMC10474689)",
                "category": "Arab clinical publication supplement",
                "source_kind": "Supplementary workbook",
                "source_build": "unknown at genomic-coordinate level",
                "coordinate_readiness": "Still HGVS-oriented without justified transcript-to-genome mapping.",
                "liftover_decision": "blocked pending transcript-to-genome mapping",
                "normalization_decision": "Blocked from normalization.",
                "brca_relevance": "Indirect",
                "review_status": "blocked",
                "project_fit": "blocked",
                "project_fit_note": "Frozen only until a justified genomic mapping strategy exists.",
                "use_tier": "blocked",
                "use_tier_label": "Blocked",
                "use_tier_summary": "Frozen only until scientific gaps are resolved",
                "snapshot_date": "2026-03-12",
                "source_version": "pmc10474689-moesm1",
                "upstream_url": "https://pmc.ncbi.nlm.nih.gov/articles/PMC10474689/",
                "raw_vault_prefix": "gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=saudi_breast_cancer_pmc10474689/",
                "raw_manifest_uri": "",
                "row_count": 38,
                "notes": ["No genomic-coordinate normalization was attempted because the retained extract still lacks a justified transcript-to-genome mapping path."],
                "artifact_links": [],
                "workflow_position": {
                    "raw_stage": "Frozen de-identified extract stays visible for audit.",
                    "brca_stage": "Blocked before normalization because genomic coordinates are still unresolved.",
                    "final_stage": "Excluded from both current checkpoints.",
                    "included_in_current_final": False,
                },
                "next_action": "Keep blocked until a transcript-to-genome mapping strategy is justified scientifically.",
                "sample": None,
            },
        ]
        source_review = build_source_review_json(sources=source_entries, decision_summary=build_decision_summary(source_entries))
        write_ui_files(review_bundle, source_review)
        update_overview_state()
        subprocess.run(["python3", "scripts/refresh_supervisor_review_bundle.py"], check=True, cwd=ROOT)

        upload_text(storage_client, f"frozen/review_bundle/snapshot_date={SNAPSHOT_DATE}/source_review.json", json.dumps(source_review, indent=2), content_type="application/json")

        print(json.dumps({
            "run_id": RUN_ID,
            "snapshot_date": SNAPSHOT_DATE,
            "normalization_report_uri": normalization_report_uri,
            "checkpoint_report_uri": checkpoint_report_uri,
            "pre_gme_parquet_uri": pre_parquet_uri,
            "final_parquet_uri": final_parquet_uri,
            "final_csv_uri": final_csv_uri,
            "final_csv_public_url": public_object_url(PUBLIC_FINAL_CSV_OBJECT),
            "pre_gme_row_count": len(pre_gme_checkpoint),
            "final_row_count": len(final_checkpoint),
        }, indent=2))


if __name__ == "__main__":
    main()
