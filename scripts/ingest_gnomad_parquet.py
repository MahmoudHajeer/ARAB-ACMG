# [AI-Agent: Codex]: Raw-as-is gnomAD snapshot pipeline.
# This script intentionally copies only required chromosomes (chr13, chr17)
# from both genomes and exomes, with no filtering and no column transformation.

import datetime
import json
import os
import subprocess
from typing import Final, Self

from google.cloud import storage

PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
SOURCE_NAME: Final[str] = "gnomad_v4.1"
SOURCE_VERSION: Final[str] = "4.1"

GNOMAD_OBJECTS: Final[list[dict[str, str]]] = [
    {
        "cohort": "genomes",
        "chrom": "chr13",
        "uri": "gs://gcp-public-data--gnomad/release/4.1/vcf/genomes/gnomad.genomes.v4.1.sites.chr13.vcf.bgz",
    },
    {
        "cohort": "genomes",
        "chrom": "chr17",
        "uri": "gs://gcp-public-data--gnomad/release/4.1/vcf/genomes/gnomad.genomes.v4.1.sites.chr17.vcf.bgz",
    },
    {
        "cohort": "exomes",
        "chrom": "chr13",
        "uri": "gs://gcp-public-data--gnomad/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr13.vcf.bgz",
    },
    {
        "cohort": "exomes",
        "chrom": "chr17",
        "uri": "gs://gcp-public-data--gnomad/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr17.vcf.bgz",
    },
]


def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gs_uri}")
    no_scheme = gs_uri[5:]
    bucket, object_name = no_scheme.split("/", 1)
    return bucket, object_name


class GnomadRawSnapshotPipeline:
    """
    Stage-driven raw snapshot pipeline for gnomAD.
    Output:
    - Byte-identical copies of required files in `raw/sources/...`
    - JSON manifest per copied object
    """

    def __init__(self) -> None:
        self.snapshot_date: str = datetime.date.today().isoformat()
        self.storage_client = storage.Client(project=PROJECT_ID)
        self.target_bucket = self.storage_client.bucket(BUCKET_NAME)
        self.copied_count: int = 0

    def build_target_prefix(self, cohort: str, chrom: str) -> str:
        return (
            f"raw/sources/{SOURCE_NAME}/release={SOURCE_VERSION}/"
            f"cohort={cohort}/chrom={chrom}/snapshot_date={self.snapshot_date}"
        )

    def copy_object_as_is(self, source_uri: str, target_prefix: str) -> str:
        """
        Stage 1: Copy upstream object byte-for-byte into project raw vault.
        Uses gsutil cp, which handles large cross-location objects correctly.
        """
        src_bucket_name, src_object_name = parse_gs_uri(source_uri)
        src_bucket = self.storage_client.bucket(src_bucket_name)
        src_blob = src_bucket.get_blob(src_object_name)
        if src_blob is None:
            raise RuntimeError(f"Source object not found: {source_uri}")

        target_name = f"{target_prefix}/{os.path.basename(src_object_name)}"
        target_uri = f"gs://{BUCKET_NAME}/{target_name}"
        target_blob = self.target_bucket.blob(target_name)

        if target_blob.exists(self.storage_client):
            print(f"ℹ️ [Raw Snapshot]: Target already exists, skipping copy: {target_uri}")
            return target_uri

        command = ["gsutil", "-m", "cp", source_uri, target_uri]
        subprocess.run(command, check=True)
        print(f"✅ [Raw Snapshot]: Copied {source_uri} -> {target_uri}")
        self.copied_count += 1
        return target_uri

    def write_manifest(self, source_uri: str, target_uri: str, target_prefix: str) -> None:
        """
        Stage 2: Write reproducibility metadata next to each copied raw object.
        """
        _, target_object_name = parse_gs_uri(target_uri)
        target_blob = self.target_bucket.get_blob(target_object_name)
        if target_blob is None:
            raise RuntimeError(f"Copied object missing: {target_uri}")

        manifest = {
            "source": SOURCE_NAME,
            "source_version": SOURCE_VERSION,
            "snapshot_date": self.snapshot_date,
            "upstream_url": source_uri,
            "license_notes": "gnomAD public release (see Broad gnomAD terms).",
            "sha256": "not_computed_server_side_copy",
            "gcs_uri": target_uri,
            "row_count": -1,
            "notes": (
                "Raw-as-is copy with no filtering or transformation. "
                f"crc32c={target_blob.crc32c}, md5_hash={target_blob.md5_hash}, "
                f"size={target_blob.size}, generation={target_blob.generation}"
            ),
        }

        manifest_blob = self.target_bucket.blob(f"{target_prefix}/manifest.json")
        manifest_blob.upload_from_string(json.dumps(manifest, indent=2), content_type="application/json")
        print(f"✅ [Manifest]: gs://{BUCKET_NAME}/{target_prefix}/manifest.json")

    def run(self) -> None:
        """
        Main execution:
        - For each required chromosome/cohort pair:
          1) copy `.vcf.bgz`
          2) copy `.vcf.bgz.tbi`
          3) write manifests
        """
        print("--- [gnomAD Raw Snapshot]: Starting raw-as-is copy for required chromosomes ---")
        for item in GNOMAD_OBJECTS:
            cohort = item["cohort"]
            chrom = item["chrom"]
            vcf_uri = item["uri"]
            tbi_uri = f"{vcf_uri}.tbi"
            target_prefix = self.build_target_prefix(cohort=cohort, chrom=chrom)

            print(f"--- [Source]: cohort={cohort}, chrom={chrom} ---")
            target_vcf_uri = self.copy_object_as_is(vcf_uri, target_prefix)
            self.write_manifest(vcf_uri, target_vcf_uri, target_prefix)

            target_tbi_uri = self.copy_object_as_is(tbi_uri, target_prefix)
            self.write_manifest(tbi_uri, target_tbi_uri, target_prefix)

        if self.copied_count == 0:
            raise SystemExit("❌ [Final Effect]: No gnomAD objects copied.")
        print(f"🎉 [Final Effect]: gnomAD raw-as-is snapshot completed ({self.copied_count} objects).")


if __name__ == "__main__":
    GnomadRawSnapshotPipeline().run()
