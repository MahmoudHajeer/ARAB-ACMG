# [AI-Agent: Gemini 2.0 Flash]: This utility generates provenance manifests for raw data artifacts.
# It ensures every GCS upload is accompanied by standardized metadata and checksums.

import json
import hashlib
import datetime
from typing import Final, TypedDict

class ManifestData(TypedDict):
    source: str
    source_version: str
    snapshot_date: str
    upstream_url: str
    license_notes: str
    sha256: str
    gcs_uri: str
    row_count: int
    notes: str

class ManifestGenerator:
    """
    [AI-Agent: Gemini 2.0 Flash]: Manages provenance manifest creation.
    Goal: Automate metadata recording for research reproducibility.
    """

    @staticmethod
    def calculate_sha256(file_path: str) -> str:
        """
        [AI-Agent: Gemini 2.0 Flash]: Computes SHA256 checksum for data integrity.
        Effect: Returns the hexadecimal hash of the local file.
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    @staticmethod
    def create_manifest(
        source: str,
        source_version: str,
        upstream_url: str,
        local_file_path: str,
        gcs_uri: str,
        row_count: int = -1,
        license_notes: str = "TBD",
        notes: str = ""
    ) -> str:
        """
        [AI-Agent: Gemini 2.0 Flash]: Generates a JSON manifest string.
        Effect: Returns a serialized JSON object compliant with conductor/data-contracts.md.
        """
        manifest: ManifestData = {
            "source": source,
            "source_version": source_version,
            "snapshot_date": datetime.date.today().isoformat(),
            "upstream_url": upstream_url,
            "license_notes": license_notes,
            "sha256": ManifestGenerator.calculate_sha256(local_file_path),
            "gcs_uri": gcs_uri,
            "row_count": row_count,
            "notes": notes
        }
        return json.dumps(manifest, indent=2)

if __name__ == "__main__":
    # [AI-Agent: Gemini 2.0 Flash]: Self-test of manifest generation logic.
    print("--- [Manifest Utility]: Testing Manifest Generation ---")
    # This is just a placeholder test for now.
    pass
