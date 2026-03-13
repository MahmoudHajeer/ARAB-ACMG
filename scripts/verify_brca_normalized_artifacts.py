"""Verify the frozen BRCA normalization artifacts and review bundle.

This verifier keeps the checks simple and explicit:
- required local UI bundle files exist
- referenced GCS artifacts exist
- checkpoint row counts are positive and logically ordered
- final CSV public URL is reachable
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from google.cloud import storage

ROOT = Path(__file__).resolve().parents[1]
UI_DIR = ROOT / "ui"
PROJECT_ID = "genome-services-platform"


def parse_gs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Unsupported URI: {uri}")
    bucket, object_name = uri[5:].split("/", 1)
    return bucket, object_name


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def blob_exists(client: storage.Client, uri: str) -> bool:
    bucket_name, object_name = parse_gs_uri(uri)
    return client.bucket(bucket_name).blob(object_name).exists(client)


def public_url_exists(url: str) -> bool:
    result = subprocess.run(["curl", "-I", "-sS", url], capture_output=True, text=True, check=True)
    status_line = result.stdout.splitlines()[0] if result.stdout else ""
    return "200" in status_line


def main() -> None:
    bundle = json.loads((UI_DIR / "review_bundle.json").read_text(encoding="utf-8"))
    source_review = json.loads((UI_DIR / "source_review.json").read_text(encoding="utf-8"))

    require(bundle["pre_gme"]["row_count"] > 0, "Pre-GME checkpoint must have rows.")
    require(bundle["registry"]["row_count"] > 0, "Final checkpoint must have rows.")
    require(bundle["registry"]["row_count"] >= bundle["pre_gme"]["row_count"], "Final checkpoint must not be smaller than pre-GME checkpoint.")

    storage_client = storage.Client(project=PROJECT_ID)
    gcs_uris = {
        bundle["artifacts"]["normalization_report_uri"],
        bundle["artifacts"]["checkpoint_report_uri"],
        bundle["pre_gme"]["table_ref"],
        bundle["registry"]["table_ref"],
    }
    for dataset in bundle["datasets"]["datasets"]:
        gcs_uris.add(dataset["table_ref"])
    for source in source_review["sources"]:
        for link in source.get("artifact_links", []):
            url = str(link.get("url", ""))
            if url.startswith("gs://"):
                gcs_uris.add(url)

    missing = sorted(uri for uri in gcs_uris if not blob_exists(storage_client, uri))
    require(not missing, f"Missing GCS artifacts: {missing}")

    require(public_url_exists(bundle["registry"]["csv_download_url"]), "Final CSV public URL did not return HTTP 200.")

    print(
        json.dumps(
            {
                "status": "ok",
                "pre_gme_row_count": bundle["pre_gme"]["row_count"],
                "final_row_count": bundle["registry"]["row_count"],
                "normalized_artifacts": len(bundle["datasets"]["datasets"]),
                "verified_gcs_artifacts": len(gcs_uris),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"verify_brca_normalized_artifacts failed: {exc}", file=sys.stderr)
        raise
