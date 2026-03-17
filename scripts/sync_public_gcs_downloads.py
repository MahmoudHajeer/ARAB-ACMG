"""Publish and verify the safe public GCS objects shown in the supervisor UI.

This script avoids the all-or-nothing mistake of making the whole bucket
public. Only the allowlisted objects surfaced in the supervisor download center
are published anonymously. Restricted study workbooks stay private.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Final

try:
    from scripts.gcs_public_policy import attachment_header_value, is_public_safe_gcs_uri, parse_gs_uri, public_url_for_gs_uri
    from scripts.runtime_config import project_id
except ModuleNotFoundError:
    from gcs_public_policy import attachment_header_value, is_public_safe_gcs_uri, parse_gs_uri, public_url_for_gs_uri  # type: ignore[no-redef]
    from runtime_config import project_id  # type: ignore[no-redef]

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
REVIEW_BUNDLE_FILE: Final[Path] = ROOT / "ui" / "review_bundle.json"
REPORT_FILE: Final[Path] = ROOT / "logs" / "public_gcs_download_audit.json"
PROJECT_ID: Final[str] = project_id()


def collect_publicable_gcs_uris(bundle: dict[str, object]) -> list[str]:
    uris: set[str] = set()
    for group in bundle.get("artifact_catalog", {}).get("groups", []):
        for entry in group.get("entries", []):
            for file_item in entry.get("files", []):
                uri = str(file_item.get("gs_uri", ""))
                if uri and is_public_safe_gcs_uri(uri):
                    uris.add(uri)
    return sorted(uris)


def publish_object(uri: str) -> dict[str, str]:
    filename = Path(parse_gs_uri(uri)[1]).name
    subprocess.run(
        [
            "gcloud",
            "storage",
            "objects",
            "update",
            uri,
            f"--project={PROJECT_ID}",
            "--predefined-acl=publicRead",
            f"--content-disposition={attachment_header_value(uri)}",
            "--cache-control=public,max-age=3600",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return {"uri": uri, "public_url": public_url_for_gs_uri(uri), "filename": filename}


def head_request(url: str) -> str:
    return subprocess.run(
        ["curl", "-I", "-L", "--max-time", "30", url],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def verify_public_download(url: str) -> dict[str, object]:
    head = head_request(url)
    subprocess.run(
        ["curl", "-L", "--range", "0-127", "--max-time", "30", "-o", "/dev/null", url],
        check=True,
        capture_output=True,
        text=True,
    )
    status_line = next((line for line in head.splitlines() if line.startswith("HTTP/")), "HTTP/unknown")
    content_length = next((line.split(":", 1)[1].strip() for line in head.splitlines() if line.lower().startswith("content-length:")), "")
    disposition = next((line.split(":", 1)[1].strip() for line in head.splitlines() if line.lower().startswith("content-disposition:")), "")
    return {
        "status_line": status_line,
        "content_length": content_length,
        "content_disposition": disposition,
    }


def object_already_public(uri: str) -> bool:
    url = public_url_for_gs_uri(uri)
    try:
        head = head_request(url)
    except subprocess.CalledProcessError:
        return False
    filename = Path(parse_gs_uri(uri)[1]).name
    status_ok = any(line.startswith("HTTP/") and " 200 " in line for line in head.splitlines())
    disposition_ok = f'filename="{filename}"' in head
    return status_ok and disposition_ok


def sync_public_downloads() -> dict[str, object]:
    bundle = json.loads(REVIEW_BUNDLE_FILE.read_text(encoding="utf-8"))
    publicable_uris = collect_publicable_gcs_uris(bundle)
    results: list[dict[str, object]] = []
    for uri in publicable_uris:
        published = (
            {"uri": uri, "public_url": public_url_for_gs_uri(uri), "filename": Path(parse_gs_uri(uri)[1]).name}
            if object_already_public(uri)
            else publish_object(uri)
        )
        verification = verify_public_download(published["public_url"])
        results.append({**published, **verification})

    report = {
        "status": "ok",
        "count": len(results),
        "results": results,
    }
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return report


def main() -> None:
    print(json.dumps(sync_public_downloads(), indent=2))


if __name__ == "__main__":
    main()
