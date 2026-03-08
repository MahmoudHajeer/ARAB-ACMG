"""Submit cloud-only gnomAD raw load jobs to BigQuery.

Each build handles one source file (cohort + chromosome) and writes one table:
- gnomad_v4_1_genomes_chr13_raw
- gnomad_v4_1_genomes_chr17_raw
- gnomad_v4_1_exomes_chr13_raw
- gnomad_v4_1_exomes_chr17_raw
"""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
import time
from pathlib import Path
from typing import Final

PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET: Final[str] = "mahmoud-arab-acmg-research-data"
DATASET: Final[str] = "arab_acmg_raw"
DEFAULT_WINDOW_BP: Final[int] = 10_000_000
ROOT: Final[Path] = Path(__file__).resolve().parents[1]
BUILD_CONFIG: Final[Path] = ROOT / "cloudbuild" / "gnomad_raw_to_bq.yaml"
DEFAULT_TARGETS: Final[dict[str, tuple[str, str]]] = {
    "genomes_chr13": ("genomes", "chr13"),
    "genomes_chr17": ("genomes", "chr17"),
    "exomes_chr13": ("exomes", "chr13"),
    "exomes_chr17": ("exomes", "chr17"),
}
TERMINAL_STATUSES: Final[set[str]] = {
    "SUCCESS",
    "FAILURE",
    "CANCELLED",
    "TIMEOUT",
    "INTERNAL_ERROR",
    "EXPIRED",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Submit 1..4 cloud jobs to load gnomAD raw data into BigQuery, "
            "one table per cohort/chromosome."
        )
    )
    parser.add_argument(
        "--snapshot-date",
        default=dt.date.today().isoformat(),
        help="Snapshot date used in GCS raw paths (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--window-bp",
        type=int,
        default=DEFAULT_WINDOW_BP,
        help="Chunk window size in base pairs.",
    )
    parser.add_argument(
        "--target",
        action="append",
        choices=sorted(DEFAULT_TARGETS),
        help=(
            "Specific target job key. Repeat for multiple jobs. "
            "Default: submit all 4 targets."
        ),
    )
    parser.add_argument(
        "--keep-working",
        action="store_true",
        help="Keep temporary raw/working transcoded objects after successful load.",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait until all submitted builds finish and return failure on any failed build.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=30,
        help="Polling interval in seconds when --wait is enabled.",
    )
    return parser.parse_args()


def submit_cloud_build(
    cohort: str,
    chrom: str,
    snapshot_date: str,
    window_bp: int,
    clean_working: bool,
    run_async: bool,
) -> str | None:
    if not BUILD_CONFIG.exists():
        print(f"❌ Missing build config: {BUILD_CONFIG}")
        return None

    substitutions = (
        f"_BUCKET={BUCKET},"
        f"_DATASET={DATASET},"
        f"_SNAPSHOT_DATE={snapshot_date},"
        f"_COHORT={cohort},"
        f"_CHROM={chrom},"
        f"_WINDOW_BP={window_bp},"
        f"_CLEAN_WORKING={'true' if clean_working else 'false'}"
    )

    cmd = [
        "gcloud",
        "builds",
        "submit",
        "--project",
        PROJECT_ID,
        "--config",
        str(BUILD_CONFIG),
        "--substitutions",
        substitutions,
    ]
    if run_async:
        cmd.extend(["--async", "--format=value(id)"])

    print("--- gnomAD Raw BigQuery Cloud Job ---")
    print(f"target: {cohort}_{chrom}")
    print(f"table: {DATASET}.gnomad_v4_1_{cohort}_{chrom}_raw")
    print(f"snapshot_date: {snapshot_date}")
    print(f"window_bp: {window_bp}")
    print(f"clean_working: {clean_working}")
    print(f"async: {run_async}")
    print(f"cmd: {' '.join(cmd)}")

    if run_async:
        completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
        if completed.returncode != 0:
            print(completed.stdout)
            print(completed.stderr)
            return None
        lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
        if not lines:
            print("Unable to parse build ID from gcloud output:")
            print(completed.stdout)
            return None
        build_id = lines[-1]
        print(f"build_id: {build_id}")
        return build_id

    completed = subprocess.run(cmd, check=False)
    return "sync-success" if completed.returncode == 0 else None


def build_status(build_id: str) -> str:
    cmd = [
        "gcloud",
        "builds",
        "describe",
        build_id,
        "--project",
        PROJECT_ID,
        "--format=value(status)",
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        return "UNKNOWN"
    return completed.stdout.strip()


def wait_for_builds(build_ids: list[str], poll_seconds: int) -> bool:
    pending = set(build_ids)
    latest_status: dict[str, str] = {}

    while pending:
        for build_id in list(pending):
            status = build_status(build_id)
            if latest_status.get(build_id) != status:
                print(f"build {build_id}: {status}")
                latest_status[build_id] = status
            if status in TERMINAL_STATUSES:
                pending.remove(build_id)
        if pending:
            time.sleep(poll_seconds)

    failed = [
        build_id for build_id, status in latest_status.items() if status != "SUCCESS"
    ]
    if failed:
        print(f"❌ Failed builds: {', '.join(failed)}")
        return False

    print("✅ All submitted builds finished successfully.")
    return True


def main() -> None:
    args = parse_args()
    selected_keys = args.target or list(DEFAULT_TARGETS.keys())

    build_ids: list[str] = []
    for key in selected_keys:
        cohort, chrom = DEFAULT_TARGETS[key]
        build_id = submit_cloud_build(
            cohort=cohort,
            chrom=chrom,
            snapshot_date=args.snapshot_date,
            window_bp=args.window_bp,
            clean_working=not args.keep_working,
            run_async=True,
        )
        if build_id is None:
            sys.exit(1)
        build_ids.append(build_id)

    if args.wait:
        ok = wait_for_builds(build_ids=build_ids, poll_seconds=args.poll_seconds)
        sys.exit(0 if ok else 1)

    print("Submitted builds:")
    for build_id in build_ids:
        print(f"- {build_id}")
    sys.exit(0)


if __name__ == "__main__":
    main()
