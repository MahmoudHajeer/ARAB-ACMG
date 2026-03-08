"""Grant dataset-level public read access to the ARAB-ACMG BigQuery datasets."""

from __future__ import annotations

import sys
from typing import Final

from google.cloud import bigquery

PROJECT_ID: Final[str] = "genome-services-platform"
DATASETS: Final[tuple[str, ...]] = (
    "arab_acmg_raw",
    "arab_acmg_harmonized",
    "arab_acmg_results",
)
PUBLIC_READER: Final[bigquery.AccessEntry] = bigquery.AccessEntry(
    role="READER",
    entity_type="specialGroup",
    entity_id="allAuthenticatedUsers",
)


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)

    for dataset_id in DATASETS:
        dataset_ref = f"{PROJECT_ID}.{dataset_id}"
        print(f"--- [Public Access Stage]: {dataset_ref} ---")
        try:
            dataset = client.get_dataset(dataset_ref)
            entries = list(dataset.access_entries)
            if not any(
                entry.role == PUBLIC_READER.role
                and entry.entity_type == PUBLIC_READER.entity_type
                and entry.entity_id == PUBLIC_READER.entity_id
                for entry in entries
            ):
                entries.append(PUBLIC_READER)
                dataset.access_entries = entries
                dataset = client.update_dataset(dataset, ["access_entries"])
            is_public = any(
                entry.role == PUBLIC_READER.role
                and entry.entity_type == PUBLIC_READER.entity_type
                and entry.entity_id == PUBLIC_READER.entity_id
                for entry in dataset.access_entries
            )
            print(f"✅ [Stage Effect]: public_reader={is_public}")
        except Exception as exc:
            print(f"❌ [Stage Effect]: failed to update access. Error: {exc}")
            sys.exit(1)

    print("🎉 [Final Effect]: BigQuery datasets are configured for public read access.")


if __name__ == "__main__":
    main()
