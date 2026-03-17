"""Runtime configuration shared by the active low-cost pipeline scripts."""

from __future__ import annotations

import os
from typing import Final

DEFAULT_PROJECT_ID: Final[str] = "genome-services-platform"
DEFAULT_BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"


def project_id() -> str:
    return os.environ.get("ARAB_ACMG_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT") or DEFAULT_PROJECT_ID


def bucket_name() -> str:
    return os.environ.get("ARAB_ACMG_BUCKET") or DEFAULT_BUCKET_NAME
