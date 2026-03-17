from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Final

try:  # pragma: no cover
    from ui.traceability import enrich_review_bundle_trace
except ModuleNotFoundError:  # pragma: no cover
    from traceability import enrich_review_bundle_trace  # type: ignore[no-redef]

BUNDLE_FILE: Final[Path] = Path(__file__).resolve().parent / "review_bundle.json"


def load_review_bundle() -> dict[str, Any]:
    return enrich_review_bundle_trace(json.loads(BUNDLE_FILE.read_text(encoding="utf-8")))


def clear_review_bundle_cache() -> None:
    return None
