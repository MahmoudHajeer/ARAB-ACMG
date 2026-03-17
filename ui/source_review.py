from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Final

try:  # pragma: no cover
    from ui.traceability import enrich_source_review_trace
except ModuleNotFoundError:  # pragma: no cover
    from traceability import enrich_source_review_trace  # type: ignore[no-redef]

SOURCE_REVIEW_FILE: Final[Path] = Path(__file__).resolve().parent / "source_review.json"


def load_source_review_payload() -> dict[str, Any]:
    return enrich_source_review_trace(json.loads(SOURCE_REVIEW_FILE.read_text(encoding="utf-8")))


def clear_source_review_cache() -> None:
    return None
