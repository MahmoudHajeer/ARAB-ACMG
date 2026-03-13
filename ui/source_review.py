from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Final

SOURCE_REVIEW_FILE: Final[Path] = Path(__file__).resolve().parent / "source_review.json"


@lru_cache(maxsize=1)
def load_source_review_payload() -> dict[str, Any]:
    return json.loads(SOURCE_REVIEW_FILE.read_text(encoding="utf-8"))


def clear_source_review_cache() -> None:
    load_source_review_payload.cache_clear()
