from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Final

CONTROLLED_ACCESS_FILE: Final[Path] = Path(__file__).resolve().parent / "controlled_access.json"


def load_controlled_access_payload() -> dict[str, Any]:
    return json.loads(CONTROLLED_ACCESS_FILE.read_text(encoding="utf-8"))


def clear_controlled_access_cache() -> None:
    return None
