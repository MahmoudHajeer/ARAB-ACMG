from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.overview_data import write_bundled_overview_payload


def main() -> None:
    payload = write_bundled_overview_payload()
    print(
        "Wrote ui/overview_state.json "
        f"(generated_at={payload['generated_at']}, tracks={len(payload['tracks'])})"
    )


if __name__ == "__main__":
    main()
