from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path

from valmer_connectors.control_plane.pipeline import run_pipeline


def main(_argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)
    run_pipeline(scripts_directory=Path(__file__).resolve().parent)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
