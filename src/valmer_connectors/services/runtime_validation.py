from __future__ import annotations

from typing import Any

from valmer_connectors.instruments.bootstrap import bootstrap_runtime


def validate_runtime(*, override: bool = True) -> dict[str, Any]:
    """Bootstrap runtime tables and return a compact validation payload."""

    result = bootstrap_runtime(override=override)
    if result is None:
        return {
            "index_type": None,
            "indexes": [],
            "index_conventions": [],
            "curves": [],
        }

    return {
        "index_type": result["index_type"].index_type,
        "indexes": sorted(result["indexes"]),
        "index_conventions": sorted(result["index_conventions"]),
        "curves": sorted(result["curves"]),
    }
