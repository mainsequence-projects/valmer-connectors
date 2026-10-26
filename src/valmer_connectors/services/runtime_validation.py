from __future__ import annotations

from typing import Any

from valmer_connectors.instruments.bootstrap import bootstrap_runtime


def validate_runtime(*, override: bool = True) -> dict[str, Any]:
    """Attach the runtime, seed the static registry rows and summarize them.

    Seeding is explicit since ``bootstrap_runtime`` stopped seeding by default;
    this command keeps doing it, because its summary is the seeded rows.
    """

    result = bootstrap_runtime(override=override, seed_static_rows=True)
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
