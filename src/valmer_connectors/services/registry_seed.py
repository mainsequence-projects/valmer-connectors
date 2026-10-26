"""The one explicit process that writes Valmer's static Markets registry rows.

``bootstrap_runtime()`` only attaches tables. The rows Valmer's producers
reference (the interest-rate IndexType, the Mexican and USD reference indexes,
their conventions, the four Valmer curves and their building details, the
default market-data set's curve selections and source bindings) are written
here, through the Markets API when the process selected it (ms-markets ADR
0047). It is idempotent. Run it once per Environment, after a release that
changes those definitions, and it is the first stage of the standard pipeline.
"""

from __future__ import annotations

from typing import Any

from valmer_connectors.instruments.bootstrap import bootstrap_runtime

__all__ = ["seed_valmer_registries", "summarize_seeded_registries"]


def seed_valmer_registries(**runtime_kwargs: Any) -> dict[str, Any]:
    """Attach the runtime and write Valmer's static registry rows."""

    result = bootstrap_runtime(override=True, seed_static_rows=True, **runtime_kwargs)
    if result is None:  # pragma: no cover - override=True always seeds
        raise RuntimeError("Valmer registry seeding did not run.")
    return result


def summarize_seeded_registries(result: dict[str, Any]) -> dict[str, Any]:
    """A JSON-friendly summary of what ``seed_valmer_registries`` wrote."""

    bindings = result.get("market_data_bindings") or {}
    return {
        "index_type": getattr(result.get("index_type"), "index_type", None),
        "indexes": sorted(result.get("indexes") or {}),
        "index_conventions": sorted(result.get("index_conventions") or {}),
        "curves": sorted(result.get("curves") or {}),
        "curve_building_details": sorted(result.get("curve_building_details") or {}),
        "curve_bindings": len(result.get("curve_bindings") or {}),
        "deprecated_curve_bindings_deleted": len(result.get("deprecated_curve_bindings") or {}),
        "market_data_set_uid": _uid(bindings.get("market_data_set")),
        "discount_curves_storage_table_uid": _storage_uid(bindings.get("discount_curves")),
        "interest_rate_index_fixings_storage_table_uid": _storage_uid(
            bindings.get("interest_rate_index_fixings")
        ),
    }


def _uid(row: Any) -> str | None:
    uid = getattr(row, "uid", None)
    return None if uid is None else str(uid)


def _storage_uid(binding: Any) -> str | None:
    uid = getattr(binding, "storage_table_uid", None)
    return None if uid is None else str(uid)
