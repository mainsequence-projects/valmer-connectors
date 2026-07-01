"""Reusable operational services for Valmer connector entry points."""

from __future__ import annotations

from typing import Any

__all__ = [
    "migration_command_lines",
    "run_mxn_government_curve_update",
    "run_tiie_irs_mxn_curve_update",
    "run_usd_sofr_curve_update",
    "run_vector_update",
    "validate_runtime",
]


def __getattr__(name: str) -> Any:
    if name == "migration_command_lines":
        from valmer_connectors.services.migrations import migration_command_lines

        return migration_command_lines
    if name == "run_tiie_irs_mxn_curve_update":
        from valmer_connectors.services.curve_update import run_tiie_irs_mxn_curve_update

        return run_tiie_irs_mxn_curve_update
    if name == "run_usd_sofr_curve_update":
        from valmer_connectors.services.curve_update import run_usd_sofr_curve_update

        return run_usd_sofr_curve_update
    if name == "run_mxn_government_curve_update":
        from valmer_connectors.services.curve_update import (
            run_mxn_government_curve_update,
        )

        return run_mxn_government_curve_update
    if name == "run_vector_update":
        from valmer_connectors.services.vector_update import run_vector_update

        return run_vector_update
    if name == "validate_runtime":
        from valmer_connectors.services.runtime_validation import validate_runtime

        return validate_runtime
    raise AttributeError(name)
