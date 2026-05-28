from __future__ import annotations

from threading import RLock
from typing import Any

from src.instruments.curve_bootstrap import bootstrap_valmer_curve_pricing

_LOCK = RLock()
_REGISTERED = False


def seed_defaults(**schema_kwargs: Any) -> dict[str, Any]:
    """Bootstrap static pricing MetaTable rows used by this project."""

    return bootstrap_valmer_curve_pricing(**schema_kwargs)


def register_all(*, override: bool = False, **schema_kwargs: Any) -> dict[str, Any] | None:
    """Initialize the current ms-markets pricing rows used by Valmer workflows."""

    global _REGISTERED

    with _LOCK:
        if _REGISTERED and not override:
            return None
        result = seed_defaults(**schema_kwargs)
        _REGISTERED = True
        return result


__all__ = ["register_all", "seed_defaults"]
