from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd
from msm.constants import ASSET_TYPE_BOND


def classify_valmer_asset_type(row: Mapping[str, Any]) -> str | None:
    """Classify Valmer rows supported by this package's bond registration workflow."""

    tipovalor = _clean_text(row.get("tipovalor"))
    emisora = _clean_text(row.get("emisora"))
    subyacente = _clean_text(row.get("subyacente"))
    monedaemision = _clean_text(row.get("monedaemision"))

    if (tipovalor, emisora) == ("BI", "CETES"):
        return ASSET_TYPE_BOND
    if (tipovalor, emisora) == ("M", "BONOS") and _numeric_gt(
        row.get("tasacupon"), 0
    ):
        return ASSET_TYPE_BOND
    if (tipovalor, emisora) == ("LD", "BONDESD"):
        return ASSET_TYPE_BOND
    if subyacente in {"TIIE28", "TIIE182", "TIIE91", "TIIE28 EQUIV 182"}:
        return ASSET_TYPE_BOND
    if subyacente in {"CETE28", "CETE182"}:
        return ASSET_TYPE_BOND
    if "Tasa TIIE Fondeo 1D" in subyacente:
        return ASSET_TYPE_BOND
    if tipovalor in {"I", "93", "92"} and monedaemision == "MPS":
        return ASSET_TYPE_BOND
    if emisora in {"BPAG91", "BPAG28", "BPA182"} and monedaemision == "MPS":
        return ASSET_TYPE_BOND
    return None


def _clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _numeric_gt(value: object, threshold: float) -> bool:
    if value is None or pd.isna(value):
        return False
    numeric = pd.to_numeric(value, errors="coerce")
    return bool(pd.notna(numeric) and float(numeric) > threshold)


__all__ = ["classify_valmer_asset_type"]
