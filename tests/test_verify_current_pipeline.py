from __future__ import annotations

import pandas as pd
import pytest
from scripts.verify_current_pipeline import (
    CURVE_IDENTIFIERS,
    VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    _validate_curve_coverage,
    _validate_government_source,
)


def _government_source() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "time_index": "2026-08-19T23:59:59Z",
                "unique_identifier": "CETES-1",
                "tipovalor": "BI",
                "emisora": "CETES",
            },
            {
                "time_index": "2026-08-19T23:59:59Z",
                "unique_identifier": "BONO-1",
                "tipovalor": "M",
                "emisora": "BONOS",
            },
            {
                "time_index": "2026-08-20T23:59:59Z",
                "unique_identifier": "CETES-1",
                "tipovalor": "BI",
                "emisora": "CETES",
            },
            {
                "time_index": "2026-08-20T23:59:59Z",
                "unique_identifier": "BONO-1",
                "tipovalor": "M",
                "emisora": "BONOS",
            },
        ]
    )


def _curve_rows(*, include_second_government_date: bool) -> list[dict[str, str]]:
    rows = [
        {
            "time_index": "2026-08-20T23:59:59Z",
            "curve_identifier": identifier,
        }
        for identifier in CURVE_IDENTIFIERS
        if identifier != VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER
    ]
    rows.append(
        {
            "time_index": "2026-08-19T23:59:59Z",
            "curve_identifier": VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
        }
    )
    if include_second_government_date:
        rows.append(
            {
                "time_index": "2026-08-20T23:59:59Z",
                "curve_identifier": VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
            }
        )
    return rows


def test_verification_accepts_dynamic_government_date_coverage() -> None:
    source = _government_source()

    cetes, bonos = _validate_government_source(source)
    counts = _validate_curve_coverage(
        source,
        _curve_rows(include_second_government_date=True),
    )

    assert len(cetes) == 2
    assert len(bonos) == 2
    assert counts[VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER] == 2


def test_verification_reports_missing_government_curve_dates() -> None:
    with pytest.raises(
        RuntimeError,
        match=(
            "source_dates=2, curve_dates=1, missing_dates=1, extra_dates=0"
        ),
    ):
        _validate_curve_coverage(
            _government_source(),
            _curve_rows(include_second_government_date=False),
        )
