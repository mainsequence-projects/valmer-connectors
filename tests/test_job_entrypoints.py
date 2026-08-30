from __future__ import annotations

import runpy
from pathlib import Path

from banxico import fixings
from banxico.settings import (
    TIIE_28_INDEX_IDENTIFIER,
    TIIE_91_INDEX_IDENTIFIER,
    TIIE_182_INDEX_IDENTIFIER,
    TIIE_OVERNIGHT_INDEX_IDENTIFIER,
)
from valmer_connectors.services import curve_update, vector_update

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _run_script(script_name: str) -> None:
    runpy.run_path(
        str(REPOSITORY_ROOT / "scripts" / script_name),
        run_name="__main__",
    )


def test_onedrive_vector_job_encodes_its_source(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(vector_update, "run_vector_update", lambda **kwargs: calls.append(kwargs))

    _run_script("update_vector_valmer_onedrive.py")

    assert calls == [{"source_kind": "onedrive-graph"}]


def test_metatable_vector_job_encodes_its_source_and_repository_config(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(vector_update, "run_vector_update", lambda **kwargs: calls.append(kwargs))

    _run_script("update_vector_valmer_metatable.py")

    assert calls == [
        {
            "source_kind": "metatable",
            "source_metatables_config_path": str(
                REPOSITORY_ROOT / "configs" / "valmer-metatable-sources.json"
            ),
        }
    ]


def test_tiie_fixings_job_encodes_the_four_vscode_indices(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        fixings,
        "run_banxico_fixings_update",
        lambda **kwargs: calls.append(kwargs),
    )

    _run_script("update_banxico_tiie_fixings.py")

    assert calls == [
        {
            "index_identifiers": (
                TIIE_OVERNIGHT_INDEX_IDENTIFIER,
                TIIE_28_INDEX_IDENTIFIER,
                TIIE_91_INDEX_IDENTIFIER,
                TIIE_182_INDEX_IDENTIFIER,
            )
        }
    ]


def test_xccy_rebuild_job_forces_current_rebuild(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        curve_update,
        "run_usd_mxn_xccy_curve_update",
        lambda **kwargs: calls.append(kwargs),
    )

    _run_script("rebuild_valmer_usd_mxn_xccy_curve.py")

    assert calls == [{"rebuild_current": True}]
