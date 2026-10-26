"""Seeding the static registry rows is one explicit process, not a bootstrap side effect."""

from __future__ import annotations

import json
import runpy
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from valmer_connectors.control_plane.pipeline import PIPELINE_SCRIPTS
from valmer_connectors.instruments import bootstrap

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def fresh_bootstrap(monkeypatch):
    monkeypatch.setattr(bootstrap, "_RUNTIME_ATTACHED", False)
    monkeypatch.setattr(bootstrap, "_STATIC_DEFAULTS_SEEDED", False)


def test_bootstrap_runtime_attaches_without_seeding_by_default(
    monkeypatch, fresh_bootstrap
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        bootstrap, "attach_runtime_defaults", lambda **kwargs: calls.append("attach") or {}
    )
    monkeypatch.setattr(
        bootstrap, "seed_static_defaults", lambda **kwargs: pytest.fail("seeding is explicit")
    )

    bootstrap.bootstrap_runtime()
    bootstrap.bootstrap_runtime()

    assert calls == ["attach"]


def test_seeding_is_requested_explicitly(monkeypatch, fresh_bootstrap) -> None:
    seeded: list[dict[str, Any]] = []
    monkeypatch.setattr(
        bootstrap, "seed_static_defaults", lambda **kwargs: seeded.append(kwargs) or {"curves": {}}
    )

    assert bootstrap.bootstrap_runtime(seed_static_rows=True, timeout=5) == {"curves": {}}
    assert seeded == [{"extra_markets_models": None, "timeout": 5}]


def _seed_result() -> dict[str, Any]:
    return {
        "index_type": SimpleNamespace(index_type="interest_rate"),
        "indexes": {"TIIE_28": object(), "CETE_28": object()},
        "index_conventions": {"TIIE_28": object()},
        "curves": {"VALMER_TIIE_OVERNIGHT": object()},
        "curve_building_details": {"VALMER_TIIE_OVERNIGHT": object()},
        "curve_bindings": {("projection", "TIIE_28", "mid"): object()},
        "deprecated_curve_bindings": {},
        "market_data_bindings": {
            "market_data_set": SimpleNamespace(uid="set-uid"),
            "discount_curves": SimpleNamespace(storage_table_uid="curves-uid"),
            "interest_rate_index_fixings": SimpleNamespace(storage_table_uid="fixings-uid"),
        },
    }


def test_registry_seed_job_seeds_through_the_markets_api(monkeypatch, capsys) -> None:
    from msm.client import LazyRemoteBackend

    from valmer_connectors.services import registry_seed

    selected: list[Any] = []

    def bootstrap_runtime(**kwargs):
        from msm.api.remote import active_remote_backend

        selected.append((kwargs, active_remote_backend()))
        return _seed_result()

    monkeypatch.setattr(registry_seed, "bootstrap_runtime", bootstrap_runtime)
    script = str(REPOSITORY_ROOT / "scripts" / "seed_valmer_registries.py")
    previous_argv = sys.argv
    sys.argv = [script]
    try:
        with pytest.raises(SystemExit) as exit_info:
            runpy.run_path(script, run_name="__main__")
    finally:
        sys.argv = previous_argv

    assert exit_info.value.code == 0
    ((kwargs, backend),) = selected
    assert kwargs == {"override": True, "seed_static_rows": True}
    assert isinstance(backend, LazyRemoteBackend)
    summary = json.loads(capsys.readouterr().out)
    assert summary["indexes"] == ["CETE_28", "TIIE_28"]
    assert summary["discount_curves_storage_table_uid"] == "curves-uid"


def test_cli_registries_seed_prints_the_summary(monkeypatch, capsys) -> None:
    from valmer_connectors.cli.main import main
    from valmer_connectors.services import registry_seed

    monkeypatch.setattr(registry_seed, "bootstrap_runtime", lambda **kwargs: _seed_result())

    assert main(["registries", "seed"]) == 0
    assert json.loads(capsys.readouterr().out)["curves"] == ["VALMER_TIIE_OVERNIGHT"]


def test_runtime_validate_seeds_explicitly(monkeypatch) -> None:
    from valmer_connectors.services import runtime_validation

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        runtime_validation,
        "bootstrap_runtime",
        lambda **kwargs: calls.append(kwargs) or _seed_result(),
    )

    assert runtime_validation.validate_runtime()["index_type"] == "interest_rate"
    assert calls == [{"override": True, "seed_static_rows": True}]


def test_the_standard_pipeline_seeds_before_any_producer() -> None:
    assert PIPELINE_SCRIPTS[0] == "seed_valmer_registries.py"
    assert (REPOSITORY_ROOT / "scripts" / PIPELINE_SCRIPTS[0]).is_file()
