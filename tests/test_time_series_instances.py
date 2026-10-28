"""Valmer writes its curves, fixings and daily Index values to its own tables (ADR 0047).

Producers, readers, the registry seed and the attachment name Valmer's tables in
the ``valmer_connectors`` namespace; only the one-off backfill touches the core
Markets tables, copying the rows Valmer's tables lack (or back on rollback).
Updaters are faked here, because constructing one needs a MetaTables catalog.
"""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from msm_pricing.time_index.curves.storage import DiscountCurvesStorage
from msm_pricing.time_index.index_fixings.storage import IndexFixingsStorage

from valmer_connectors import storage
from valmer_connectors.services import time_series_backfill
from valmer_connectors.storage import (
    valmer_discount_curves_instance,
    valmer_index_fixings_instance,
)


def test_valmer_instances_live_in_its_namespace() -> None:
    curves, fixings = storage.valmer_time_series_instances()

    assert curves is valmer_discount_curves_instance()
    assert fixings is valmer_index_fixings_instance()
    assert curves.__table__.name == "valmer_connectors__discountcurvests__valmer"
    assert fixings.__table__.name == "valmer_connectors__indexfixingsts__valmer"
    assert {curves.__table__.info["namespace"], fixings.__table__.info["namespace"]} == {
        "valmer_connectors"
    }


def test_the_storage_module_has_no_selector() -> None:
    """No environment switch and no selector: Valmer's tables are named directly."""

    for name in (
        "VALMER_TIME_SERIES_STORAGE_ENV",
        "resolve_time_series_storage",
        "discount_curves_storage",
        "index_fixings_storage",
        "daily_index_values_storage",
    ):
        assert not hasattr(storage, name), name


def test_bootstrap_attaches_only_valmer_tables() -> None:
    """Under the Markets API the runtime attaches nothing in mainsequence.markets."""

    from msm.api.remote import use_remote_backend
    from msm.models.registration import resolve_markets_meta_table_models

    from valmer_connectors.data_nodes.canonical_index_values import ValmerDailyIndexValuesStorage
    from valmer_connectors.instruments.bootstrap import _valmer_markets_models

    use_remote_backend(SimpleNamespace(call=lambda *args: pytest.fail("no row call")))
    models = _valmer_markets_models()

    assert valmer_discount_curves_instance() in models and ValmerDailyIndexValuesStorage in models
    assert valmer_index_fixings_instance() in models
    assert DiscountCurvesStorage not in models and IndexFixingsStorage not in models
    resolved = resolve_markets_meta_table_models(models)
    assert {model.__table__.info["namespace"] for model in resolved} == {"valmer_connectors"}


def test_valmer_owns_its_daily_index_value_table() -> None:
    from msm.time_index.indices import configured_index_values_storage

    from valmer_connectors.data_nodes.canonical_index_values import ValmerDailyIndexValuesStorage

    table = ValmerDailyIndexValuesStorage.__table__
    assert table.name == "valmer_connectors__index_values__t_1d"
    assert table.info["namespace"] == "valmer_connectors"
    assert table.info["identifier"] == "valmer_connectors.IndexValuesTS.1d"
    assert ValmerDailyIndexValuesStorage.__cadence__ == "1d"
    assert ValmerDailyIndexValuesStorage.__index_names__ == ["time_index", "index_identifier"]
    # The same columns as the Markets daily Index-value contract.
    assert [column.name for column in table.columns] == [
        column.name for column in configured_index_values_storage(cadence="1d").__table__.columns
    ]
    assert {key.target_fullname for key in table.foreign_keys} == {
        "ms_markets__index.unique_identifier",
        "ms_markets__indexformuladefinition.uid",
    }


def test_daily_index_values_are_written_to_valmers_table() -> None:
    from valmer_connectors.data_nodes.canonical_index_values import (
        DailyIndexValuesNode,
        ValmerDailyIndexValuesStorage,
    )
    from valmer_connectors.data_nodes.curve_building import (
        sofr_curve_config,
        tiie_curve_config,
        xccy_curve_config,
    )

    # Every quote, FRED and Banxico policy producer writes Valmer's table, and the
    # curve builders read their quotes from it.
    assert DailyIndexValuesNode._required_output_table() is ValmerDailyIndexValuesStorage
    for config in (tiie_curve_config(), sofr_curve_config(), xccy_curve_config()):
        assert config.quote_storage_table is ValmerDailyIndexValuesStorage


def test_the_old_daily_index_values_name_is_gone() -> None:
    import valmer_connectors.data_nodes as data_nodes
    from valmer_connectors.data_nodes import canonical_index_values

    assert not hasattr(canonical_index_values, "DailyIndexValuesStorage")
    assert not hasattr(data_nodes, "DailyIndexValuesStorage")


def test_banxico_fixings_write_valmers_table(monkeypatch) -> None:
    from valmer_connectors.banxico import fixings

    built: dict[str, Any] = {}

    class FakeNode:
        def __init__(self, **kwargs):
            built.update(kwargs)

        def set_fixing_builders(self, builders):
            return self

        def run(self):
            built["ran"] = True

    monkeypatch.setattr(fixings, "BanxicoFixingsNode", FakeNode)
    monkeypatch.setattr(fixings, "make_banxico_fixing_builders", lambda **kwargs: {})
    monkeypatch.setattr("valmer_connectors.instruments.bootstrap.bootstrap_runtime", lambda: None)

    fixings.run_banxico_fixings_update(token="token", validate_metadata=False)

    assert built["output_table"] is valmer_index_fixings_instance()
    assert built["ran"] is True


def test_the_xccy_curve_reads_and_binds_upstream_curves_in_valmers_table(monkeypatch) -> None:
    from valmer_connectors.data_nodes import curve_building
    from valmer_connectors.services import curve_update

    built: list[dict[str, Any]] = []

    class FakeXccyNode:
        def __init__(self, **kwargs):
            built.append(kwargs)

        def run(self, **kwargs):
            return None

    monkeypatch.setattr(curve_update, "bootstrap_runtime", lambda: None)
    monkeypatch.setattr(curve_update, "ValmerUsdMxnCollateralDiscountCurveNode", FakeXccyNode)

    curve_update.run_usd_mxn_xccy_curve_update()

    (kwargs,) = built
    assert kwargs["output_table"] is valmer_discount_curves_instance()
    assert kwargs["curve_config"].curve_storage_table is valmer_discount_curves_instance()
    assert curve_building.xccy_curve_config().curve_storage_table is (
        valmer_discount_curves_instance()
    )


def test_the_registry_seed_binds_valmer_instances(monkeypatch) -> None:
    from msm_pricing.api import PricingMarketDataSet, PricingMarketDataSetBinding

    from valmer_connectors.instruments.curve_bootstrap import (
        upsert_valmer_market_data_source_bindings,
    )

    curves_uid, fixings_uid = uuid.uuid4(), uuid.uuid4()
    monkeypatch.setattr(
        valmer_discount_curves_instance(), "get_meta_table_uid", classmethod(lambda cls: curves_uid)
    )
    monkeypatch.setattr(
        valmer_index_fixings_instance(), "get_meta_table_uid", classmethod(lambda cls: fixings_uid)
    )
    for instance in storage.valmer_time_series_instances():
        # The registered identifier needs an attached runtime; the authored one matches it.
        monkeypatch.setattr(
            instance,
            "get_identifier",
            classmethod(lambda cls: storage.storage_table_identifier(cls)),
        )
    default_set = SimpleNamespace(uid=uuid.uuid4())
    monkeypatch.setattr(
        PricingMarketDataSet, "get_by_key", classmethod(lambda cls, key: default_set)
    )
    upserts: list[dict[str, Any]] = []
    monkeypatch.setattr(
        PricingMarketDataSetBinding,
        "upsert",
        classmethod(lambda cls, payload: upserts.append(payload) or SimpleNamespace(**payload)),
    )

    result = upsert_valmer_market_data_source_bindings()

    assert {payload["concept_key"]: payload["storage_table_uid"] for payload in upserts} == {
        "discount_curves": curves_uid,
        "interest_rate_index_fixings": fixings_uid,
    }
    assert {payload["storage_table_identifier"] for payload in upserts} == {
        "valmer_connectors.DiscountCurvesTS.valmer",
        "valmer_connectors.IndexFixingsTS.valmer",
    }
    assert result["market_data_set"] is default_set


class FakeTables:
    """Rows of each table as ``{key: {time_index, ...}}``, copied the way the shared copy does.

    It replaces ``msm.storage_copies.copy_time_index_rows``: it writes only the
    source rows the target lacks (identified by key and time), unless a key is
    told to fail, and reports the counts of both sides afterwards.
    """

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, set[str]]] = {}
        self.calls: list[tuple[str, str, bool]] = []
        self.unwritable: set[str] = set()

    def table(self, storage_class: type[Any]) -> dict[str, set[str]]:
        return self.rows.setdefault(storage_class.__table__.name, {})

    def copy(self, source, target, *, key_column, keys, dry_run=False, **_kwargs):
        from msm.storage_copies import KeyCopy, TimeIndexCopyReport

        self.calls.append((source.__table__.name, target.__table__.name, dry_run))
        source_rows, target_rows = self.table(source), self.table(target)
        results = []
        for key in keys:
            held = target_rows.setdefault(key, set())
            missing = source_rows.get(key, set()) - held
            copied = 0
            if not dry_run and key not in self.unwritable:
                held |= missing
                copied = len(missing)
            results.append(
                KeyCopy(
                    key=key,
                    source_rows=len(source_rows.get(key, set())),
                    target_rows=len(held),
                    missing_rows=len(source_rows.get(key, set()) - held),
                    copied_rows=copied,
                )
            )
        return TimeIndexCopyReport(
            source_table=source.__table__.name,
            target_table=target.__table__.name,
            key_column=key_column,
            keys=tuple(results),
            dry_run=dry_run,
        )


@pytest.fixture
def tables(monkeypatch) -> FakeTables:
    fake = FakeTables()
    monkeypatch.setattr("msm.storage_copies.copy_time_index_rows", fake.copy)
    return fake


QUOTE = "VALMER_CURVE_QUOTE.MXN.TIIE_OIS.1M.MID"


@pytest.fixture(autouse=True)
def attached(monkeypatch) -> list[list[Any]]:
    """The models each ``msm.start_engine`` call attached, without a MetaTables catalog."""

    import msm

    calls: list[list[Any]] = []
    monkeypatch.setattr(msm, "start_engine", lambda *, models, **_kwargs: calls.append(models))
    return calls


def test_valmer_series_moves_name_valmer_tables_and_identities(attached) -> None:
    from msm.storage_copies import TableSeriesMove

    curves, fixings = time_series_backfill.valmer_series_moves()

    assert all(isinstance(move, TableSeriesMove) for move in (curves, fixings))
    assert (curves.core, curves.instance, curves.key_column, curves.label) == (
        DiscountCurvesStorage,
        valmer_discount_curves_instance(),
        "curve_identifier",
        "discount_curves",
    )
    assert "VALMER_TIIE_OVERNIGHT" in curves.keys and len(curves.keys) == 4
    assert (fixings.core, fixings.instance, fixings.key_column) == (
        IndexFixingsStorage,
        valmer_index_fixings_instance(),
        "index_identifier",
    )
    assert set(fixings.keys) >= {"TIIE_28", "CETE_28"} and len(fixings.keys) == 7
    # The backfill is the only process that attaches the core Markets tables.
    assert attached == [
        [
            DiscountCurvesStorage,
            IndexFixingsStorage,
            valmer_discount_curves_instance(),
            valmer_index_fixings_instance(),
        ]
    ]
    assert time_series_backfill.valmer_series_moves(["index_fixings"])[0].label == "index_fixings"
    with pytest.raises(ValueError, match="Unknown Valmer time series"):
        time_series_backfill.valmer_series_moves(["vector"])
    # Daily Index values have no move: Valmer writes them only to its own table.
    with pytest.raises(ValueError, match="Unknown Valmer time series"):
        time_series_backfill.valmer_series_moves(["index_values"])


def test_the_backfill_copies_history_then_fills_the_gaps(tables) -> None:
    """A second pass copies only what the core table got before the producers moved."""

    core = tables.table(IndexFixingsStorage)
    instance = tables.table(valmer_index_fixings_instance())
    core["TIIE_28"] = {"d1", "d2", "d3"}

    report = time_series_backfill.backfill_valmer_time_series(series=["index_fixings"])

    tiie = report["series"]["index_fixings"]["identities"]["TIIE_28"]
    assert tiie == {
        "source_rows": 3,
        "target_rows": 3,
        "instance_rows": 3,
        "missing_rows": 0,
        "copied_rows": 3,
    }
    assert report["complete"] is True and report["series"]["index_fixings"]["status"] == "moved"

    # The release: the producers write d5 to Valmer's table; the core table got d4 first.
    instance["TIIE_28"].add("d5")
    core["TIIE_28"].add("d4")
    report = time_series_backfill.backfill_valmer_time_series(series=["index_fixings"])

    tiie = report["series"]["index_fixings"]["identities"]["TIIE_28"]
    assert tiie["copied_rows"] == 1 and tiie["instance_rows"] == 5 and tiie["missing_rows"] == 0
    assert instance["TIIE_28"] == {"d1", "d2", "d3", "d4", "d5"}


def test_a_dry_run_only_counts(tables) -> None:
    tables.table(DiscountCurvesStorage)["VALMER_TIIE_OVERNIGHT"] = {"d1", "d2"}

    report = time_series_backfill.backfill_valmer_time_series(dry_run=True)

    curve = report["series"]["discount_curves"]["identities"]["VALMER_TIIE_OVERNIGHT"]
    assert curve["missing_rows"] == 2 and curve["copied_rows"] == 0
    assert report["complete"] is False
    assert {dry_run for *_tables, dry_run in tables.calls} == {True}
    assert tables.table(valmer_discount_curves_instance())["VALMER_TIIE_OVERNIGHT"] == set()


def test_an_incomplete_copy_fails_the_backfill(tables) -> None:
    tables.table(IndexFixingsStorage)["CETE_28"] = {"d1"}
    tables.unwritable.add("CETE_28")

    with pytest.raises(RuntimeError, match="index_fixings.*CETE_28"):
        time_series_backfill.backfill_valmer_time_series()


def test_a_failed_copy_fails_the_backfill(monkeypatch) -> None:
    def copy(*args, **kwargs):
        raise LookupError("valmer_connectors__indexfixingsts__valmer is not registered")

    monkeypatch.setattr("msm.storage_copies.copy_time_index_rows", copy)

    with pytest.raises(RuntimeError, match="not registered"):
        time_series_backfill.backfill_valmer_time_series(series=["index_fixings"])


def test_the_rollback_copies_the_instance_rows_back_into_the_core_table(tables) -> None:
    core = tables.table(DiscountCurvesStorage)
    instance = tables.table(valmer_discount_curves_instance())
    core["VALMER_TIIE_OVERNIGHT"] = {"d1", "d2"}
    instance["VALMER_TIIE_OVERNIGHT"] = {"d1", "d2", "d3", "d4"}

    report = time_series_backfill.backfill_valmer_time_series(
        series=["discount_curves"], rollback=True
    )

    entry = report["series"]["discount_curves"]
    assert (entry["source_table"], entry["target_table"]) == (
        "valmer_connectors__discountcurvests__valmer",
        "ms_markets__discountcurvests",
    )
    assert entry["status"] == "rolled_back"
    assert entry["identities"]["VALMER_TIIE_OVERNIGHT"]["instance_rows"] == 4
    assert entry["identities"]["VALMER_TIIE_OVERNIGHT"]["copied_rows"] == 2
    assert core["VALMER_TIIE_OVERNIGHT"] == {"d1", "d2", "d3", "d4"}


def test_the_backfill_job_and_cli_forward_their_options(monkeypatch, capsys) -> None:
    import runpy
    import sys
    from pathlib import Path

    from valmer_connectors.cli.main import main

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        time_series_backfill,
        "backfill_valmer_time_series",
        lambda **kwargs: calls.append(kwargs) or {"complete": True},
    )

    assert main(["time-series", "backfill", "--series", "index_fixings"]) == 0
    assert main(["time-series", "backfill", "--execute", "--rollback"]) == 0
    script = str(Path(__file__).resolve().parents[1] / "scripts" / "backfill_valmer_time_series.py")
    previous_argv = sys.argv
    sys.argv = [script, "--execute", "--rollback"]
    try:
        with pytest.raises(SystemExit) as exit_info:
            runpy.run_path(script, run_name="__main__")
    finally:
        sys.argv = previous_argv

    assert exit_info.value.code == 0
    assert calls == [
        {"series": ["index_fixings"], "dry_run": True, "rollback": False},
        {"series": None, "dry_run": False, "rollback": True},
        {"series": None, "dry_run": False, "rollback": True},
    ]
    assert json.loads(capsys.readouterr().out.split("\n}\n")[0] + "\n}") == {"complete": True}


def test_msm_series_loads_the_valmer_moves() -> None:
    """``msm series move|rollback`` calls the module:callable the runbook names."""

    from msm_cli.rename_preflight import load_objects

    moves = load_objects("valmer_connectors.services.time_series_backfill:valmer_series_moves")

    assert [move.label for move in moves] == ["discount_curves", "index_fixings"]


def test_the_control_plane_launches_the_backfill_with_its_flags() -> None:
    from valmer_connectors.control_plane.catalog import JOB_LAUNCH_PROFILES_BY_KEY
    from valmer_connectors.control_plane.service import _job_command_args

    profile = JOB_LAUNCH_PROFILES_BY_KEY["time-series-backfill"]

    # Without ``execute`` the Job only counts the rows to copy.
    assert _job_command_args(profile, {}) == []
    assert _job_command_args(profile, {"execute": True}) == ["--execute"]
    assert _job_command_args(profile, {"execute": True, "rollback": True}) == [
        "--execute",
        "--rollback",
    ]
