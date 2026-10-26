"""Valmer's curves and fixings move to instances in its own namespace (ms-markets ADR 0047).

``VALMER_TIME_SERIES_STORAGE`` selects the core Markets tables (default) or Valmer's
instances; producers, readers, the registry seed and the attachment follow it, and the
backfill copies the core rows the instances lack. Updaters are faked here, because
constructing one needs a MetaTables catalog.
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
    VALMER_TIME_SERIES_STORAGE_ENV,
    discount_curves_storage,
    index_fixings_storage,
    resolve_time_series_storage,
    valmer_discount_curves_instance,
    valmer_index_fixings_instance,
)


@pytest.fixture
def valmer_storage(monkeypatch):
    monkeypatch.setenv(VALMER_TIME_SERIES_STORAGE_ENV, "valmer")


def test_the_core_markets_tables_stay_the_default(monkeypatch) -> None:
    monkeypatch.delenv(VALMER_TIME_SERIES_STORAGE_ENV, raising=False)

    assert resolve_time_series_storage() == "markets"
    assert discount_curves_storage() is DiscountCurvesStorage
    assert index_fixings_storage() is IndexFixingsStorage


def test_valmer_storage_selects_the_instances(valmer_storage) -> None:
    assert discount_curves_storage() is valmer_discount_curves_instance()
    assert index_fixings_storage() is valmer_index_fixings_instance()
    assert discount_curves_storage().__table__.info["namespace"] == "valmer_connectors"


def test_an_unknown_storage_is_refused(monkeypatch) -> None:
    monkeypatch.setenv(VALMER_TIME_SERIES_STORAGE_ENV, "elsewhere")

    with pytest.raises(ValueError, match=VALMER_TIME_SERIES_STORAGE_ENV):
        resolve_time_series_storage()


def test_valmer_storage_attaches_no_markets_table(valmer_storage, monkeypatch) -> None:
    """With Valmer's own tables the runtime attaches nothing in mainsequence.markets."""

    from msm.api.remote import use_remote_backend
    from msm.models.registration import resolve_markets_meta_table_models

    from valmer_connectors.data_nodes.canonical_index_values import (
        MarketsDailyIndexValuesStorage,
        ValmerDailyIndexValuesStorage,
    )
    from valmer_connectors.instruments.bootstrap import _valmer_markets_models

    use_remote_backend(SimpleNamespace(call=lambda *args: pytest.fail("no row call")))
    models = _valmer_markets_models()

    assert valmer_discount_curves_instance() in models and ValmerDailyIndexValuesStorage in models
    assert DiscountCurvesStorage not in models and IndexFixingsStorage not in models
    assert MarketsDailyIndexValuesStorage not in models
    resolved = resolve_markets_meta_table_models(models)
    assert {model.__table__.info["namespace"] for model in resolved} == {"valmer_connectors"}


def test_valmer_owns_its_daily_index_value_table() -> None:
    from valmer_connectors.data_nodes.canonical_index_values import (
        MarketsDailyIndexValuesStorage,
        ValmerDailyIndexValuesStorage,
    )

    table = ValmerDailyIndexValuesStorage.__table__
    assert table.name == "valmer_connectors__index_values__t_1d"
    assert table.info["namespace"] == "valmer_connectors"
    assert table.info["identifier"] == "valmer_connectors.IndexValuesTS.1d"
    assert ValmerDailyIndexValuesStorage.__cadence__ == "1d"
    assert ValmerDailyIndexValuesStorage.__index_names__ == ["time_index", "index_identifier"]
    assert [column.name for column in table.columns] == [
        column.name for column in MarketsDailyIndexValuesStorage.__table__.columns
    ]
    assert {key.target_fullname for key in table.foreign_keys} == {
        "ms_markets__index.unique_identifier",
        "ms_markets__indexformuladefinition.uid",
    }
    assert MarketsDailyIndexValuesStorage.__table__.name == "ms_markets__index_values__t_1d"
    assert MarketsDailyIndexValuesStorage.__table__.info["namespace"] == "mainsequence.markets"


def test_daily_index_values_follow_the_switch(monkeypatch) -> None:
    from valmer_connectors.data_nodes.canonical_index_values import (
        DailyIndexValuesNode,
        MarketsDailyIndexValuesStorage,
        ValmerDailyIndexValuesStorage,
    )
    from valmer_connectors.data_nodes.curve_building import (
        sofr_curve_config,
        tiie_curve_config,
        xccy_curve_config,
    )

    monkeypatch.delenv(VALMER_TIME_SERIES_STORAGE_ENV, raising=False)
    assert storage.daily_index_values_storage() is MarketsDailyIndexValuesStorage
    assert DailyIndexValuesNode._required_output_table() is MarketsDailyIndexValuesStorage
    assert tiie_curve_config().quote_storage_table is MarketsDailyIndexValuesStorage

    monkeypatch.setenv(VALMER_TIME_SERIES_STORAGE_ENV, "valmer")
    assert storage.daily_index_values_storage() is ValmerDailyIndexValuesStorage
    # Every quote, FRED and Banxico policy producer writes the selected table.
    assert DailyIndexValuesNode._required_output_table() is ValmerDailyIndexValuesStorage
    for config in (tiie_curve_config(), sofr_curve_config(), xccy_curve_config()):
        assert config.quote_storage_table is ValmerDailyIndexValuesStorage


def test_the_old_daily_index_values_name_is_a_deprecated_alias() -> None:
    import valmer_connectors.data_nodes as data_nodes
    from valmer_connectors.data_nodes import canonical_index_values

    with pytest.warns(DeprecationWarning, match="ValmerDailyIndexValuesStorage"):
        alias = canonical_index_values.DailyIndexValuesStorage
    with pytest.warns(DeprecationWarning):
        package_alias = data_nodes.DailyIndexValuesStorage

    assert alias is package_alias is canonical_index_values.ValmerDailyIndexValuesStorage
    with pytest.raises(AttributeError):
        canonical_index_values.NoSuchStorage  # noqa: B018


def test_banxico_fixings_write_the_selected_storage(valmer_storage, monkeypatch) -> None:
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


def test_the_xccy_curve_reads_and_binds_upstream_curves_in_the_selected_storage(
    valmer_storage, monkeypatch
) -> None:
    from valmer_connectors.data_nodes import curve_building
    from valmer_connectors.services import curve_update

    built: list[dict[str, Any]] = []

    class FakeXccyNode:
        def __init__(self, **kwargs):
            built.append(kwargs)

        def run(self, **kwargs):
            return None

    monkeypatch.setattr(curve_update, "bootstrap_runtime", lambda: None)
    monkeypatch.setattr(curve_update, "configure_valmer_discount_curves_cadence", lambda: None)
    monkeypatch.setattr(curve_update, "ValmerUsdMxnCollateralDiscountCurveNode", FakeXccyNode)

    curve_update.run_usd_mxn_xccy_curve_update()

    (kwargs,) = built
    assert kwargs["output_table"] is valmer_discount_curves_instance()
    assert kwargs["curve_config"].curve_storage_table is valmer_discount_curves_instance()
    assert curve_building.xccy_curve_config().curve_storage_table is (
        valmer_discount_curves_instance()
    )


def test_the_registry_seed_binds_valmer_instances(valmer_storage, monkeypatch) -> None:
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
def published_index_values(monkeypatch):
    """The daily Index values Valmer publishes, without reading the MetaTables catalog."""

    monkeypatch.setattr(
        time_series_backfill,
        "daily_index_value_identifiers",
        lambda: ("BANXICO_POLICY_TARGET", QUOTE, "US_TREASURY_CMT_10Y"),
    )


def test_valmer_series_moves_name_valmer_tables_and_identities() -> None:
    from msm.storage_copies import TableSeriesMove

    from valmer_connectors.data_nodes.canonical_index_values import (
        MarketsDailyIndexValuesStorage,
        ValmerDailyIndexValuesStorage,
    )

    curves, fixings, index_values = time_series_backfill.valmer_series_moves()

    assert (
        index_values.core,
        index_values.instance,
        index_values.key_column,
        index_values.label,
    ) == (
        MarketsDailyIndexValuesStorage,
        ValmerDailyIndexValuesStorage,
        "index_identifier",
        "index_values",
    )
    assert tuple(index_values.keys) == ("BANXICO_POLICY_TARGET", QUOTE, "US_TREASURY_CMT_10Y")

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
    assert time_series_backfill.valmer_series_moves(["index_fixings"])[0].label == "index_fixings"
    with pytest.raises(ValueError, match="Unknown Valmer time series"):
        time_series_backfill.valmer_series_moves(["vector"])


def test_the_backfill_copies_history_then_fills_the_gaps(tables) -> None:
    """A second pass after the switch copies only what the core table got in between."""

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

    # The switch: the producers write d5 to the instance; the core table got d4 first.
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

    assert main(["time-series", "backfill", "--series", "index_fixings", "--dry-run"]) == 0
    assert main(["time-series", "backfill", "--rollback"]) == 0
    script = str(Path(__file__).resolve().parents[1] / "scripts" / "backfill_valmer_time_series.py")
    previous_argv = sys.argv
    sys.argv = [script, "--rollback"]
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

    assert [move.label for move in moves] == ["discount_curves", "index_fixings", "index_values"]


def test_the_control_plane_launches_the_rollback_with_its_flag() -> None:
    from valmer_connectors.control_plane.catalog import JOB_LAUNCH_PROFILES_BY_KEY
    from valmer_connectors.control_plane.service import _job_command_args

    profile = JOB_LAUNCH_PROFILES_BY_KEY["time-series-backfill"]

    assert _job_command_args(profile, {}) == []
    assert _job_command_args(profile, {"rollback": True}) == ["--rollback"]
    assert _job_command_args(profile, {"dry_run": True, "rollback": True}) == [
        "--dry-run",
        "--rollback",
    ]


def test_the_index_values_move_and_rollback(tables) -> None:
    from valmer_connectors.data_nodes.canonical_index_values import (
        MarketsDailyIndexValuesStorage,
        ValmerDailyIndexValuesStorage,
    )

    markets = tables.table(MarketsDailyIndexValuesStorage)
    valmer = tables.table(ValmerDailyIndexValuesStorage)
    markets[QUOTE] = {"d1", "d2"}
    markets["US_TREASURY_CMT_10Y"] = {"d1"}
    # Another producer's Index in the Markets table is not Valmer's to move.
    markets["SOMEONE_ELSES_INDEX"] = {"d1"}

    report = time_series_backfill.backfill_valmer_time_series(series=["index_values"])

    assert report["complete"] is True
    assert set(report["series"]["index_values"]["identities"]) == {
        "BANXICO_POLICY_TARGET",
        QUOTE,
        "US_TREASURY_CMT_10Y",
    }
    assert valmer[QUOTE] == {"d1", "d2"} and "SOMEONE_ELSES_INDEX" not in valmer

    valmer[QUOTE].add("d3")
    report = time_series_backfill.backfill_valmer_time_series(
        series=["index_values"], rollback=True
    )

    assert report["series"]["index_values"]["target_table"] == "ms_markets__index_values__t_1d"
    assert markets[QUOTE] == {"d1", "d2", "d3"}


def test_daily_index_value_identifiers_take_only_valmer_quotes_from_the_statistics(
    monkeypatch,
) -> None:
    monkeypatch.undo()  # drop the autouse fake of this function
    from valmer_connectors.data_nodes.canonical_index_values import (
        MarketsDailyIndexValuesStorage,
        ValmerDailyIndexValuesStorage,
    )

    statistics = {
        MarketsDailyIndexValuesStorage: {QUOTE: "t", "SOMEONE_ELSES_INDEX": "t"},
        ValmerDailyIndexValuesStorage: None,  # not registered yet
    }

    def registered(storage):
        progress = statistics[storage]
        if progress is None:
            raise LookupError("not registered")
        return SimpleNamespace(
            get_data_updates=lambda: SimpleNamespace(index_progress=progress)
        )

    monkeypatch.setattr("msm.storage_copies.registered_time_index_meta_table", registered)

    identifiers = time_series_backfill.daily_index_value_identifiers()

    assert QUOTE in identifiers and "SOMEONE_ELSES_INDEX" not in identifiers
    assert {"BANXICO_POLICY_TARGET", "US_TREASURY_CMT_10Y", "FED_FUNDS_TARGET_UPPER"} <= set(
        identifiers
    )
    assert list(identifiers) == sorted(identifiers)
