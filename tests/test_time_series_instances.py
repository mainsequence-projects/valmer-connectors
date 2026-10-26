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

import pandas as pd
import pytest
from msm_pricing.time_index.curves.storage import DiscountCurvesStorage
from msm_pricing.time_index.index_fixings.storage import IndexFixingsStorage

from valmer_connectors import storage
from valmer_connectors.data_nodes import time_series_backfill
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


def test_valmer_storage_attaches_no_markets_curve_or_fixing_table(
    valmer_storage, monkeypatch
) -> None:
    """Only the daily Index values table still pulls Markets tables into the runtime."""

    from msm.api.remote import use_remote_backend
    from msm.models.registration import resolve_markets_meta_table_models

    from valmer_connectors.instruments.bootstrap import _valmer_markets_models

    use_remote_backend(SimpleNamespace(call=lambda *args: pytest.fail("no row call")))
    models = _valmer_markets_models()

    assert valmer_discount_curves_instance() in models
    assert DiscountCurvesStorage not in models and IndexFixingsStorage not in models
    resolved = {model.__name__ for model in resolve_markets_meta_table_models(models)}
    assert resolved & {"CurveTable", "DiscountCurvesStorage", "IndexFixingsStorage"} == set()
    assert {"IndexTable", "IndexFormulaDefinitionTable"} <= resolved


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


def _fake_tables(monkeypatch, *, source: list[dict[str, Any]], instance: list[dict[str, Any]]):
    """Serve compiled selects from two in-memory tables, keyed by physical name."""

    tables = {
        IndexFixingsStorage.__table__.name: source,
        valmer_index_fixings_instance().__table__.name: instance,
    }
    reads: list[str] = []

    def select(statement):
        (table,) = statement.get_final_froms()
        reads.append(table.name)
        rows = tables[table.name]
        compiled = statement.compile()
        params = compiled.params
        identity = next(value for key, value in params.items() if key.startswith("index_identifier"))
        rows = [row for row in rows if row["index_identifier"] == identity]
        times = next((value for key, value in params.items() if key.startswith("time_index")), None)
        if times is not None:
            wanted = {pd.Timestamp(value) for value in times}
            rows = [row for row in rows if pd.Timestamp(row["time_index"]) in wanted]
        columns = [column.name for column in statement.selected_columns]
        return [{name: row[name] for name in columns} for row in rows]

    monkeypatch.setattr(time_series_backfill, "_select", select)
    return reads


def test_the_backfill_reads_only_the_rows_an_instance_lacks(monkeypatch) -> None:
    day = pd.Timestamp("2026-10-01", tz="UTC")
    source = [
        {"time_index": (day + pd.Timedelta(days=offset)).isoformat(), "index_identifier": "TIIE_28", "rate": 0.07 + offset / 1000}
        for offset in range(3)
    ]
    instance = [dict(source[0])]
    _fake_tables(monkeypatch, source=source, instance=instance)

    missing = time_series_backfill.missing_time_indexes(
        source_table=IndexFixingsStorage,
        instance_table=valmer_index_fixings_instance(),
        identity_column="index_identifier",
        identity="TIIE_28",
    )
    rows = time_series_backfill.read_rows(
        IndexFixingsStorage,
        identity_column="index_identifier",
        identity="TIIE_28",
        time_indexes=missing,
    )

    assert missing == [day + pd.Timedelta(days=1), day + pd.Timedelta(days=2)]
    assert [row["rate"] for row in rows] == pytest.approx([0.071, 0.072])
    (plan,) = time_series_backfill.time_series_copy_plans(["index_fixings"])
    status = time_series_backfill.copy_status(plan)
    assert status["TIIE_28"] == {
        "source_rows": 3,
        "instance_rows": 1,
        "missing_rows": 2,
        "latest_source": (day + pd.Timedelta(days=2)).isoformat(),
        "latest_instance": day.isoformat(),
    }


def test_the_copy_updater_returns_the_missing_rows_in_the_instance_contract(monkeypatch) -> None:
    day = pd.Timestamp("2026-10-01", tz="UTC")
    source = [
        {"time_index": (day + pd.Timedelta(days=offset)).isoformat(), "index_identifier": "TIIE_28", "rate": 0.07}
        for offset in range(2)
    ]
    _fake_tables(monkeypatch, source=source, instance=[])
    updater = object.__new__(time_series_backfill.ValmerTimeSeriesCopyUpdater)
    updater.copy_config = time_series_backfill.ValmerTimeSeriesCopyConfiguration(
        series="index_fixings",
        identities=("TIIE_28",),
    )
    updater.page_size = 1
    updater._output_table = valmer_index_fixings_instance()

    frame = updater.update()

    assert list(frame.index.names) == ["time_index", "index_identifier"]
    assert len(frame) == 2 and list(frame.columns) == ["rate"]


def test_curve_metadata_read_back_as_text_is_decoded() -> None:
    frame = pd.DataFrame([{"metadata_json": json.dumps({"source": "valmer"})}, {"metadata_json": None}])

    decoded = time_series_backfill._decoded_json_columns(frame, valmer_discount_curves_instance())

    assert decoded["metadata_json"].tolist() == [{"source": "valmer"}, None]


def test_the_backfill_copies_every_series_and_fails_on_gaps(monkeypatch) -> None:
    import msm

    from valmer_connectors.services import time_series_backfill as service

    attached: list[Any] = []
    ran: list[tuple[str, Any, int]] = []

    class FakeCopyUpdater:
        def __init__(self, config, output_table):
            self.config, self.output_table = config, output_table

        def set_page_size(self, page_size):
            self.page_size = page_size
            return self

        def run(self, *, override_update_stats=None):
            # Empty statistics: the runner must not drop rows older than the
            # instance's latest observation, which is what a gap is.
            assert override_update_stats is not None
            assert override_update_stats.max_time_index_value is None
            ran.append((self.config.series, self.output_table, self.page_size))
            return (False, pd.DataFrame())

    statuses = {"discount_curves": 0, "index_fixings": 0}
    monkeypatch.setattr(msm, "start_engine", lambda **kwargs: attached.extend(kwargs["models"]))
    monkeypatch.setattr(service, "ValmerTimeSeriesCopyUpdater", FakeCopyUpdater)
    monkeypatch.setattr(
        service,
        "copy_status",
        lambda plan: {
            identity: {"missing_rows": statuses[plan.series]} for identity in plan.identities
        },
    )

    report = service.backfill_valmer_time_series(page_size=50)

    assert [series for series, _table, _size in ran] == ["discount_curves", "index_fixings"]
    assert [table for _series, table, _size in ran] == storage.valmer_time_series_instances()
    assert {size for *_rest, size in ran} == {50}
    assert DiscountCurvesStorage in attached and valmer_index_fixings_instance() in attached
    assert report["complete"] is True

    statuses["index_fixings"] = 3
    with pytest.raises(RuntimeError, match="index_fixings"):
        service.backfill_valmer_time_series()

    ran.clear()
    report = service.backfill_valmer_time_series(dry_run=True)
    assert ran == [] and report["complete"] is False and report["dry_run"] is True


def test_the_backfill_job_and_cli_forward_their_options(monkeypatch, capsys) -> None:
    import runpy
    import sys
    from pathlib import Path

    from valmer_connectors.cli.main import main
    from valmer_connectors.services import time_series_backfill as service

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        service,
        "backfill_valmer_time_series",
        lambda **kwargs: calls.append(kwargs) or {"complete": True},
    )

    assert main(["time-series", "backfill", "--series", "index_fixings", "--dry-run"]) == 0
    script = str(Path(__file__).resolve().parents[1] / "scripts" / "backfill_valmer_time_series.py")
    previous_argv = sys.argv
    sys.argv = [script, "--page-size", "25"]
    try:
        with pytest.raises(SystemExit) as exit_info:
            runpy.run_path(script, run_name="__main__")
    finally:
        sys.argv = previous_argv

    assert exit_info.value.code == 0
    assert calls == [
        {"series": ["index_fixings"], "page_size": 200, "dry_run": True},
        {"series": None, "page_size": 25, "dry_run": False},
    ]
    assert json.loads(capsys.readouterr().out.split("\n}\n")[0] + "\n}") == {"complete": True}
