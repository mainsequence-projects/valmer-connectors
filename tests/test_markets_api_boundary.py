"""Valmer reaches Markets registries through the Markets API (ms-markets ADR 0047).

Valmer's composition roots (the Job scripts, the CLI and the control-plane API)
select ms-markets' remote row backend; ``bootstrap_runtime()`` then attaches only
the tables Valmer writes itself, and every registry read or write is a set-based
``msm.api`` row call. Library code never selects the backend, so an application
that imports ``valmer_connectors`` keeps the backend it chose. The row classes are
replaced by recording fakes here, so these tests check what Valmer asks Markets for.
"""

from __future__ import annotations

import ast
import importlib
import runpy
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def no_discovery(monkeypatch):
    """Fail if anything looks the Markets API up before the first row call."""

    monkeypatch.setattr(
        "msm.client.MarketsClient.from_environment",
        classmethod(lambda cls: pytest.fail("the Markets API is found on the first row call")),
    )


def _selected_backend() -> Any:
    from msm.api.remote import active_remote_backend

    return active_remote_backend()


def test_the_cli_selects_the_markets_api(no_discovery, monkeypatch) -> None:
    from msm.client import LazyRemoteBackend

    from valmer_connectors.cli.main import main

    assert _selected_backend() is None
    assert main(["version"]) == 0
    assert isinstance(_selected_backend(), LazyRemoteBackend)


def test_job_scripts_select_the_markets_api(no_discovery, monkeypatch) -> None:
    from msm.client import LazyRemoteBackend

    from valmer_connectors.data_nodes import curve_quote_indices

    selected: list[Any] = []
    monkeypatch.setattr(
        curve_quote_indices,
        "run_valmer_irs_mxn_quote_update",
        lambda: selected.append(_selected_backend()),
    )
    script = str(REPOSITORY_ROOT / "scripts" / "update_valmer_irs_mxn_quotes.py")
    previous_argv = sys.argv
    sys.argv = [script]
    try:
        runpy.run_path(script, run_name="__main__")
    finally:
        sys.argv = previous_argv

    assert len(selected) == 1 and isinstance(selected[0], LazyRemoteBackend)


def test_every_markets_job_script_selects_the_markets_api() -> None:
    """Every Job entry point except the migration and the pipeline driver selects it.

    The migration Job only applies Valmer's provider, the pipeline driver runs the
    other scripts as subprocesses, the time-series backfill only copies table
    rows, and the operator-only pricing-details repair stays on the runtime
    backend (see docs/deployment.md, Markets API Access).
    """

    exempt = {
        "__init__.py",
        "migrate_valmer.py",
        "run_control_plane_pipeline.py",
        "patch_valmer_asset_pricing_details.py",
        "backfill_valmer_time_series.py",
        "test_script.py",
    }
    for path in sorted((REPOSITORY_ROOT / "scripts").glob("*.py")):
        if path.name in exempt:
            continue
        assert "use_markets_api()" in path.read_text(encoding="utf-8"), path.name


def test_the_control_plane_entry_module_selects_the_markets_api(
    no_discovery, monkeypatch
) -> None:
    import msm
    from msm.client import LazyRemoteBackend

    monkeypatch.setattr(msm, "start_engine", lambda **_kwargs: pytest.fail("nothing to attach"))
    monkeypatch.delitem(sys.modules, "api.valmer_control_plane.main", raising=False)
    try:
        importlib.import_module("api.valmer_control_plane.main")
    finally:
        sys.modules.pop("api.valmer_control_plane.main", None)

    assert isinstance(_selected_backend(), LazyRemoteBackend)


def test_library_imports_never_select_a_backend() -> None:
    """Importing Valmer's modules leaves the importer's backend choice alone."""

    for module in (
        "valmer_connectors.instruments.bootstrap",
        "valmer_connectors.instruments.asset_identity",
        "valmer_connectors.queries",
        "valmer_connectors.data_nodes.valmer_vector_storage",
        "valmer_connectors.meta_tables.valmer_asset_details",
    ):
        importlib.import_module(module)
    assert _selected_backend() is None


def _attach(monkeypatch) -> dict[str, Any]:
    import msm
    import msm_pricing.bootstrap

    from valmer_connectors.instruments import bootstrap

    attached: dict[str, Any] = {}
    monkeypatch.setattr(bootstrap, "_RUNTIME_ATTACHED", False)
    monkeypatch.setattr(
        msm, "start_engine", lambda **kwargs: attached.setdefault("models", kwargs["models"])
    )
    monkeypatch.setattr(
        msm_pricing.bootstrap,
        "attach_pricing_schemas",
        lambda **kwargs: attached.setdefault("pricing", kwargs["models"]),
    )
    monkeypatch.setattr(
        "valmer_connectors.meta_tables.valmer_asset_details.ensure_valmer_asset_detail_runtime",
        lambda **_kwargs: "details-context",
    )
    bootstrap.bootstrap_runtime(seed_static_rows=False, override=True)
    return attached


def test_bootstrap_attaches_only_valmer_tables_under_the_markets_api(monkeypatch) -> None:
    from msm.api.remote import use_remote_backend
    from msm.models.registration import resolve_markets_meta_table_models
    from msm_pricing.data_nodes.curves.storage import DiscountCurvesStorage
    from msm_pricing.data_nodes.index_fixings.storage import IndexFixingsStorage

    from valmer_connectors.data_nodes.canonical_index_values import DailyIndexValuesStorage
    from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
    from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable

    use_remote_backend(SimpleNamespace(call=lambda *args: pytest.fail("no row call")))
    attached = _attach(monkeypatch)

    assert attached["models"] == [
        ValmerAssetDetailsTable,
        ValmerVectorPricesStorage,
        DailyIndexValuesStorage,
        DiscountCurvesStorage,
        IndexFixingsStorage,
    ]
    assert "pricing" not in attached
    # The Markets registries are not attached: AssetTable is only the foreign-key
    # target of Valmer's tables. Index, IndexFormulaDefinition and Curve still are,
    # as targets of the Markets-namespace storages Valmer writes directly.
    resolved = {model.__name__ for model in resolve_markets_meta_table_models(attached["models"])}
    assert "AssetTable" not in resolved and "AssetTypeTable" not in resolved
    assert "AssetCurrentPricingDetailsTable" not in resolved


def test_bootstrap_keeps_the_runtime_attachment_for_runtime_importers(monkeypatch) -> None:
    """An application on the runtime backend still gets registries and pricing tables."""

    from msm.models import IndexDatasetAvailabilityTable

    from valmer_connectors.instruments.curve_bootstrap import valmer_pricing_runtime_models

    attached = _attach(monkeypatch)

    assert attached["models"][:4] == ["AssetType", "Asset", "IndexType", "Index"]
    assert IndexDatasetAvailabilityTable in attached["models"]
    assert attached["pricing"] == valmer_pricing_runtime_models()


def test_custom_indexes_are_upserted_in_two_row_calls(monkeypatch) -> None:
    from msm.api.indices import Index

    from valmer_connectors.instruments.index_registry import upsert_custom_indexes

    calls: list[tuple[str, Any]] = []
    existing = {"TIIE_28": SimpleNamespace(calculation_method="custom")}
    monkeypatch.setattr(
        Index,
        "get_many_by_unique_identifier",
        classmethod(lambda cls, identifiers: calls.append(("get", identifiers)) or existing),
    )
    monkeypatch.setattr(
        Index,
        "upsert_many",
        classmethod(
            lambda cls, values: calls.append(("upsert_many", values))
            or [SimpleNamespace(unique_identifier=value["unique_identifier"]) for value in values]
        ),
    )
    payloads = [
        {"unique_identifier": "TIIE_28", "calculation_method": "custom"},
        {"unique_identifier": "TIIE_91", "calculation_method": "custom"},
    ]

    assert set(upsert_custom_indexes(payloads)) == {"TIIE_28", "TIIE_91"}
    assert calls == [("get", ["TIIE_28", "TIIE_91"]), ("upsert_many", payloads)]


def test_custom_index_upsert_sends_each_identifier_once(monkeypatch) -> None:
    """A bulk upsert cannot touch one row twice; the last payload wins."""

    from msm.api.indices import Index

    from valmer_connectors.instruments.index_registry import upsert_custom_indexes

    sent: list[list[dict[str, Any]]] = []
    monkeypatch.setattr(
        Index, "get_many_by_unique_identifier", classmethod(lambda cls, identifiers: {})
    )
    monkeypatch.setattr(
        Index,
        "upsert_many",
        classmethod(
            lambda cls, values: sent.append(values)
            or [SimpleNamespace(unique_identifier=value["unique_identifier"]) for value in values]
        ),
    )

    upsert_custom_indexes(
        [
            {"unique_identifier": "X", "display_name": "first", "calculation_method": "custom"},
            {"unique_identifier": "X", "display_name": "last", "calculation_method": "custom"},
        ]
    )

    assert sent == [[{"unique_identifier": "X", "display_name": "last", "calculation_method": "custom"}]]


def test_custom_index_upsert_refuses_a_calculation_method_change(monkeypatch) -> None:
    from msm.api.indices import Index

    from valmer_connectors.instruments.index_registry import upsert_custom_indexes

    monkeypatch.setattr(
        Index,
        "get_many_by_unique_identifier",
        classmethod(lambda cls, identifiers: {"X": SimpleNamespace(calculation_method="formula")}),
    )
    monkeypatch.setattr(
        Index, "upsert_many", classmethod(lambda cls, values: pytest.fail("must not write"))
    )

    with pytest.raises(ValueError, match="calculation_method"):
        upsert_custom_indexes([{"unique_identifier": "X", "calculation_method": "custom"}])


def test_curve_quote_registration_is_set_based(monkeypatch) -> None:
    import pandas as pd
    from msm.api.indices import Index, IndexType

    from valmer_connectors.data_nodes.curve_quote_indices import (
        upsert_valmer_curve_quote_indexes,
    )

    calls: list[str] = []
    monkeypatch.setattr(
        IndexType,
        "upsert_many",
        classmethod(lambda cls, values: calls.append(f"types:{len(values)}") or []),
    )
    monkeypatch.setattr(
        Index,
        "get_many_by_unique_identifier",
        classmethod(lambda cls, identifiers: calls.append("get") or {}),
    )
    monkeypatch.setattr(
        Index,
        "upsert_many",
        classmethod(
            lambda cls, values: calls.append(f"indexes:{len(values)}")
            or [SimpleNamespace(unique_identifier=value["unique_identifier"]) for value in values]
        ),
    )
    metadata = {
        "source_family": "tiie_ois",
        "source_file": "IRS_MXN_CURVE.csv",
        "source_instrument_identifier": "Swap.1M",
        "quote_side": "mid",
    }
    frame = pd.DataFrame(
        [
            {"index_identifier": f"VALMER_CURVE_QUOTE.MXN.{tenor}", "metadata_json": metadata}
            for tenor in ("1M", "3M", "6M")
        ]
    )

    upsert_valmer_curve_quote_indexes(frame)

    assert calls == ["types:3", "get", "indexes:3"]


def test_default_bindings_are_read_not_written_under_the_markets_api(monkeypatch) -> None:
    from msm.api.remote import use_remote_backend
    from msm_pricing.api import PricingMarketDataSet, PricingMarketDataSetBinding

    from valmer_connectors.instruments.curve_bootstrap import (
        upsert_valmer_market_data_source_bindings,
    )

    use_remote_backend(SimpleNamespace(call=lambda *args: pytest.fail("faked")))
    default_set = SimpleNamespace(uid=uuid.uuid4())
    monkeypatch.setattr(
        PricingMarketDataSet, "get_by_key", classmethod(lambda cls, key: default_set)
    )
    monkeypatch.setattr(
        PricingMarketDataSet, "upsert", classmethod(lambda cls, *a, **k: pytest.fail("exists"))
    )
    monkeypatch.setattr(
        PricingMarketDataSetBinding, "upsert", classmethod(lambda cls, *a, **k: pytest.fail("read"))
    )
    bindings = [
        SimpleNamespace(concept_key="discount_curves"),
        SimpleNamespace(concept_key="interest_rate_index_fixings"),
    ]
    monkeypatch.setattr(
        PricingMarketDataSetBinding,
        "filter_for_set_and_concepts",
        classmethod(lambda cls, **kwargs: bindings),
    )

    result = upsert_valmer_market_data_source_bindings()

    assert result["market_data_set"] is default_set
    assert result["discount_curves"] is bindings[0]


# Markets tables (namespace mainsequence.markets) that a statement could target.
MARKETS_TABLE_CLASSES = {
    "AssetTable",
    "AssetTypeTable",
    "IndexTable",
    "IndexTypeTable",
    "IndexDatasetAvailabilityTable",
    "IndexFormulaInputTable",
    "IndexFormulaDefinitionTable",
    "AssetSnapshotsStorage",
    "AssetCurrentPricingDetailsTable",
    "AssetPricingDetailsStorage",
    "CurveTable",
    "CurveBuildingDetailsTable",
    "IndexConventionDetailsTable",
    "PricingMarketDataSetTable",
    "PricingMarketDataSetBindingTable",
    "PricingMarketDataSetCurveBindingTable",
    # Core time-series storages: read through valmer_connectors.storage, which
    # selects them or Valmer's own instances.
    "DiscountCurvesStorage",
    "IndexFixingsStorage",
}
STATEMENT_BUILDERS = {
    "compile_markets_statement",
    "execute_markets_operation",
    "bulk_upsert_model",
    "search_model",
    "upsert_model",
    "create_model",
    "count_model",
    "get_model_by_uid",
}
# Modules that still build statements on a Markets table, each for a reason
# documented in docs/deployment.md (Markets API Access).
REMAINING_DIRECT_ACCESS = {
    # Operator-only repair of stored pricing-detail payloads: no Markets route
    # reads timestamped pricing details or filters them by payload content.
    "src/valmer_connectors/services/pricing_details_repair.py",
    # One-off copy of Valmer's own curve and fixing rows out of the core tables
    # into Valmer's instances (docs/time-series-instances.md).
    "src/valmer_connectors/data_nodes/time_series_backfill.py",
}


def _python_modules() -> list[Path]:
    roots = ("src/valmer_connectors", "api", "dashboards", "scripts")
    return sorted(
        path
        for root in roots
        for path in (REPOSITORY_ROOT / root).rglob("*.py")
        if "__pycache__" not in path.parts
    )


def _markets_table_uses(tree: ast.AST, tables: set[str]) -> list[str]:
    """Uses of Markets table classes other than naming a foreign-key target.

    ``AssetTable.__table__.fullname`` (the target of a Valmer foreign key) names
    the table without reading it; any other use could build a statement.
    """

    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    uses = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Name) and node.id in tables):
            continue
        table_attribute = parents.get(node)
        name_attribute = parents.get(table_attribute)
        if (
            isinstance(table_attribute, ast.Attribute)
            and table_attribute.attr == "__table__"
            and isinstance(name_attribute, ast.Attribute)
            and name_attribute.attr in {"fullname", "name"}
        ):
            continue
        uses.append(f"{node.id}@{node.lineno}")
    return uses


def test_no_module_builds_a_statement_on_a_markets_table() -> None:
    """Markets registries are row calls; only Valmer's own tables get compiled SQL."""

    offenders: dict[str, list[str]] = {}
    for path in _python_modules():
        relative = str(path.relative_to(REPOSITORY_ROOT))
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported = {
            alias.asname or alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and (node.module or "").split(".")[0] in {"msm", "msm_pricing", "msm_portfolios"}
            for alias in node.names
        }
        tables = imported & MARKETS_TABLE_CLASSES
        if not tables or not imported & STATEMENT_BUILDERS:
            continue
        if relative in REMAINING_DIRECT_ACCESS:
            continue
        uses = _markets_table_uses(tree, tables)
        if uses:
            offenders[relative] = uses
    assert offenders == {}


def test_no_module_borrows_a_markets_row_runtime_context() -> None:
    """``Row._active_context()`` is the runtime backend's context; the API has none."""

    offenders = [
        str(path.relative_to(REPOSITORY_ROOT))
        for path in _python_modules()
        if "._active_context()" in path.read_text(encoding="utf-8")
        and str(path.relative_to(REPOSITORY_ROOT)) not in REMAINING_DIRECT_ACCESS
    ]
    assert offenders == []
