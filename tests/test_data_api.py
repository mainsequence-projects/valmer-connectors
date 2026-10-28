from __future__ import annotations

import datetime as dt
import importlib
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from valmer_connectors.data_api.api import create_data_api
from valmer_connectors.data_api.models import (
    AssetMaturityRequest,
    BondComparisonUniverseRequest,
    BondSearchRequest,
    BondSnapshotRequest,
    VectorLatestRequest,
    VectorSnapshotRequest,
)
from valmer_connectors.data_api.service import DataApiService
from valmer_connectors.data_api.tables import (
    ASSET_DETAILS_TABLE_IDENTIFIER,
    VECTOR_TABLE_IDENTIFIER,
    DataApiBadRequest,
    DataApiError,
    DataApiNotFound,
    ValmerTables,
)

LATEST = "2026-10-27T00:00:00+00:00"
VIEWER_UID = "9f68191a-54c2-4e3f-bd07-0f1c6dbe60e9"
ENTRY_MODULE = "api.valmer_data_api.main"
HOSTED_RUNTIME_VARIABLES = (
    "APP_NAME",
    "FASTAPI_PUBLIC_BASE_URL",
    "MAINSEQUENCE_CALLER_AUTH_MODE",
    "MAINSEQUENCE_CALLER_ASSERTION_ISSUER",
    "MAINSEQUENCE_CALLER_ASSERTION_JWKS_URL",
    "MAINSEQUENCE_ORGANIZATION_ENVIRONMENT_UID",
    "FASTAPI_PUBLIC_INGRESS",
)


def _meta_table(identifier: str, physical_name: str) -> SimpleNamespace:
    return SimpleNamespace(
        identifier=identifier,
        uid=f"{physical_name}-uid",
        physical_schema="public",
        physical_table_name=physical_name,
        data_source_uid="data-source-1",
    )


class RecordingExecutor:
    """Answers compiled operations from a list of (sql fragment, rows) rules."""

    def __init__(self, rules: list[tuple[str, list[dict[str, Any]]]] | None = None) -> None:
        self.rules = list(rules or [])
        self.operations: list[Any] = []

    def __call__(self, operation: Any) -> dict[str, Any]:
        self.operations.append(operation)
        sql = operation.statement.sql
        for fragment, rows in self.rules:
            if fragment in sql:
                return {"rows": [dict(row) for row in rows]}
        return {"rows": []}

    @property
    def sql(self) -> list[str]:
        return [operation.statement.sql for operation in self.operations]


def _service(
    executor: RecordingExecutor,
    *,
    assets: list[tuple[str, str]] | None = None,
    vector_ref: Any = None,
) -> tuple[DataApiService, list[dict[str, Any]]]:
    lookups: list[dict[str, Any]] = []

    def asset_lookup(**kwargs: Any) -> list[tuple[str, str]]:
        lookups.append(kwargs)
        wanted = set(kwargs.get("unique_identifiers") or ()) | set(kwargs.get("uids") or ())
        return [pair for pair in assets or [] if pair[0] in wanted or pair[1] in wanted]

    tables = ValmerTables(
        finder=lambda: (
            _meta_table(VECTOR_TABLE_IDENTIFIER, "vector_physical"),
            _meta_table(ASSET_DETAILS_TABLE_IDENTIFIER, "details_physical"),
        ),
        executor=executor,
    )
    if vector_ref is not None:
        tables.vector_ref = lambda: vector_ref  # type: ignore[method-assign]
    return DataApiService(tables, asset_lookup=asset_lookup), lookups


def _latest_rules() -> list[tuple[str, list[dict[str, Any]]]]:
    return [
        ("max(", [{"time_index": LATEST}]),
        ("SELECT DISTINCT", [{"time_index": LATEST}, {"time_index": "2026-10-24T00:00:00+00:00"}]),
    ]


def test_statements_bind_the_registered_physical_tables() -> None:
    executor = RecordingExecutor(_latest_rules())
    service, _lookups = _service(executor)

    service.vector_snapshot(VectorSnapshotRequest(columns=["yield_rate"]))

    assert all("public.vector_physical" in sql for sql in executor.sql)
    assert {operation.data_source_uid for operation in executor.operations} == {"data-source-1"}
    assert {operation.dialect for operation in executor.operations} == {"postgresql"}


def test_unregistered_tables_answer_unavailable() -> None:
    def missing() -> tuple[Any, Any]:
        raise DataApiError("The Valmer table 'x' is not registered.")

    service = DataApiService(ValmerTables(finder=missing, executor=RecordingExecutor()))

    with pytest.raises(DataApiError, match="not registered"):
        service.valuation_times()
    assert service.datasets().datasets[0].uid is None


def test_a_valuation_date_without_a_snapshot_is_not_found() -> None:
    service, _lookups = _service(RecordingExecutor([("max(", [{"time_index": None}])]))

    with pytest.raises(DataApiNotFound, match="2026-10-25"):
        service.resolve_valuation_time(dt.date(2026, 10, 25))


def test_valuation_dates_are_newest_first_and_cached() -> None:
    executor = RecordingExecutor(_latest_rules())
    service, _lookups = _service(executor)

    first = service.valuation_dates(2).valuation_dates
    second = service.valuation_dates(2).valuation_dates

    assert first == second == [
        dt.datetime(2026, 10, 27, tzinfo=dt.UTC),
        dt.datetime(2026, 10, 24, tzinfo=dt.UTC),
    ]
    assert len(executor.operations) == 1


def test_vector_snapshot_reads_one_valuation_time_and_named_columns() -> None:
    executor = RecordingExecutor(
        [
            *_latest_rules()[:1],
            (
                "vector_physical.yield_rate",
                [{"time_index": LATEST, "asset_identifier": "M_BONOS_301128", "yield_rate": 9.1}],
            ),
        ]
    )
    service, _lookups = _service(executor)

    frame = service.vector_snapshot(
        VectorSnapshotRequest(
            valuation_date=dt.date(2026, 10, 27),
            asset_identifiers=["M_BONOS_301128"],
            columns=["yield_rate", "asset_identifier"],
            limit=10,
            offset=20,
        )
    )

    assert frame.columns == ["time_index", "asset_identifier", "yield_rate"]
    assert frame.rows == [
        {"time_index": LATEST, "asset_identifier": "M_BONOS_301128", "yield_rate": 9.1}
    ]
    assert frame.model_dump(mode="json")["meta"]["valuation_time"] == "2026-10-27T00:00:00+00:00"
    snapshot = executor.operations[-1]
    assert "LIMIT" in snapshot.statement.sql and "OFFSET" in snapshot.statement.sql
    assert ["M_BONOS_301128"] in [
        value for value in snapshot.statement.parameters.values() if isinstance(value, list)
    ] or "M_BONOS_301128" in snapshot.statement.parameters.values()


def test_unknown_columns_are_a_bad_request() -> None:
    service, _lookups = _service(RecordingExecutor(_latest_rules()))

    with pytest.raises(DataApiBadRequest, match="Unknown vector columns: price"):
        service.vector_snapshot(VectorSnapshotRequest(columns=["price"]))
    with pytest.raises(DataApiBadRequest, match="Unknown asset-detail columns: yield_rate"):
        service.bond_snapshot(BondSnapshotRequest(detail_columns=["yield_rate"]))


def test_an_empty_identifier_scope_reads_nothing() -> None:
    executor = RecordingExecutor(_latest_rules())
    service, _lookups = _service(executor)

    frame = service.bond_snapshot(BondSnapshotRequest(asset_identifiers=[]))

    assert frame.rows == []
    assert not any("details_physical" in sql for sql in executor.sql)


def test_bond_snapshot_applies_the_screen_filters() -> None:
    executor = RecordingExecutor(_latest_rules())
    service, _lookups = _service(executor)

    service.bond_snapshot(
        BondSnapshotRequest(
            security_types=["CETE"],
            require_yield=True,
            require_future_maturity=True,
            order_by="maturity",
            detail_columns=["series", "full_name", "maturity_date"],
            vector_columns=["yield_rate", "liquidity"],
            limit=500,
        )
    )

    sql = executor.sql[-1]
    assert "JOIN public.vector_physical ON" in sql
    assert "details_physical.security_type IN" in sql
    assert "vector_physical.yield_rate IS NOT NULL" in sql
    assert "details_physical.maturity_date >" in sql
    assert "ORDER BY public.details_physical.maturity_date ASC" in sql


def test_bond_search_escapes_like_and_reports_total_rows() -> None:
    executor = RecordingExecutor(
        [
            *_latest_rules(),
            ("count(*) OVER ()", [{"asset_identifier": "M_BONOS_301128", "total_rows": 41}]),
        ]
    )
    service, _lookups = _service(executor)

    frame = service.bond_search(BondSearchRequest(query="50%_x", sort_by="rating", page_size=25))

    dumped = frame.model_dump(mode="json")
    assert dumped["meta"]["total_rows"] == 41
    assert "total_rows" not in frame.rows[0]
    search = executor.operations[-1]
    assert "%50/%/_x%" in search.statement.parameters.values()
    assert "coalesce(nullif(" in search.statement.sql
    assert "LEFT OUTER JOIN public.vector_physical" in search.statement.sql


def test_an_empty_later_search_page_counts_the_matches() -> None:
    executor = RecordingExecutor([*_latest_rules(), ("count(*) AS total_rows", [{"total_rows": 7}])])
    service, _lookups = _service(executor)

    frame = service.bond_search(BondSearchRequest(page=3, page_size=25))

    assert frame.rows == []
    assert frame.model_dump(mode="json")["meta"]["total_rows"] == 7


def test_bond_comparison_universe_keeps_the_reference_laterals_and_scope() -> None:
    executor = RecordingExecutor(
        [
            *_latest_rules(),
            ("reference_m_bond", [{"asset_identifier": "M_BONOS_301128", "total_rows": 2_500}]),
        ]
    )
    service, _lookups = _service(executor)

    frame = service.bond_comparison_universe(
        BondComparisonUniverseRequest(asset_identifiers=["M_BONOS_301128"], rate_types=["Fixed"])
    )

    sql = executor.sql[-1]
    for fragment in ("LATERAL", "reference_m_bond", "previous_quote", "spread_history", "stddev_pop"):
        assert fragment in sql
    assert frame.model_dump(mode="json")["meta"]["total_rows"] == 2_500
    assert "valuation_date" in frame.columns and "uh_date" in frame.columns

    service.bond_comparison_universe(BondComparisonUniverseRequest(asset_identifiers=[]))
    assert "false" in executor.sql[-1].lower()


def test_asset_maturity_resolves_markets_identifiers_and_expands_aliases() -> None:
    detail_uid = "11111111-1111-1111-1111-111111111111"
    executor = RecordingExecutor(
        [
            (
                "detail_asset_uid",
                [
                    {
                        "detail_asset_uid": detail_uid,
                        "asset_identifier": "M_BONOS_301128",
                        "valmer_unique_identifier": "M_BONOS_301128",
                        "maturity_date": "2030-11-28",
                    }
                ],
            )
        ]
    )
    service, lookups = _service(executor, assets=[(detail_uid, "MX_M_301128")])

    frame = service.asset_maturity(AssetMaturityRequest(asset_identifiers=["MX_M_301128"]))

    assert lookups == [{"unique_identifiers": ["MX_M_301128"]}]
    assert [row["asset_identifier"] for row in frame.rows] == ["M_BONOS_301128", "MX_M_301128"]
    assert {row["asset_uid"] for row in frame.rows} == {detail_uid}
    assert {row["asset_table_identifier"] for row in frame.rows} == {"MX_M_301128"}


class FakeVectorRef:
    def __init__(self, frame: pd.DataFrame | Exception) -> None:
        self.frame = frame
        self.calls: list[Any] = []

    def get_last_observation(self, *, dimension_range_map: list[dict[str, Any]]) -> pd.DataFrame:
        self.calls.append(dimension_range_map)
        if isinstance(self.frame, Exception):
            raise self.frame
        return self.frame


def test_vector_latest_reads_the_last_observation_per_identifier() -> None:
    ref = FakeVectorRef(
        pd.DataFrame(
            {"dirty_price": [101.5, float("nan")]},
            index=pd.MultiIndex.from_tuples(
                [
                    (pd.Timestamp(LATEST), "M_BONOS_301128"),
                    (pd.Timestamp(LATEST), "CETES_BI_261105"),
                ],
                names=["time_index", "asset_identifier"],
            ),
        )
    )
    service, _lookups = _service(RecordingExecutor(), vector_ref=ref)
    as_of = dt.datetime(2026, 10, 28, tzinfo=dt.UTC)

    frame = service.vector_latest(
        VectorLatestRequest(
            asset_identifiers=["M_BONOS_301128", "CETES_BI_261105"],
            as_of=as_of,
            columns=["dirty_price"],
        )
    )

    assert ref.calls[0][0] == {"coordinate": {"asset_identifier": "M_BONOS_301128"}, "end_date": as_of}
    assert frame.columns == ["time_index", "asset_identifier", "dirty_price"]
    assert [row["dirty_price"] for row in frame.rows] == [101.5, None]
    assert frame.model_dump(mode="json")["meta"]["source"] == "latest"


def test_vector_latest_falls_back_to_bounded_history_in_request_order() -> None:
    executor = RecordingExecutor(
        [
            (
                "DISTINCT ON",
                [
                    {"time_index": LATEST, "asset_identifier": "A", "dirty_price": 99.0},
                    {"time_index": LATEST, "asset_identifier": "B", "dirty_price": 98.0},
                ],
            )
        ]
    )
    service, _lookups = _service(executor, vector_ref=FakeVectorRef(RuntimeError("down")))
    as_of = dt.datetime(2026, 10, 28, tzinfo=dt.UTC)

    frame = service.vector_latest(
        VectorLatestRequest(
            asset_identifiers=["B", "A"],
            as_of=as_of,
            columns=["dirty_price"],
            fallback_lookback_days=14,
        )
    )

    assert [row["asset_identifier"] for row in frame.rows] == ["B", "A"]
    assert frame.model_dump(mode="json")["meta"]["source"] == "history_fallback"
    statement = executor.operations[-1].statement
    window = [
        pd.Timestamp(statement.parameters[name])
        for name, sql_type in (statement.parameter_types or {}).items()
        if sql_type == "timestamp with time zone"
    ]
    assert sorted(window) == [pd.Timestamp(as_of - dt.timedelta(days=14)), pd.Timestamp(as_of)]


def test_vector_latest_reports_both_errors_when_the_fallback_fails() -> None:
    def failing(_operation: Any) -> dict[str, Any]:
        raise RuntimeError("history down")

    service, _lookups = _service(RecordingExecutor(), vector_ref=FakeVectorRef(RuntimeError("latest down")))
    service.tables._executor = failing

    with pytest.raises(DataApiError, match="Primary error: latest down") as raised:
        service.vector_latest(VectorLatestRequest(asset_identifiers=["A"]))
    assert "history down" in str(raised.value)


def test_bond_instrument_falls_back_to_the_markets_identifier() -> None:
    detail_uid = "11111111-1111-1111-1111-111111111111"
    executor = RecordingExecutor()
    service, lookups = _service(executor, assets=[(detail_uid, "MX_M_301128")])

    executor.rules = [
        ("details_physical.asset_uid IN", [{"asset_identifier": "M_BONOS_301128", "name": "Bono"}])
    ]
    instrument = service.bond_instrument("MX_M_301128")

    assert instrument.found is True
    assert instrument.asset_identifier == "M_BONOS_301128"
    assert lookups == [{"unique_identifiers": ["MX_M_301128"]}]

    executor.rules = []
    assert service.bond_instrument("UNKNOWN").found is False


# HTTP: the routes, the request identity and the error mapping.


@pytest.fixture(scope="module")
def users_me_endpoint():
    """Serve a loopback MAINSEQUENCE_ENDPOINT whose /api/v1/users/me/ knows one token."""

    class UsersMeHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - http.server handler name
            scheme, _, token = self.headers.get("Authorization", "").partition(" ")
            if self.path == "/api/v1/users/me/" and scheme == "Bearer" and token == "viewer-token":
                status_code, body = 200, {"uid": VIEWER_UID, "username": "viewer"}
            else:
                status_code, body = 401, {"detail": "Invalid token."}
            payload = json.dumps(body).encode()
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args: object) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), UsersMeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def local_runtime(monkeypatch, users_me_endpoint) -> None:
    for name in HOSTED_RUNTIME_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", users_me_endpoint)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")


def _app(executor: RecordingExecutor | None = None) -> Any:
    service, _lookups = _service(executor or RecordingExecutor(_latest_rules()))
    return create_data_api(service)


def _route_requests(app: Any) -> list[tuple[str, str]]:
    requests = []
    for route in app.routes:
        methods = sorted(set(getattr(route, "methods", None) or ()) - {"HEAD"})
        if methods and route.path.startswith("/api/v1/"):
            requests.append((methods[0], route.path))
    return requests


def test_every_route_refuses_a_request_without_a_platform_credential(local_runtime) -> None:
    app = _app()
    requests = _route_requests(app)

    assert len(requests) == 14
    with TestClient(app) as client:
        for method, path in requests:
            response = client.request(method, path, json={})
            assert response.status_code == 401, (method, path, response.text)


def test_routes_answer_an_authenticated_caller(local_runtime) -> None:
    executor = RecordingExecutor(
        [
            *_latest_rules(),
            ("vector_physical.spread", [{"time_index": LATEST, "asset_identifier": "A", "spread": 0.4}]),
        ]
    )
    headers = {"Authorization": "Bearer viewer-token"}
    with TestClient(_app(executor), headers=headers) as client:
        dates = client.get("/api/v1/vector/valuation-dates/", params={"limit": 2})
        snapshot = client.post(
            "/api/v1/vector/snapshot/",
            json={"valuation_date": "2026-10-27", "columns": ["spread"]},
        )
        datasets = client.get("/api/v1/datasets/")

    assert dates.status_code == 200
    assert dates.json()["valuation_dates"][0].startswith("2026-10-27")
    assert snapshot.status_code == 200
    assert snapshot.json()["rows"] == [{"time_index": LATEST, "asset_identifier": "A", "spread": 0.4}]
    assert snapshot.json()["meta"]["valuation_time"].startswith("2026-10-27")
    assert datasets.json()["datasets"][0]["identifier"] == VECTOR_TABLE_IDENTIFIER


def test_errors_map_to_http_statuses(local_runtime) -> None:
    headers = {"Authorization": "Bearer viewer-token"}
    missing_date = RecordingExecutor([("max(", [{"time_index": None}])])
    with TestClient(_app(missing_date), headers=headers) as client:
        not_found = client.post("/api/v1/vector/snapshot/", json={"valuation_date": "2026-10-25"})
        bad_column = client.post(
            "/api/v1/bonds/snapshot/",
            json={"valuation_date": "2026-10-25", "vector_columns": ["price"]},
        )
        invalid = client.post("/api/v1/vector/history/", json={"asset_identifiers": []})

    def unavailable() -> tuple[Any, Any]:
        raise DataApiError("The Valmer table 'x' is not registered.")

    app = create_data_api(DataApiService(ValmerTables(finder=unavailable, executor=RecordingExecutor())))
    with TestClient(app, headers=headers) as client:
        down = client.get("/api/v1/vector/valuation-dates/")

    assert not_found.status_code == 404
    assert "2026-10-25" in not_found.json()["detail"]
    assert bad_column.status_code in {400, 404}
    assert invalid.status_code == 422
    assert down.status_code == 503


def test_entry_module_selects_the_markets_api_and_installs_the_request_identity(
    local_runtime,
    monkeypatch,
) -> None:
    from msm.api.remote import active_remote_backend

    monkeypatch.delitem(sys.modules, ENTRY_MODULE, raising=False)
    try:
        app = importlib.import_module(ENTRY_MODULE).app
    finally:
        sys.modules.pop(ENTRY_MODULE, None)

    assert app.state.mainsequence_request_identity["installed"] is True
    assert active_remote_backend() is not None
    assert ("POST", "/api/v1/bonds/search/") in _route_requests(app)
