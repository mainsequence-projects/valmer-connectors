from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pandas as pd
from fastapi.testclient import TestClient
from msm.models import AssetTable
from msm_pricing.data_nodes.curves.storage import DiscountCurvesStorage
from msm_pricing.data_nodes.index_fixings.storage import IndexFixingsStorage
from msm_pricing.models.pricing_details import AssetCurrentPricingDetailsTable

from mainsequence.client.metatables import MetaTable
from valmer_connectors.control_plane import service as control_plane_service
from valmer_connectors.control_plane.api import _request_user_uid, create_app
from valmer_connectors.control_plane.catalog import (
    CANONICAL_ASSET_TABLE_IDENTIFIER,
    CURRENT_PRICING_DETAILS_TABLE_IDENTIFIER,
    DATA_PRODUCTS,
    DISCOUNT_CURVE_TABLE_IDENTIFIER,
    INDEX_FIXINGS_TABLE_IDENTIFIER,
    INDEX_VALUES_TABLE_IDENTIFIER,
    JOB_ACTIONS,
    VALMER_ASSET_DETAILS_TABLE_IDENTIFIER,
    VECTOR_TABLE_IDENTIFIER,
)
from valmer_connectors.control_plane.service import (
    ControlPlaneError,
    ControlPlaneService,
    Job,
    PlatformControlPlaneGateway,
    _approved_job_definition,
    _qualified_table_name,
    _select_meta_table_rows,
)
from valmer_connectors.data_nodes.canonical_index_values import DailyIndexValuesStorage
from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable


class FakeControlPlaneGateway:
    def __init__(self) -> None:
        self.launched_job_uids: list[str] = []
        self.job_status = "succeeded"

    def data_products(self) -> list[dict[str, object]]:
        base = {
            "table_identifier": "example",
            "cadence": "1d",
            "latest_observation": "2026-08-30T00:00:00+00:00",
            "lag_hours": 1.0,
            "latest_rows": 4,
            "description": "Test product",
            "error": None,
        }
        return [
            {
                **base,
                "uid": "valmer-assets",
                "name": "Registered Valmer assets",
                "category": "registry",
                "status": "healthy",
                "latest_rows": 12,
            },
            {
                **base,
                "uid": "pricing-details",
                "name": "Current pricing details",
                "category": "registry",
                "status": "healthy",
                "latest_rows": 10,
            },
            {
                **base,
                "uid": "valmer-vector",
                "name": "Valmer vector",
                "category": "source",
                "status": "healthy",
            },
            *[
                {
                    **base,
                    "uid": f"curve-{curve}",
                    "name": f"{curve.upper()} curve",
                    "category": "curve",
                    "status": "healthy",
                }
                for curve in ("tiie", "sofr", "xccy", "government")
            ],
        ]

    def assets(self) -> list[dict[str, object]]:
        return [
            {
                "uid": "BI_CETES_260101",
                "name": "CETES 260101",
                "security_type": "BI",
                "issuer": "CETES",
                "series": "260101",
                "currency": "MPS",
                "sector": "Government",
                "dirty_price": 99.2,
                "yield_rate": 0.091,
                "duration": 0.25,
                "latest_observation": "2026-08-30T00:00:00+00:00",
                "pricing_target": True,
                "status": "healthy",
            }
        ]

    def jobs(self) -> list[dict[str, object]]:
        return [
            {
                "uid": f"job-{index}",
                "key": action.key,
                "name": action.job_name,
                "description": action.description,
                "status": self.job_status,
                "last_run_status": "SUCCEEDED",
                "last_run_at": "2026-08-30T00:00:00+00:00",
                "execution_path": action.execution_path,
                "schedule": None,
                "image_status": "ready",
                "automatic_deployment": True,
                "dependencies": list(action.dependencies),
                "approved_action": True,
            }
            for index, action in enumerate(JOB_ACTIONS, start=1)
        ]

    def environment_name(self) -> str:
        return "Test"

    def job_runs(self) -> list[dict[str, object]]:
        return [
            {
                "uid": "run-1",
                "job_uid": "job-1",
                "job_name": "Valmer Vector Refresh",
                "status": "SUCCEEDED",
                "execution_start": "2026-08-30T00:00:00+00:00",
                "execution_end": "2026-08-30T00:02:00+00:00",
                "commit_hash": "a" * 40,
                "triggered_by": "user",
                "logs_url": None,
                "resource_usage_url": None,
            }
        ]

    def run_job(self, job_uid: str) -> tuple[SimpleNamespace, dict[str, str]]:
        self.launched_job_uids.append(job_uid)
        return SimpleNamespace(name="Valmer Vector Refresh"), {
            "uid": "run-2",
            "status": "PENDING",
        }


def _client(user_uid: str, *, gateway: FakeControlPlaneGateway | None = None) -> TestClient:
    gateway = gateway or FakeControlPlaneGateway()
    service = ControlPlaneService(
        gateway=gateway,
        operator_uids={"operator-user"},
        now=lambda: dt.datetime(2026, 8, 30, 12, tzinfo=dt.UTC),
    )
    app = create_app(service)
    app.dependency_overrides[_request_user_uid] = lambda: user_uid
    return TestClient(app)


def test_health_is_available_for_release_checks() -> None:
    response = TestClient(create_app(ControlPlaneService(gateway=FakeControlPlaneGateway()))).get(
        "/health"
    )

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "valmer-control-plane"}


def test_platform_job_projection_accepts_current_script_discriminator() -> None:
    job = Job.model_validate(
        {
            "name": "Valmer Vector Refresh",
            "related_image_uid": "11111111-1111-4111-8111-111111111111",
            "image_status": "ready",
            "type": "script",
        }
    )

    assert job.type == "script"


def test_registry_reads_use_scoped_compiled_metatable_operations(monkeypatch) -> None:
    table = SimpleNamespace(
        uid="table-1",
        identifier="ValmerAssetDetails",
        physical_schema="public",
        physical_table_name="valmer_asset_details",
    )
    captured: list[tuple[dict[str, object], int]] = []

    def execute(operation: dict[str, object], *, timeout: int) -> dict[str, object]:
        captured.append((operation, timeout))
        return {"rows": [{"asset_count": 12}], "truncated": False}

    monkeypatch.setattr(MetaTable, "execute_operation", execute)
    rows = _select_meta_table_rows(
        table,
        f"SELECT COUNT(*) AS asset_count FROM {_qualified_table_name(table)}",
        max_rows=1,
    )

    assert rows == [{"asset_count": 12}]
    assert captured[0][0]["scope"] == {
        "tables": [{"meta_table_uid": "table-1", "access": "read"}]
    }
    assert captured[0][1] == 90


def test_registry_read_rejects_unsafe_physical_identity() -> None:
    table = SimpleNamespace(
        identifier="ValmerAssetDetails",
        physical_schema="public",
        physical_table_name='details"; DROP TABLE assets; --',
    )

    try:
        _qualified_table_name(table)
    except ControlPlaneError as exc:
        assert "unsafe physical table identity" in str(exc)
    else:
        raise AssertionError("Unsafe physical MetaTable names must be rejected.")


def test_control_plane_uses_registered_namespaced_valmer_table_identifiers() -> None:
    assert CANONICAL_ASSET_TABLE_IDENTIFIER == AssetTable.__table__.name
    assert (
        CURRENT_PRICING_DETAILS_TABLE_IDENTIFIER
        == AssetCurrentPricingDetailsTable.__table__.name
    )
    assert (
        VALMER_ASSET_DETAILS_TABLE_IDENTIFIER
        == ValmerAssetDetailsTable.__table__.name
    )
    assert VECTOR_TABLE_IDENTIFIER == ValmerVectorPricesStorage.__table__.name
    assert INDEX_VALUES_TABLE_IDENTIFIER == DailyIndexValuesStorage.__table__.name
    assert INDEX_FIXINGS_TABLE_IDENTIFIER == IndexFixingsStorage.__table__.name
    assert DISCOUNT_CURVE_TABLE_IDENTIFIER == DiscountCurvesStorage.__table__.name
    registered_assets = next(
        product for product in DATA_PRODUCTS if product.key == "valmer-assets"
    )
    assert registered_assets.table_identifier == ValmerAssetDetailsTable.__table__.name


def test_empty_registered_asset_collection_is_not_a_failure() -> None:
    class EmptyRegistryGateway(FakeControlPlaneGateway):
        def data_products(self) -> list[dict[str, object]]:
            products = super().data_products()
            products[0]["latest_rows"] = 0
            return products

    overview = ControlPlaneService(gateway=EmptyRegistryGateway()).overview()
    registered_assets = next(
        metric for metric in overview.metrics if metric.id == "registered-assets"
    )

    assert registered_assets.value == 0
    assert registered_assets.display == "0"
    assert registered_assets.status == "healthy"
    assert registered_assets.detail == "No Valmer assets are registered yet."
    assert overview.failures == []


def test_overview_does_not_expose_upstream_html_error_pages() -> None:
    class FailedJobsGateway(FakeControlPlaneGateway):
        def jobs(self) -> list[dict[str, object]]:
            raise RuntimeError(
                "502 GET https://example.test/jobs: "
                "<!DOCTYPE html><html><body>Bad Gateway</body></html>"
            )

    overview = ControlPlaneService(gateway=FailedJobsGateway()).overview()

    assert overview.failures[-1] == "Jobs: upstream service returned HTTP 502"
    assert "DOCTYPE" not in overview.failures[-1]


def test_approved_job_requires_both_catalog_name_and_execution_path() -> None:
    approved = SimpleNamespace(
        name="Valmer Vector Refresh",
        execution_path="scripts/update_vector_valmer.py",
    )
    replaced = SimpleNamespace(
        name="Valmer Vector Refresh",
        execution_path="scripts/unapproved.py",
    )

    assert _approved_job_definition(approved) is not None
    assert _approved_job_definition(replaced) is None


def test_authenticated_overview_and_resource_collection() -> None:
    with _client("viewer-user") as client:
        overview = client.get("/api/v1/control-plane/overview")
        products = client.get(
            "/api/v1/control-plane/data-products",
            params={"pageIndex": 0, "pageSize": 2, "category": "curve"},
        )

    assert overview.status_code == 200
    assert overview.json()["status"] == "healthy"
    assert overview.json()["metrics"][0]["display"] == "12"
    vector_metric = next(
        metric
        for metric in overview.json()["metrics"]
        if metric["id"] == "latest-vector-observation"
    )
    assert vector_metric == {
        "id": "latest-vector-observation",
        "label": "Latest vector observation",
        "display": "2026-08-30",
        "value": "2026-08-30T00:00:00+00:00",
        "detail": "Most recent persisted Valmer vector time index.",
        "status": "healthy",
    }
    assert products.status_code == 200
    assert products.json()["pageInfo"] == {
        "pageIndex": 0,
        "pageSize": 2,
        "totalItems": 4,
        "hasNextPage": True,
        "hasPreviousPage": False,
    }


def test_overview_reports_missing_approved_jobs_as_a_failure() -> None:
    class MissingJobsGateway(FakeControlPlaneGateway):
        def jobs(self) -> list[dict[str, object]]:
            return []

    overview = ControlPlaneService(gateway=MissingJobsGateway()).overview()
    available_jobs = next(
        metric for metric in overview.metrics if metric.id == "available-jobs"
    )

    assert overview.status == "failed"
    assert available_jobs.display == f"0/{len(JOB_ACTIONS)}"
    assert available_jobs.status == "failed"
    assert overview.failures[-1] == (
        f"Jobs: 0 of {len(JOB_ACTIONS)} approved control-plane Jobs are "
        "available in this branch."
    )


def test_environment_name_comes_from_registered_time_index_tables() -> None:
    gateway = PlatformControlPlaneGateway()
    gateway._time_index_tables = lambda: {
        "vector": SimpleNamespace(organization_environment_name="Production")
    }

    assert gateway.environment_name() == "Production"


def test_platform_gateway_reads_current_pricing_details() -> None:
    class StubbedPlatformGateway(PlatformControlPlaneGateway):
        def _registered_valmer_asset_count(self) -> int:
            return 12

        def _current_pricing_details_count(self) -> int:
            return 10

        def _latest_observation(self, definition):
            return pd.DataFrame(
                {"time_index": [pd.Timestamp("2026-08-30T00:00:00Z")]}
            )

    products = StubbedPlatformGateway().data_products()
    pricing_details = next(
        product
        for product in products
        if product["uid"] == "pricing-details"
    )
    vector = next(product for product in products if product["uid"] == "valmer-vector")

    assert pricing_details["status"] == "healthy"
    assert pricing_details["latest_rows"] == 10
    assert pricing_details["error"] is None
    assert "identity_count" not in pricing_details
    assert "identity_count" not in vector


def test_platform_gateway_bulk_loads_required_jobs_and_runs(monkeypatch) -> None:
    first_action, second_action = JOB_ACTIONS[:2]
    jobs = [
        SimpleNamespace(
            uid="job-1",
            name=first_action.job_name,
            execution_path=first_action.execution_path,
            task_schedule=None,
            image_status="ready",
            automatic_deployment=True,
        ),
        SimpleNamespace(
            uid="job-2",
            name=second_action.job_name,
            execution_path=second_action.execution_path,
            task_schedule=None,
            image_status="ready",
            automatic_deployment=True,
        ),
    ]
    runs = [
        SimpleNamespace(
            uid="run-1",
            job_uid="job-1",
            status="SUCCEEDED",
            execution_start=dt.datetime(2026, 8, 30, tzinfo=dt.UTC),
        )
    ]
    job_queries: list[dict[str, object]] = []
    run_queries: list[dict[str, object]] = []

    def filter_jobs(*, timeout, **kwargs):
        job_queries.append({"timeout": timeout, **kwargs})
        return jobs

    def filter_runs(*, timeout, **kwargs):
        run_queries.append({"timeout": timeout, **kwargs})
        return runs

    monkeypatch.setattr(control_plane_service.Job, "filter", filter_jobs)
    monkeypatch.setattr(control_plane_service.JobRun, "filter", filter_runs)

    result = PlatformControlPlaneGateway().jobs()

    assert [item["uid"] for item in result] == ["job-1", "job-2"]
    assert job_queries == [
        {
            "timeout": 60,
            "name__in": [definition.job_name for definition in JOB_ACTIONS],
        }
    ]
    assert run_queries == [
        {"timeout": 60, "job__uid__in": ["job-1", "job-2"]}
    ]


def test_assets_use_persisted_pricing_details_to_identify_pricing_targets() -> None:
    class StubbedPlatformGateway(PlatformControlPlaneGateway):
        def _vector_snapshot(self) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "unique_identifier": ["target", "non-target"],
                    "time_index": [
                        pd.Timestamp("2026-08-30T00:00:00Z"),
                        pd.Timestamp("2026-08-30T00:00:00Z"),
                    ],
                    "dirty_price": [99.0, 101.0],
                }
            )

        def _asset_details(self, identifiers):
            return {}

        def _pricing_target_identifiers(self) -> set[str]:
            return {"target"}

    assets = StubbedPlatformGateway().assets()

    assert [asset["pricing_target"] for asset in assets] == [True, False]


def test_assets_use_asset_details_for_static_fields_and_vector_time_for_freshness(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        control_plane_service,
        "_utc_now",
        lambda: dt.datetime(2026, 8, 30, 12, tzinfo=dt.UTC),
    )

    class StubbedPlatformGateway(PlatformControlPlaneGateway):
        def _vector_snapshot(self) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "unique_identifier": ["fresh", "stale"],
                    "time_index": [
                        pd.Timestamp("2026-08-30T00:00:00Z"),
                        pd.Timestamp("2026-08-27T00:00:00Z"),
                    ],
                    "issue_currency": ["WRONG", "WRONG"],
                    "sector": ["WRONG", "WRONG"],
                }
            )

        def _asset_details(self, identifiers):
            return {
                identifier: {
                    "valmer_full_name": identifier.title(),
                    "valmer_issue_currency": "MXN",
                    "valmer_sector": "Government",
                }
                for identifier in identifiers
            }

        def _pricing_target_identifiers(self) -> set[str]:
            return set()

    assets = StubbedPlatformGateway().assets()
    assets_by_uid = {asset["uid"]: asset for asset in assets}

    assert [asset["currency"] for asset in assets] == ["MXN", "MXN"]
    assert [asset["sector"] for asset in assets] == ["Government", "Government"]
    assert assets_by_uid["fresh"]["status"] == "healthy"
    assert assets_by_uid["stale"]["status"] == "stale"


def test_registered_valmer_asset_count_uses_valmer_asset_details_relation(
    monkeypatch,
) -> None:
    table = SimpleNamespace(
        uid="details-table",
        identifier=VALMER_ASSET_DETAILS_TABLE_IDENTIFIER,
        physical_schema="public",
        physical_table_name="valmer_connectors__valmerassetdetails",
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        control_plane_service,
        "_optional_registered_meta_table",
        lambda identifier: table if identifier == VALMER_ASSET_DETAILS_TABLE_IDENTIFIER else None,
    )

    def select_rows(selected_table, sql, *, max_rows):
        captured.update(table=selected_table, sql=sql, max_rows=max_rows)
        return [{"asset_count": 6_669}]

    monkeypatch.setattr(control_plane_service, "_select_meta_table_rows", select_rows)

    assert PlatformControlPlaneGateway()._registered_valmer_asset_count() == 6_669
    assert captured["table"] is table
    assert captured["max_rows"] == 1
    assert "valmer_connectors__valmerassetdetails" in str(captured["sql"])


def test_pricing_target_query_requests_the_complete_persisted_relation(
    monkeypatch,
) -> None:
    runtime = SimpleNamespace(
        context=SimpleNamespace(
            data_source_uid=None,
            namespace=None,
            reserved_policy=None,
        )
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        control_plane_service,
        "resolve_pricing_runtime",
        lambda **kwargs: runtime,
    )

    def compile_statement(statement, *, context, **kwargs):
        captured["limits"] = context.limits
        return {"compiled": True}

    monkeypatch.setattr(
        control_plane_service,
        "compile_markets_statement",
        compile_statement,
    )
    monkeypatch.setattr(
        control_plane_service,
        "execute_markets_operation",
        lambda operation, *, context: {
            "rows": [{"unique_identifier": "persisted-target"}],
            "truncated": False,
        },
    )

    identifiers = PlatformControlPlaneGateway()._pricing_target_identifiers()

    assert identifiers == {"persisted-target"}
    assert captured["limits"] == {
        "max_rows": 100_000,
        "statement_timeout_ms": 60_000,
    }


def test_job_discovery_is_authorized_per_human() -> None:
    with _client("viewer-user") as viewer, _client("operator-user") as operator:
        viewer_payload = viewer.get("/api/v1/control-plane/jobs/discovery/").json()
        operator_payload = operator.get("/api/v1/control-plane/jobs/discovery/").json()

    assert viewer_payload["bulk_actions"] == []
    assert operator_payload["bulk_actions"][0]["id"] == "run"
    assert operator_payload["bulk_actions"][0]["selection_modes"] == ["explicit"]


def test_viewer_cannot_execute_job_action() -> None:
    payload = {"selection": {"mode": "explicit", "uids": ["job-1"]}, "options": {}}

    with _client("viewer-user") as client:
        preflight = client.post(
            "/api/v1/control-plane/jobs/actions/run/preflight", json=payload
        )
        execution = client.post("/api/v1/control-plane/jobs/actions/run", json=payload)

    assert preflight.status_code == 200
    assert preflight.json()["allowed"] is False
    assert execution.status_code == 403


def test_operator_preflights_and_launches_one_approved_job() -> None:
    gateway = FakeControlPlaneGateway()
    payload = {"selection": {"mode": "explicit", "uids": ["job-1"]}, "options": {}}

    with _client("operator-user", gateway=gateway) as client:
        preflight = client.post(
            "/api/v1/control-plane/jobs/actions/run/preflight", json=payload
        )
        execution = client.post("/api/v1/control-plane/jobs/actions/run", json=payload)

    assert preflight.status_code == 200
    assert preflight.json()["allowed"] is True
    assert execution.status_code == 202
    assert execution.json()["job_run_uid"] == "run-2"
    assert execution.json()["requested_by_user_uid"] == "operator-user"
    assert gateway.launched_job_uids == ["job-1"]


def test_pipeline_reconciles_declared_actions_with_registered_jobs() -> None:
    class PartialJobsGateway(FakeControlPlaneGateway):
        def jobs(self) -> list[dict[str, object]]:
            return super().jobs()[:1]

    pipeline = ControlPlaneService(gateway=PartialJobsGateway()).pipeline()
    actions = [action for stage in pipeline.stages for action in stage.actions]
    vector = next(action for action in actions if action.key == "vector-refresh")
    missing = next(
        action for action in actions if action.key == "irs-mxn-quotes-refresh"
    )

    assert vector.available is True
    assert vector.job_uid == "job-1"
    assert vector.status == "succeeded"
    assert missing.available is False
    assert missing.job_uid is None
    assert missing.status == "missing"
    assert {action.key for action in actions} == {action.key for action in JOB_ACTIONS}


def test_launch_rejects_platform_response_without_run_status() -> None:
    class IncompleteLaunchGateway(FakeControlPlaneGateway):
        def run_job(self, job_uid: str):
            return SimpleNamespace(name="Valmer Vector Refresh"), {"uid": "run-2"}

    service = ControlPlaneService(
        gateway=IncompleteLaunchGateway(),
        operator_uids={"operator-user"},
    )
    request = control_plane_service.BulkActionExecution.model_validate(
        {"selection": {"mode": "explicit", "uids": ["job-1"]}, "options": {}}
    )

    try:
        service.launch("operator-user", request)
    except ControlPlaneError as exc:
        assert "without returning its run status" in str(exc)
    else:
        raise AssertionError("A Job launch without platform status must be rejected.")


def test_operator_cannot_launch_an_already_running_job() -> None:
    gateway = FakeControlPlaneGateway()
    gateway.job_status = "running"
    payload = {"selection": {"mode": "explicit", "uids": ["job-1"]}, "options": {}}

    with _client("operator-user", gateway=gateway) as client:
        response = client.post("/api/v1/control-plane/jobs/actions/run", json=payload)

    assert response.status_code == 409
    assert gateway.launched_job_uids == []
