from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse

from valmer_connectors.control_plane.models import (
    BulkActionExecution,
    BulkActionPreflight,
    CurrentUserResponse,
    JobRunParametersResponse,
    LaunchResponse,
    OverviewResponse,
    PipelineResponse,
    ResourceCollection,
)
from valmer_connectors.control_plane.service import (
    ControlPlaneConflict,
    ControlPlaneError,
    ControlPlaneForbidden,
    ControlPlaneService,
    asset_discovery,
    data_product_discovery,
    job_discovery,
    job_run_discovery,
)


def _service(request: Request) -> ControlPlaneService:
    return request.app.state.control_plane_service


def _request_user_uid(request: Request) -> str:
    user_uid = str(getattr(request.state, "user_uid", "") or "").strip()
    if not user_uid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="The platform-authenticated request user is unavailable.",
        )
    return user_uid


Service = Annotated[ControlPlaneService, Depends(_service)]
RequestUserUid = Annotated[str, Depends(_request_user_uid)]


def create_app(
    service: ControlPlaneService | None = None,
    *,
    app: FastAPI | None = None,
) -> FastAPI:
    app = app or FastAPI(
        title="Valmer Control Plane API",
        version="1.0.0",
        description=(
            "Authenticated operational read models and approved Job actions for the "
            "Valmer Command Center static-site application."
        ),
    )
    app.state.control_plane_service = service or ControlPlaneService()

    @app.exception_handler(ControlPlaneForbidden)
    async def forbidden_handler(_request: Request, exc: ControlPlaneForbidden) -> JSONResponse:
        return JSONResponse(status_code=status.HTTP_403_FORBIDDEN, content={"detail": str(exc)})

    @app.exception_handler(ControlPlaneConflict)
    async def conflict_handler(_request: Request, exc: ControlPlaneConflict) -> JSONResponse:
        return JSONResponse(status_code=status.HTTP_409_CONFLICT, content={"detail": str(exc)})

    @app.exception_handler(ControlPlaneError)
    async def control_plane_error_handler(
        _request: Request, exc: ControlPlaneError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": str(exc)},
        )

    @app.get("/health", operation_id="getValmerControlPlaneHealth")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "valmer-control-plane"}

    @app.get(
        "/api/v1/control-plane/me",
        response_model=CurrentUserResponse,
        operation_id="getValmerControlPlaneCurrentUser",
    )
    def current_user(service: Service, user_uid: RequestUserUid) -> CurrentUserResponse:
        return service.current_user(user_uid)

    @app.get(
        "/api/v1/control-plane/overview",
        response_model=OverviewResponse,
        operation_id="getValmerControlPlaneOverview",
    )
    def overview(service: Service, _user_uid: RequestUserUid) -> OverviewResponse:
        return service.overview()

    @app.get(
        "/api/v1/control-plane/pipeline",
        response_model=PipelineResponse,
        operation_id="getValmerControlPlanePipeline",
    )
    def pipeline(service: Service, _user_uid: RequestUserUid) -> PipelineResponse:
        return service.pipeline()

    @app.get(
        "/api/v1/control-plane/data-products/discovery/",
        operation_id="discoverValmerDataProducts",
    )
    def discover_data_products(_user_uid: RequestUserUid) -> dict[str, object]:
        return data_product_discovery()

    @app.get(
        "/api/v1/control-plane/data-products",
        response_model=ResourceCollection,
        operation_id="listValmerDataProducts",
    )
    def list_data_products(
        service: Service,
        _user_uid: RequestUserUid,
        page_index: Annotated[int, Query(alias="pageIndex", ge=0)] = 0,
        page_size: Annotated[int, Query(alias="pageSize", ge=1, le=250)] = 25,
        search: str | None = None,
        status_filter: Annotated[str | None, Query(alias="status")] = None,
        category: str | None = None,
        ordering: str | None = None,
    ) -> ResourceCollection:
        return service.data_products(
            page_index=page_index,
            page_size=page_size,
            search=search,
            status=status_filter,
            category=category,
            ordering=ordering,
        )

    @app.get(
        "/api/v1/control-plane/assets/discovery/",
        operation_id="discoverValmerAssets",
    )
    def discover_assets(_user_uid: RequestUserUid) -> dict[str, object]:
        return asset_discovery()

    @app.get(
        "/api/v1/control-plane/assets",
        response_model=ResourceCollection,
        operation_id="listValmerAssets",
    )
    def list_assets(
        service: Service,
        _user_uid: RequestUserUid,
        page_index: Annotated[int, Query(alias="pageIndex", ge=0)] = 0,
        page_size: Annotated[int, Query(alias="pageSize", ge=1, le=250)] = 25,
        search: str | None = None,
        pricing_target: bool | None = None,
        ordering: str | None = None,
    ) -> ResourceCollection:
        return service.assets(
            page_index=page_index,
            page_size=page_size,
            search=search,
            pricing_target=pricing_target,
            ordering=ordering,
        )

    @app.get(
        "/api/v1/control-plane/jobs/discovery/",
        operation_id="discoverValmerJobs",
    )
    def discover_jobs(service: Service, user_uid: RequestUserUid) -> dict[str, object]:
        return job_discovery(can_operate=service.is_operator(user_uid))

    @app.get(
        "/api/v1/control-plane/jobs",
        response_model=ResourceCollection,
        operation_id="listValmerJobs",
    )
    def list_jobs(
        service: Service,
        _user_uid: RequestUserUid,
        page_index: Annotated[int, Query(alias="pageIndex", ge=0)] = 0,
        page_size: Annotated[int, Query(alias="pageSize", ge=1, le=250)] = 25,
        search: str | None = None,
        status_filter: Annotated[str | None, Query(alias="status")] = None,
        ordering: str | None = None,
    ) -> ResourceCollection:
        return service.jobs(
            page_index=page_index,
            page_size=page_size,
            search=search,
            status=status_filter,
            ordering=ordering,
        )

    @app.get(
        "/api/v1/control-plane/jobs/{job_uid}/run-parameters",
        response_model=JobRunParametersResponse,
        operation_id="getValmerJobRunParameters",
    )
    def get_job_run_parameters(
        job_uid: str,
        service: Service,
        user_uid: RequestUserUid,
    ) -> JobRunParametersResponse:
        return service.job_run_parameters(user_uid, job_uid)

    @app.get(
        "/api/v1/control-plane/job-runs/discovery/",
        operation_id="discoverValmerJobRuns",
    )
    def discover_job_runs(_user_uid: RequestUserUid) -> dict[str, object]:
        return job_run_discovery()

    @app.get(
        "/api/v1/control-plane/job-runs",
        response_model=ResourceCollection,
        operation_id="listValmerJobRuns",
    )
    def list_job_runs(
        service: Service,
        _user_uid: RequestUserUid,
        page_index: Annotated[int, Query(alias="pageIndex", ge=0)] = 0,
        page_size: Annotated[int, Query(alias="pageSize", ge=1, le=250)] = 25,
        search: str | None = None,
        status_filter: Annotated[str | None, Query(alias="status")] = None,
        ordering: str | None = None,
    ) -> ResourceCollection:
        return service.job_runs(
            page_index=page_index,
            page_size=page_size,
            search=search,
            status=status_filter,
            ordering=ordering,
        )

    @app.post(
        "/api/v1/control-plane/jobs/actions/run/preflight",
        response_model=BulkActionPreflight,
        operation_id="preflightValmerJobRun",
    )
    def preflight_job_run(
        payload: BulkActionExecution,
        service: Service,
        user_uid: RequestUserUid,
    ) -> BulkActionPreflight:
        return service.preflight(user_uid, payload)

    @app.post(
        "/api/v1/control-plane/jobs/actions/run",
        response_model=LaunchResponse,
        status_code=status.HTTP_202_ACCEPTED,
        operation_id="runValmerJob",
    )
    def run_job(
        payload: BulkActionExecution,
        service: Service,
        user_uid: RequestUserUid,
    ) -> LaunchResponse:
        return service.launch(user_uid, payload)

    return app


__all__ = ["create_app"]
