from __future__ import annotations

import datetime as dt
import logging
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from msm.api.http import TabularFrameResponse

from mainsequence.client import RequestIdentityError, User
from mainsequence.server.fastapi import install_request_identity
from valmer_connectors.data_api.models import (
    MAX_VALUATION_DATES,
    AssetDetailsRequest,
    AssetMaturityRequest,
    BondComparisonFiltersResponse,
    BondComparisonUniverseRequest,
    BondFiltersResponse,
    BondHistoryRequest,
    BondInstrumentResponse,
    BondSearchRequest,
    BondSnapshotRequest,
    DatasetsResponse,
    ValuationDatesResponse,
    VectorHistoryRequest,
    VectorLatestRequest,
    VectorSnapshotRequest,
)
from valmer_connectors.data_api.service import DEFAULT_VALUATION_DATES, DataApiService
from valmer_connectors.data_api.tables import DataApiBadRequest, DataApiError, DataApiNotFound

LOGGER = logging.getLogger(__name__)
API_PREFIX = "/api/v1"


def _service(request: Request) -> DataApiService:
    return request.app.state.data_api_service


def _request_user_uid(request: Request) -> str:
    """Return the caller the SDK request identity authenticated, and log the read.

    The middleware that `create_data_api` installs rejects an unauthenticated
    request before any handler runs. This check still refuses a request that
    reaches a route without a bound caller.
    """
    try:
        user_uid = User.get_logged_user().uid
    except RequestIdentityError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="The platform-authenticated request user is unavailable.",
        ) from exc
    LOGGER.info("Valmer data read %s %s by %s", request.method, request.url.path, user_uid)
    return user_uid


Service = Annotated[DataApiService, Depends(_service)]
RequestUserUid = Annotated[str, Depends(_request_user_uid)]


def create_data_api(
    service: DataApiService | None = None,
    *,
    app: FastAPI | None = None,
) -> FastAPI:
    """Mount the Valmer data API on `app`, or on a new application, and return it.

    Every call installs the SDK request identity on the application it builds, so
    pass an `app` that has none yet; installing it twice raises.
    """
    app = app or FastAPI(
        title="Valmer Data API",
        version="1.0.0",
        description="Read models over Valmer's published tables for other applications.",
    )
    app.state.data_api_service = service or DataApiService()

    @app.exception_handler(DataApiNotFound)
    async def not_found_handler(_request: Request, exc: DataApiNotFound) -> JSONResponse:
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": str(exc)})

    @app.exception_handler(DataApiBadRequest)
    async def bad_request_handler(_request: Request, exc: DataApiBadRequest) -> JSONResponse:
        return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"detail": str(exc)})

    @app.exception_handler(DataApiError)
    async def unavailable_handler(_request: Request, exc: DataApiError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": str(exc)},
        )

    @app.get("/health", operation_id="getValmerDataApiHealth")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "valmer-data-api"}

    @app.get(
        f"{API_PREFIX}/datasets/",
        response_model=DatasetsResponse,
        operation_id="listValmerDatasets",
    )
    def datasets(service: Service, _user_uid: RequestUserUid) -> DatasetsResponse:
        return service.datasets()

    @app.get(
        f"{API_PREFIX}/vector/valuation-dates/",
        response_model=ValuationDatesResponse,
        operation_id="listValmerValuationDates",
    )
    def valuation_dates(
        service: Service,
        _user_uid: RequestUserUid,
        limit: Annotated[int, Query(ge=1, le=MAX_VALUATION_DATES)] = DEFAULT_VALUATION_DATES,
    ) -> ValuationDatesResponse:
        return service.valuation_dates(limit)

    @app.post(
        f"{API_PREFIX}/vector/snapshot/",
        response_model=TabularFrameResponse,
        operation_id="readValmerVectorSnapshot",
    )
    def vector_snapshot(
        payload: VectorSnapshotRequest,
        service: Service,
        _user_uid: RequestUserUid,
    ) -> TabularFrameResponse:
        return service.vector_snapshot(payload)

    @app.post(
        f"{API_PREFIX}/vector/history/",
        response_model=TabularFrameResponse,
        operation_id="readValmerVectorHistory",
    )
    def vector_history(
        payload: VectorHistoryRequest,
        service: Service,
        _user_uid: RequestUserUid,
    ) -> TabularFrameResponse:
        return service.vector_history(payload)

    @app.post(
        f"{API_PREFIX}/vector/latest/",
        response_model=TabularFrameResponse,
        operation_id="readValmerVectorLatest",
    )
    def vector_latest(
        payload: VectorLatestRequest,
        service: Service,
        _user_uid: RequestUserUid,
    ) -> TabularFrameResponse:
        return service.vector_latest(payload)

    @app.post(
        f"{API_PREFIX}/assets/details/",
        response_model=TabularFrameResponse,
        operation_id="readValmerAssetDetails",
    )
    def asset_details(
        payload: AssetDetailsRequest,
        service: Service,
        _user_uid: RequestUserUid,
    ) -> TabularFrameResponse:
        return service.asset_details(payload)

    @app.post(
        f"{API_PREFIX}/assets/maturity/",
        response_model=TabularFrameResponse,
        operation_id="readValmerAssetMaturity",
    )
    def asset_maturity(
        payload: AssetMaturityRequest,
        service: Service,
        _user_uid: RequestUserUid,
    ) -> TabularFrameResponse:
        return service.asset_maturity(payload)

    @app.post(
        f"{API_PREFIX}/bonds/snapshot/",
        response_model=TabularFrameResponse,
        operation_id="readValmerBondSnapshot",
    )
    def bond_snapshot(
        payload: BondSnapshotRequest,
        service: Service,
        _user_uid: RequestUserUid,
    ) -> TabularFrameResponse:
        return service.bond_snapshot(payload)

    @app.post(
        f"{API_PREFIX}/bonds/history/",
        response_model=TabularFrameResponse,
        operation_id="readValmerBondHistory",
    )
    def bond_history(
        payload: BondHistoryRequest,
        service: Service,
        _user_uid: RequestUserUid,
    ) -> TabularFrameResponse:
        return service.bond_history(payload)

    @app.get(
        f"{API_PREFIX}/bonds/filters/",
        response_model=BondFiltersResponse,
        operation_id="readValmerBondFilters",
    )
    def bond_filters(
        service: Service,
        _user_uid: RequestUserUid,
        valuation_date: dt.date | None = None,
        security_type: Annotated[str | None, Query(max_length=64)] = None,
        issuer: Annotated[str | None, Query(max_length=128)] = None,
    ) -> BondFiltersResponse:
        return service.bond_filters(
            valuation_date=valuation_date,
            security_type=security_type,
            issuer=issuer,
        )

    @app.post(
        f"{API_PREFIX}/bonds/search/",
        response_model=TabularFrameResponse,
        operation_id="searchValmerBonds",
    )
    def bond_search(
        payload: BondSearchRequest,
        service: Service,
        _user_uid: RequestUserUid,
    ) -> TabularFrameResponse:
        return service.bond_search(payload)

    @app.get(
        f"{API_PREFIX}/bonds/instrument/",
        response_model=BondInstrumentResponse,
        operation_id="readValmerBondInstrument",
    )
    def bond_instrument(
        service: Service,
        _user_uid: RequestUserUid,
        asset_identifier: Annotated[str, Query(min_length=1, max_length=255)],
    ) -> BondInstrumentResponse:
        return service.bond_instrument(asset_identifier)

    @app.get(
        f"{API_PREFIX}/bond-comparison/filters/",
        response_model=BondComparisonFiltersResponse,
        operation_id="readValmerBondComparisonFilters",
    )
    def bond_comparison_filters(
        service: Service,
        _user_uid: RequestUserUid,
        valuation_date: dt.date | None = None,
        security_type: Annotated[str | None, Query(max_length=64)] = None,
    ) -> BondComparisonFiltersResponse:
        return service.bond_comparison_filters(
            valuation_date=valuation_date,
            security_type=security_type,
        )

    @app.post(
        f"{API_PREFIX}/bond-comparison/universe/",
        response_model=TabularFrameResponse,
        operation_id="readValmerBondComparisonUniverse",
    )
    def bond_comparison_universe(
        payload: BondComparisonUniverseRequest,
        service: Service,
        _user_uid: RequestUserUid,
    ) -> TabularFrameResponse:
        return service.bond_comparison_universe(payload)

    # Once, after the routes are mounted. The SDK verifies each request's caller
    # (the platform's signed caller assertion when hosted, a direct Bearer token
    # against MAINSEQUENCE_ENDPOINT locally), binds it for User.get_logged_user()
    # and records the declaration the platform launcher checks before serving.
    install_request_identity(app)
    return app


__all__ = ["API_PREFIX", "create_data_api"]
