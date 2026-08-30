"""Development-only ASGI wrapper for the VS Code control-plane review launch."""

from __future__ import annotations

import os
from types import SimpleNamespace

import msm
from fastapi.responses import JSONResponse
from msm.models import AssetTable
from msm_pricing.bootstrap import attach_pricing_schemas
from msm_pricing.models.pricing_details import AssetCurrentPricingDetailsTable
from starlette.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send

from valmer_connectors.control_plane.api import create_app

LOCAL_REVIEW_ORIGIN = os.getenv(
    "VALMER_LOCAL_REVIEW_ORIGIN", "http://127.0.0.1:5187"
)
LOCAL_FASTAPI_UID = os.getenv(
    "VALMER_LOCAL_REVIEW_FASTAPI_UID", "22222222-2222-4222-8222-222222222222"
)
LOCAL_TOKEN = os.getenv("VALMER_LOCAL_REVIEW_TOKEN", "valmer-local-review")
LOCAL_USER_UID = os.getenv(
    "VALMER_LOCAL_REVIEW_USER_UID", "11111111-1111-4111-8111-111111111111"
)


class LocalReviewIdentityMiddleware:
    """Model the platform's narrow runtime boundary without changing production code."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = str(scope.get("path") or "")
        method = str(scope.get("method") or "").upper()
        if path != "/health" and method != "OPTIONS":
            headers = {
                key.decode("latin-1").lower(): value.decode("latin-1")
                for key, value in scope.get("headers", [])
            }
            if (
                headers.get("authorization") != f"Bearer {LOCAL_TOKEN}"
                or headers.get("x-resource-release-uid") != LOCAL_FASTAPI_UID
            ):
                response = JSONResponse(
                    status_code=401,
                    content={"detail": "The local review capability is invalid."},
                )
                await response(scope, receive, send)
                return

        state = scope.setdefault("state", {})
        state["user_uid"] = LOCAL_USER_UID
        state["user"] = SimpleNamespace(uid=LOCAL_USER_UID, username="local-review")
        await self.app(scope, receive, send)


msm.start_engine(models=[AssetTable])
attach_pricing_schemas(
    models=[AssetTable, AssetCurrentPricingDetailsTable],
    seed_default_market_data_bindings=False,
)
production_app = create_app()
identity_app = LocalReviewIdentityMiddleware(production_app)
app = CORSMiddleware(
    identity_app,
    allow_origins=[LOCAL_REVIEW_ORIGIN],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["Authorization", "Content-Type", "X-Resource-Release-UID"],
)
