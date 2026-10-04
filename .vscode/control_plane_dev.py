"""Development-only ASGI wrapper for the VS Code control-plane review launch."""

from __future__ import annotations

import os

import msm
from msm.models import AssetTable
from msm_pricing.bootstrap import attach_pricing_schemas
from msm_pricing.models.pricing_details import AssetCurrentPricingDetailsTable
from starlette.middleware.cors import CORSMiddleware

from valmer_connectors.control_plane.api import create_app

LOCAL_REVIEW_ORIGIN = os.getenv(
    "VALMER_LOCAL_REVIEW_ORIGIN", "http://127.0.0.1:5187"
)

msm.start_engine(models=[AssetTable])
attach_pricing_schemas(
    models=[AssetTable, AssetCurrentPricingDetailsTable],
    seed_default_market_data_bindings=False,
)
# create_app() installs the SDK request identity. Without hosted markers it runs in
# local mode: every request needs a Bearer token that MAINSEQUENCE_ENDPOINT's
# /api/v1/users/me/ accepts, and the handlers see that user. Only CORS for the
# local review origin is added here.
app = CORSMiddleware(
    create_app(),
    allow_origins=[LOCAL_REVIEW_ORIGIN],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["Authorization", "Content-Type", "X-Resource-Release-UID"],
)
