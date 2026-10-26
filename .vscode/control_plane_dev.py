"""Development-only ASGI wrapper for the VS Code control-plane review launch."""

from __future__ import annotations

import os

from starlette.middleware.cors import CORSMiddleware

from valmer_connectors.control_plane.api import create_app
from valmer_connectors.runtime import use_markets_api

LOCAL_REVIEW_ORIGIN = os.getenv(
    "VALMER_LOCAL_REVIEW_ORIGIN", "http://127.0.0.1:5187"
)

# Same backend as the hosted entry point (api/valmer_control_plane/main.py):
# Markets registries through the Markets API (MARKETS_API_URL selects a local one).
use_markets_api()
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
