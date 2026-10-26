from fastapi import FastAPI

from valmer_connectors.control_plane.api import create_app
from valmer_connectors.runtime import use_markets_api

# The control plane reads Markets registries (current pricing details) through
# the Markets API (ms-markets ADR 0047) and Valmer's own tables through
# MetaTables by identifier, so it attaches no markets runtime and holds no
# grant on mainsequence.markets registries.
use_markets_api()
app = FastAPI(
    title="Valmer Control Plane API",
    version="1.0.0",
    description=(
        "Authenticated operational read models and approved Job actions for the "
        "Valmer Command Center static-site application."
    ),
)
# Mounts the routes and installs the SDK request identity once; the platform
# launcher checks that declaration right after importing this module.
create_app(app=app)

__all__ = ["app"]
