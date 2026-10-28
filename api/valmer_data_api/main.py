from fastapi import FastAPI

from valmer_connectors.data_api.api import create_data_api
from valmer_connectors.runtime import use_markets_api

# The data API reads Valmer's own tables through MetaTables by identifier and
# Markets registries (Asset rows, for identifier aliases) through the Markets
# API (ms-markets ADR 0047), so it attaches no markets runtime. Other
# applications read Valmer data only through this API (ADR 0010).
use_markets_api()
app = FastAPI(
    title="Valmer Data API",
    version="1.0.0",
    description="Read models over Valmer's published tables for other applications.",
)
# Mounts the routes and installs the SDK request identity once; the platform
# launcher checks that declaration right after importing this module.
create_data_api(app=app)

__all__ = ["app"]
