import msm
from fastapi import FastAPI
from msm.models import AssetTable
from msm_pricing.bootstrap import attach_pricing_schemas
from msm_pricing.models.pricing_details import AssetCurrentPricingDetailsTable

from valmer_connectors.control_plane.api import create_app

msm.start_engine(models=[AssetTable])
attach_pricing_schemas(
    models=[AssetTable, AssetCurrentPricingDetailsTable],
    seed_default_market_data_bindings=False,
)
app = FastAPI(
    title="Valmer Control Plane API",
    version="1.0.0",
    description=(
        "Authenticated operational read models and approved Job actions for the "
        "Valmer Command Center static-site application."
    ),
)
create_app(app=app)

__all__ = ["app"]
