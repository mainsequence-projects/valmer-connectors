import msm
from msm.models import AssetTable
from msm_pricing.bootstrap import attach_pricing_schemas
from msm_pricing.models.pricing_details import AssetCurrentPricingDetailsTable

from valmer_connectors.control_plane.api import create_app

msm.start_engine(models=[AssetTable])
attach_pricing_schemas(
    models=[AssetTable, AssetCurrentPricingDetailsTable],
    seed_default_market_data_bindings=False,
)
app = create_app()

__all__ = ["app"]
