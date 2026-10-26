"""Publish the Valmer MXN government curve from persisted vector observations."""

from valmer_connectors.runtime import use_markets_api
from valmer_connectors.services.curve_update import run_mxn_government_curve_update

if __name__ == "__main__":
    use_markets_api()
    run_mxn_government_curve_update()
