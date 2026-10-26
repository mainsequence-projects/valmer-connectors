"""Publish the dependency-backed Valmer USD/MXN XCCY curve."""

from valmer_connectors.runtime import use_markets_api
from valmer_connectors.services.curve_update import run_usd_mxn_xccy_curve_update

if __name__ == "__main__":
    use_markets_api()
    run_usd_mxn_xccy_curve_update()
