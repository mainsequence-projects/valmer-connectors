"""Publish the Valmer MXN government curve from persisted vector observations."""

from valmer_connectors.services.curve_update import run_mxn_government_curve_update

if __name__ == "__main__":
    run_mxn_government_curve_update()
