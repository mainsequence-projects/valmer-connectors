"""Publish the dependency-backed Valmer TIIE curve."""

from valmer_connectors.services.curve_update import run_tiie_irs_mxn_curve_update

if __name__ == "__main__":
    run_tiie_irs_mxn_curve_update()
