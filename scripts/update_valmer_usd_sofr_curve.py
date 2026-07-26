"""Publish the dependency-backed Valmer USD SOFR curve."""

from valmer_connectors.services.curve_update import run_usd_sofr_curve_update

if __name__ == "__main__":
    run_usd_sofr_curve_update()
