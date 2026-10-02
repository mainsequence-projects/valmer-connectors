"""FRED source integration for external reference-rate observations."""

from valmer_connectors.fred.reference_rates import (
    FredReferenceRatesNode,
    run_fred_reference_rates_update,
)

__all__ = ["FredReferenceRatesNode", "run_fred_reference_rates_update"]
