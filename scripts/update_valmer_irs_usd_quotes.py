"""Publish the complete Valmer IRS USD quote snapshot."""

from valmer_connectors.data_nodes.curve_quote_indices import (
    run_valmer_irs_usd_quote_update,
)
from valmer_connectors.runtime import use_markets_api

if __name__ == "__main__":
    use_markets_api()
    run_valmer_irs_usd_quote_update()
