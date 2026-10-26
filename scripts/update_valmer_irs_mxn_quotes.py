"""Publish the complete Valmer IRS MXN quote snapshot."""

from valmer_connectors.data_nodes.curve_quote_indices import (
    run_valmer_irs_mxn_quote_update,
)
from valmer_connectors.runtime import use_markets_api

if __name__ == "__main__":
    use_markets_api()
    run_valmer_irs_mxn_quote_update()
