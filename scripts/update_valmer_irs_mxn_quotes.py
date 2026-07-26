"""Publish the complete Valmer IRS MXN quote snapshot."""

from valmer_connectors.data_nodes.curve_quote_indices import (
    run_valmer_irs_mxn_quote_update,
)

if __name__ == "__main__":
    run_valmer_irs_mxn_quote_update()
