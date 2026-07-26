"""Publish the complete Valmer IRS USD quote snapshot."""

from valmer_connectors.data_nodes.curve_quote_indices import (
    run_valmer_irs_usd_quote_update,
)

if __name__ == "__main__":
    run_valmer_irs_usd_quote_update()
