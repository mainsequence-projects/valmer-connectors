# Dashboards

## Entry Point

The dashboard entry point is:

- `dashboards/valmer_monitor/app.py`

## Pages

The project-specific dashboard now includes:

- overview page in `app.py`
- `pages/1_Source_Data.py`
- `pages/2_Pricing_Hydration.py`
- `pages/3_Curve_Health.py`

## What It Shows

- recent `vector_de_precios_valmer` coverage
- focused source exploration for a selected Valmer asset or `unique_identifier`
- target-bond pricing-detail hydration gaps
- standard curve health plus visibility into the standalone curve table if it
  already exists

The dashboard reuses MainSequence Streamlit scaffolding plus sidebar components
for authenticated-user display and asset lookup, then layers Valmer-specific
charts and tables on top of the stored DataNode schema.

The curve page monitors the wired standard path and also checks whether the
standalone curve table exists, but it does not imply that the standalone node is
part of the checked-in execution flow.

## Failure Handling

The dashboard surfaces backend query failures directly in the UI instead of
silently degrading.

That makes it useful both as a monitoring tool and as a deployment verification
surface after sync and image creation.
