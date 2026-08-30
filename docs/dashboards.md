# Dashboards

## Entry Points

The dashboard entry points are:

- `dashboards/valmer_monitor/app.py`
- `dashboards/spread_analysis/app.py`

## Valmer Monitor Pages

The Valmer monitor dashboard includes:

- overview page in `app.py`
- `pages/1_Source_Data.py`
- `pages/2_Pricing_Hydration.py`
- `pages/3_Curve_Health.py`

## Valmer Monitor

- recent `vector_de_precios_valmer` coverage
- focused source exploration for a selected Valmer asset or `unique_identifier`
- target-bond pricing-detail hydration gaps
- canonical `discount_curves` time-index table health for the Valmer TIIE and MXN
  government curves

The dashboard reuses MainSequence Streamlit scaffolding plus sidebar components
for authenticated-user display and asset lookup, then layers Valmer-specific
charts and tables on top of the stored output table schema.

The curve page monitors `msm_pricing.data_nodes.DiscountCurvesNode` output. It
no longer reads the old standalone curve table.

## Spread Analysis

`dashboards/spread_analysis/app.py` is a single-page Valmer fixed-income
relative-value example. It reads Valmer vector yield history and latest market
snapshots through `valmer_connectors.analytics.spread_market_data`, then uses
`msm_pricing.analytics.spreads` for canonical spread, z-score, hedge-ratio, and
OU forecast-cone calculations.

The dashboard keeps its Streamlit setup, download button, error display,
Plotly chart builders, query-parameter handling, AR(1)+GARCH optional model,
and other UI helpers inside `dashboards/spread_analysis/app.py`. It does not
add a new core `valmer_connectors` service layer.

## Failure Handling

The dashboards surface backend query failures directly in the UI instead of
silently degrading.

That makes them useful both as monitoring tools and as deployment verification
surfaces after sync and image creation.
