# Valmer Spread Analysis

Streamlit dashboard for M Bonos relative-value spreads. It reads Valmer
`yield_rate` history and latest market snapshots from published
`ValmerVectorPricesStorage` data, displays a zero-centered z-score heatmap,
computes selected-pair metrics with DV01 hedge ratios, and models spread
forecast cones.

Run locally:

```bash
.venv/bin/streamlit run dashboards/spread_analysis/app.py
```

Primary controls:

- List A/List B choose instrument universes from `AVAILABLE_LISTS` in `app.py`.
- Z-window, max rows, and max columns control the heatmap calculation.
- Start/end dates control Valmer history reads; there is no hardcoded valuation date.
- Asset A/Asset B are persisted in query params for selected-pair routing.
- Forecast horizon, confidence, and model choose OU or AR(1)+GARCH when `arch` is available.

Data dependencies:

- Valmer vector price history keyed by `asset_identifier`
- `yield_rate`, `dirty_price`, `duration`, `macaulay_duration`, and related fixed-income fields
  from Valmer storage
- optional `arch` dependency for GARCH forecast cones

Implementation boundary:

- the app consumes `valmer_connectors.analytics.spread_market_data`
- dashboard-specific Streamlit helpers and plotting helpers stay in `app.py`
- no extra core `valmer_connectors` service or analytics module is required

Implemented calculations:

- Spread: `yield_A - yield_B`.
- Z-score: latest aligned spread minus window mean divided by population standard deviation.
- Heatmap color scale: centered at zero with a configurable absolute cap.
- DV01: `dirty_price * modified_duration * 1bp`, separate from Macaulay duration.
- Hedge ratio: `DV01_A / DV01_B`.
- Forecasts: OU/AR(1) closed form and AR(1)+GARCH(1,1) with OU fallback if fitting fails.

Tabs:

- Z-Score Heatmap: plot and numeric matrix.
- Selected Pair: pair metrics, historical spread chart, and CSV download.
- Forecast Cone: forecast mean and upper/lower cone.
- Diagnostics: forecast diagnostics and latest Valmer snapshot.
