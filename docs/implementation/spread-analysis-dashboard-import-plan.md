# Spread Analysis Dashboard Import Plan

## Goal

Import only the donor `spread_analysis` Streamlit dashboard into this project as a
Valmer connector example dashboard.

The imported dashboard must remain dashboard-owned. The implementation must not
create new core `valmer_connectors` services, query helpers, analytics modules,
or reusable library code. The existing core read path already exists through
`valmer_connectors.analytics.spread_market_data`; the dashboard should consume
that API directly.

## Donor

Copy from:

```text
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mexicofundcompetition-9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16/dashboards/spread_analysis/app.py
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mexicofundcompetition-9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16/dashboards/spread_analysis/README.md
```

Do not copy:

```text
.DS_Store
__pycache__/
*.pyc
```

## Target Files

Create:

```text
dashboards/spread_analysis/app.py
dashboards/spread_analysis/README.md
```

Update:

```text
docs/dashboards.md
docs/SUMMARY.md
pyproject.toml
uv.lock
requirements.txt
```

Do not create or modify for this migration:

```text
src/valmer_connectors/**
dashboards/components/**
```

The dashboard may define small Streamlit helpers inside
`dashboards/spread_analysis/app.py`. Do not introduce shared dashboard helper
modules for this import.

## Required Dashboard Edits

The donor app imports shared dashboard helpers from the fund competition
project:

```python
from dashboards.components.streamlit_common import configure_page
from dashboards.components.streamlit_common import bootstrap_runtime_once
from dashboards.components.streamlit_common import display_error_list
from dashboards.components.streamlit_common import download_dataframe_button
```

Replace those imports with local functions in `dashboards/spread_analysis/app.py`.

Required local functions:

```python
def configure_page(title: str, *, layout: str = "wide") -> None:
    st.set_page_config(page_title=title, layout=layout)
    st.title(title)
```

```python
@st.cache_resource(show_spinner="Initializing Valmer runtime")
def bootstrap_runtime_once() -> object:
    from valmer_connectors.instruments.bootstrap import bootstrap_runtime

    return bootstrap_runtime(seed_static_rows=False)
```

```python
def display_error_list(errors: Iterable[Mapping[str, Any]] | None) -> None:
    rows = [dict(error) for error in errors or []]
    if rows:
        st.error(f"{len(rows)} issue(s) found.")
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
```

```python
def download_dataframe_button(
    df: pd.DataFrame,
    filename: str,
    *,
    label: str = "Download CSV",
) -> None:
    st.download_button(
        label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        disabled=df.empty,
    )
```

Keep the rest of the dashboard-local functionality in `app.py`:

- `AVAILABLE_LISTS`
- query-parameter handling
- Plotly chart builders
- z-score matrix construction
- pair-history formatting
- DV01 and hedge-ratio display helpers
- OU forecast cone helper
- AR(1)+GARCH helper
- Streamlit layout, controls, tabs, diagnostics, and downloads

Do not move these helpers into `src/valmer_connectors`.

## Data Path

The dashboard must keep this existing core dependency:

```python
from valmer_connectors.analytics import spread_market_data
```

The dashboard reads:

- `spread_market_data.fetch_yield_history(...)`
- `spread_market_data.fetch_market_snapshot(...)`
- `spread_market_data.default_start_date()`

Those functions already use the canonical Valmer vector query helpers. No new
storage query API is required for this dashboard import.

The dashboard must keep the generic analytics imports:

```python
from msm_pricing.analytics.spreads import (
    build_pair_history_frame as canonical_pair_history_frame,
    build_spread_series,
    dv01_hedge_ratio,
    ornstein_uhlenbeck_forecast_cone,
    spread_zscore,
)
```

## Dependencies

The donor dashboard requires dependencies that are not declared in this project
today:

```text
plotly
scipy
arch
```

Add them with `uv`:

```bash
uv add plotly scipy arch
uv export --format requirements-txt --output-file requirements.txt
```

`arch` is still optional at runtime in the dashboard UI because the app checks
availability before enabling AR(1)+GARCH. It should still be declared here so
the imported dashboard preserves the donor feature set when the project
environment is built from `pyproject.toml`.

## README Requirements

Create `dashboards/spread_analysis/README.md` from the donor README with the run
command set to this repository:

```bash
.venv/bin/streamlit run dashboards/spread_analysis/app.py
```

The README must state:

- this is a Valmer vector spread-analysis example;
- it reads Valmer `yield_rate` history and latest market snapshots;
- it depends on published `ValmerVectorPricesStorage` data;
- it computes spread z-scores, DV01 hedge ratio, pair history, and forecast
  cones;
- it keeps all dashboard-specific helpers inside the dashboard.

## Documentation Updates

Update `docs/dashboards.md` to list both dashboard roots:

```text
dashboards/valmer_monitor/app.py
dashboards/spread_analysis/app.py
```

Describe `spread_analysis` as a Valmer fixed-income relative-value example that
uses:

- Valmer vector yield history;
- latest dirty price and duration fields;
- `msm_pricing.analytics.spreads`;
- dashboard-local Streamlit UI helpers.

Update `docs/SUMMARY.md` to include this implementation plan under
`Implementation Plans`.

## Validation

After the import, run:

```bash
.venv/bin/python -m ruff check dashboards/spread_analysis/app.py
.venv/bin/python -m py_compile dashboards/spread_analysis/app.py
.venv/bin/python -c "import plotly, scipy, arch; print('spread dashboard deps ok')"
```

Then run a local Streamlit smoke check:

```bash
.venv/bin/streamlit run dashboards/spread_analysis/app.py
```

The manual smoke check passes when the app starts, the sidebar renders, and the
page shows the initial instruction to load Valmer spread data without import
errors.

## Out Of Scope

This import must not:

- move spread dashboard helpers into `src/valmer_connectors`;
- create new Valmer query helpers;
- create new Valmer services;
- import or copy any fundcompetition portfolio services;
- import or copy `dashboards/components/streamlit_common.py`;
- migrate `single_fi_analysis`;
- change Valmer vector storage contracts;
- change `spread_market_data.py`.
