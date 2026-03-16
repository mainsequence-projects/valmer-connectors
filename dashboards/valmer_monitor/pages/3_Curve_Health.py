from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

from mainsequence.dashboards.streamlit.scaffold import PageConfig, run_page

APP_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
for path in (APP_DIR, REPO_ROOT):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from valmer_dashboard import (
    latest_curve_points,
    load_curve_health,
    render_empty_header,
    render_sidebar_context,
    render_title,
    runtime_notes,
)

run_page(
    PageConfig(
        title="Valmer Curve Health",
        render_header=render_empty_header,
        use_wide_layout=True,
        inject_theme_css=True,
    )
)

render_title(
    "Curve Health",
    "Monitor the wired standard discount-curve path and, when present, inspect the standalone Valmer curve table.",
)

lookback_days = st.slider("Lookback days", min_value=1, max_value=90, value=30)
curve_payload = load_curve_health(lookback_days=lookback_days)
render_sidebar_context(None, allow_asset_lookup=False)

for label, result in curve_payload.items():
    st.subheader(f"{label.title()} Path")
    if result.error:
        st.error(result.error)
        continue
    if result.data.empty:
        st.warning("No rows returned for this path.")
        continue

    latest_points = latest_curve_points(result.data)
    if latest_points.empty:
        st.warning("The path returned rows but no curve payload could be decoded.")
        continue

    st.metric("Latest curve points", len(latest_points.index))
    st.line_chart(latest_points.set_index("days_to_maturity")["zero_rate"])
    st.dataframe(latest_points, use_container_width=True, hide_index=True)

st.subheader("Runtime Notes")
for note in runtime_notes():
    st.markdown(f"- {note}")
