from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

APP_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
for path in (APP_DIR, REPO_ROOT):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from page_bootstrap import PageConfig, run_page
from valmer_dashboard import (
    category_breakdown,
    load_vector_history,
    load_vector_snapshot,
    render_empty_header,
    render_sidebar_context,
    render_title,
    selected_asset_history,
    selected_asset_snapshot,
    source_activity_counts,
    top_movers,
)

run_page(
    PageConfig(
        title="Valmer Source Data",
        render_header=render_empty_header,
        use_wide_layout=True,
        inject_theme_css=True,
    )
)

render_title(
    "Source Data",
    "Current backend snapshot from `vector_de_precios_valmer` with bounded history for activity charts.",
)

lookback_days = st.slider("Lookback days", min_value=1, max_value=60, value=14)
history = load_vector_history(lookback_days=lookback_days)
snapshot = load_vector_snapshot()

if snapshot.error:
    st.error(f"Vector snapshot query failed: {snapshot.error}")
else:
    latest = snapshot.data
    selected_uid = render_sidebar_context(latest)
    st.metric("Raw history rows", 0 if history.error else len(history.data.index))
    st.metric("Latest unique identifiers", latest["unique_identifier"].nunique() if not latest.empty else 0)

    if history.error:
        st.error(f"Vector history query failed: {history.error}")
        counts = None
    else:
        counts = source_activity_counts(history.data)
    if counts is not None and not counts.empty:
        st.line_chart(counts.set_index("day")["asset_count"], use_container_width=True)

    if latest.empty:
        st.warning("No latest snapshot rows were returned.")
    else:
        overview_left, overview_right = st.columns((1.1, 0.9))

        with overview_left:
            st.subheader("Latest Snapshot")
            preview_cols = [
                col
                for col in (
                    "time_index",
                    "unique_identifier",
                    "dirty_price",
                    "clean_price",
                    "yield_rate",
                    "issue_currency",
                    "underlying",
                    "sector",
                )
                if col in latest.columns
            ]
            st.dataframe(latest[preview_cols].head(50), use_container_width=True, hide_index=True)

        with overview_right:
            st.subheader("Top Movers")
            movers = top_movers(latest, limit=12)
            if movers.empty:
                st.info("No change metrics are available in the latest snapshot.")
            else:
                st.dataframe(movers, use_container_width=True, hide_index=True)

            st.subheader("Currency Mix")
            currency_mix = category_breakdown(latest, "issue_currency", top_n=10)
            if not currency_mix.empty:
                st.bar_chart(currency_mix.set_index("issue_currency")["count"], use_container_width=True)

        st.subheader("Sector Distribution")
        sector_mix = category_breakdown(latest, "sector", top_n=12)
        if sector_mix.empty:
            st.info("No sector labels are available in the latest snapshot.")
        else:
            st.bar_chart(sector_mix.set_index("sector")["count"], use_container_width=True)

        if selected_uid:
            st.subheader(f"Focused Asset: {selected_uid}")
            asset_history = (
                pd.DataFrame()
                if history.error
                else selected_asset_history(history.data, selected_uid)
            )
            latest_asset = selected_asset_snapshot(latest, selected_uid)
            if latest_asset is None:
                st.warning(f"{selected_uid} is not present in the current Valmer snapshot.")
            else:
                detail_cols = [
                    col
                    for col in (
                        "full_name",
                        "sector",
                        "issue_currency",
                        "underlying",
                        "dirty_price",
                        "yield_rate",
                        "duration",
                        "adjusted_face_value",
                        "fitch_rating",
                        "time_index",
                    )
                    if col in latest_asset.index
                ]
                st.dataframe(
                    latest_asset[detail_cols].rename_axis("field").reset_index(name="value"),
                    use_container_width=True,
                    hide_index=True,
                )
                chart_cols = [col for col in ("close", "yield_rate") if col in asset_history.columns]
                if not asset_history.empty and chart_cols:
                    st.line_chart(
                        asset_history.set_index("time_index")[chart_cols],
                        use_container_width=True,
                    )
