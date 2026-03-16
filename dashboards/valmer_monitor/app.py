from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

from mainsequence.dashboards.streamlit.scaffold import PageConfig, run_page

APP_DIR = Path(__file__).resolve().parent
REPO_ROOT = APP_DIR.parent.parent
for path in (APP_DIR, REPO_ROOT):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from valmer_dashboard import (
    category_breakdown,
    latest_curve_points,
    latest_vector_snapshot,
    load_curve_health,
    load_pricing_health,
    load_vector_history,
    render_empty_header,
    render_kpi_cards,
    render_sidebar_context,
    render_title,
    runtime_notes,
    selected_asset_history,
    selected_asset_snapshot,
    source_activity_counts,
    top_movers,
)

run_page(
    PageConfig(
        title="Valmer Connector Monitor",
        render_header=render_empty_header,
        use_wide_layout=True,
        inject_theme_css=True,
    )
)

render_title(
    "Valmer Connector Monitor",
    "Backend-aware monitoring for source vectors, pricing hydration, and curve publication paths.",
)

vector_history = load_vector_history()
pricing_health = load_pricing_health()
curve_health = load_curve_health()

latest_vector = (
    latest_vector_snapshot(vector_history.data) if vector_history.data is not None else pd.DataFrame()
)
standard_curve_points = latest_curve_points(curve_health["standard"].data)
selected_uid = render_sidebar_context(latest_vector)
target_bonds = pricing_health.get("target", pd.DataFrame())

render_kpi_cards(
    [
        (
            "Vector Rows",
            str(len(vector_history.data.index)) if vector_history.error is None else "n/a",
            vector_history.storage_hash or vector_history.error or "No vector storage resolved.",
        ),
        (
            "Latest Assets",
            str(latest_vector["unique_identifier"].nunique()) if not latest_vector.empty else "0",
            "Latest unique identifiers visible in the source node.",
        ),
        (
            "Target Bonds",
            str(target_bonds["unique_identifier"].nunique()) if not target_bonds.empty else "0",
            "Latest snapshot rows that match the pricing-detail target universe.",
        ),
        (
            "Pricing Gaps",
            str(len(pricing_health.get("missing_pricing", [])))
            if not pricing_health.get("error")
            else "n/a",
            "Target bond assets missing pricing details on the platform.",
        ),
        (
            "Curve Points",
            str(len(standard_curve_points.index)) if not standard_curve_points.empty else "0",
            curve_health["standard"].storage_hash or curve_health["standard"].error or "Standard curve path unavailable.",
        ),
    ]
)

left, right = st.columns((1.2, 0.8))

with left:
    st.subheader("Source Overview")
    if vector_history.error:
        st.error(f"Vector query failed: {vector_history.error}")
    elif latest_vector.empty:
        st.warning("No recent Valmer vector rows were returned.")
    elif selected_uid:
        asset_history = selected_asset_history(vector_history.data, selected_uid)
        latest_asset = selected_asset_snapshot(latest_vector, selected_uid)
        if latest_asset is None:
            st.warning(f"{selected_uid} is not present in the queried Valmer window.")
        else:
            profile_cols = [
                col
                for col in (
                    "full_name",
                    "sector",
                    "issue_currency",
                    "underlying",
                    "dirty_price",
                    "yield_rate",
                    "duration",
                    "fitch_rating",
                    "time_index",
                )
                if col in latest_asset.index
            ]
            st.dataframe(
                latest_asset[profile_cols].rename_axis("field").reset_index(name="value"),
                use_container_width=True,
                hide_index=True,
            )
            if not asset_history.empty and "time_index" in asset_history.columns:
                chart_cols = [col for col in ("close", "yield_rate") if col in asset_history.columns]
                if chart_cols:
                    st.line_chart(
                        asset_history.set_index("time_index")[chart_cols],
                        use_container_width=True,
                    )
    else:
        movers = top_movers(latest_vector)
        if movers.empty:
            st.info("No mover statistics are available yet.")
        else:
            st.dataframe(movers, use_container_width=True, hide_index=True)

        snapshot_cols = [
            col
            for col in (
                "time_index",
                "unique_identifier",
                "dirty_price",
                "clean_price",
                "yield_rate",
                "issue_currency",
                "underlying",
            )
            if col in latest_vector.columns
        ]
        st.dataframe(latest_vector[snapshot_cols].head(20), use_container_width=True, hide_index=True)

with right:
    st.subheader("Source Activity")
    activity = source_activity_counts(vector_history.data)
    if activity.empty:
        st.info("No activity history is available yet.")
    else:
        st.line_chart(activity.set_index("day")["asset_count"], use_container_width=True)

    st.subheader("Sector Mix")
    sector_mix = category_breakdown(latest_vector, "sector", top_n=10)
    if sector_mix.empty:
        st.info("No sector breakdown is available yet.")
    else:
        st.bar_chart(sector_mix.set_index("sector")["count"], use_container_width=True)

    st.subheader("Curve Status")
    for label, result in curve_health.items():
        if result.error:
            st.error(f"{label.title()} path: {result.error}")
            continue
        if result.data.empty:
            st.warning(f"{label.title()} path returned no rows.")
            continue
        latest_row = result.data.sort_values("time_index").iloc[-1]
        st.success(
            f"{label.title()} path updated at {latest_row['time_index']} with storage `{result.storage_hash}`."
        )

    st.subheader("Runtime Notes")
    for note in runtime_notes():
        st.markdown(f"- {note}")

st.info(
    "Use the multipage navigation for detailed source exploration, pricing hydration checks, and curve monitoring."
)
