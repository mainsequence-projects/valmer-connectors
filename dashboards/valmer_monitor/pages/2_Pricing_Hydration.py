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
    category_breakdown,
    load_pricing_health,
    render_empty_header,
    render_sidebar_context,
    render_title,
    selected_asset_snapshot,
)

run_page(
    PageConfig(
        title="Valmer Pricing Hydration",
        render_header=render_empty_header,
        use_wide_layout=True,
        inject_theme_css=True,
    )
)

render_title(
    "Pricing Hydration",
    "Target-bond coverage from the latest vector snapshot versus current platform asset pricing details.",
)

lookback_days = st.slider("Lookback days", min_value=1, max_value=60, value=14)
payload = load_pricing_health(lookback_days=lookback_days)

if payload.get("error"):
    st.error(f"Pricing health query failed: {payload['error']}")
else:
    latest = payload["latest"]
    target = payload["target"]
    missing_pricing = payload["missing_pricing"]
    selected_uid = render_sidebar_context(latest)

    first, second, third = st.columns(3)
    first.metric("Latest assets", latest["unique_identifier"].nunique() if not latest.empty else 0)
    second.metric("Target bonds", target["unique_identifier"].nunique() if not target.empty else 0)
    third.metric("Missing pricing details", len(missing_pricing))

    summary_left, summary_right = st.columns((1.0, 1.0))
    with summary_left:
        st.subheader("Target Underlyings")
        underlying_mix = category_breakdown(target, "underlying", top_n=10)
        if underlying_mix.empty:
            st.info("No target-underlying distribution is available.")
        else:
            st.bar_chart(underlying_mix.set_index("underlying")["count"], use_container_width=True)

    with summary_right:
        st.subheader("Target Currency Mix")
        currency_mix = category_breakdown(target, "issue_currency", top_n=10)
        if currency_mix.empty:
            st.info("No target-currency distribution is available.")
        else:
            st.bar_chart(currency_mix.set_index("issue_currency")["count"], use_container_width=True)

    if missing_pricing:
        st.subheader("Assets Missing Pricing Details")
        missing_frame = target[target["unique_identifier"].isin(missing_pricing)].copy()
        cols = [
            col
            for col in (
                "unique_identifier",
                "underlying",
                "issuer",
                "series",
                "yield_rate",
                "adjusted_face_value",
                "issue_currency",
            )
            if col in missing_frame.columns
        ]
        st.dataframe(missing_frame[cols], use_container_width=True, hide_index=True)
    else:
        st.success("All target bond assets visible in the latest snapshot have pricing details.")

    if selected_uid:
        st.subheader(f"Focused Asset: {selected_uid}")
        latest_asset = selected_asset_snapshot(target, selected_uid)
        if latest_asset is None:
            st.info(f"{selected_uid} is not part of the current target-bond universe.")
        else:
            status = "Missing pricing details" if selected_uid in missing_pricing else "Pricing detail present"
            st.metric("Focus status", status)
            detail_cols = [
                col
                for col in (
                    "full_name",
                    "underlying",
                    "issue_currency",
                    "yield_rate",
                    "adjusted_face_value",
                    "duration",
                    "time_index",
                )
                if col in latest_asset.index
            ]
            st.dataframe(
                latest_asset[detail_cols].rename_axis("field").reset_index(name="value"),
                use_container_width=True,
                hide_index=True,
            )

    st.subheader("Target Bond Universe")
    preview_cols = [
        col
        for col in (
            "time_index",
            "unique_identifier",
            "full_name",
            "underlying",
            "issue_currency",
            "yield_rate",
            "adjusted_face_value",
            "fitch_rating",
        )
        if col in target.columns
    ]
    st.dataframe(target[preview_cols], use_container_width=True, hide_index=True)
