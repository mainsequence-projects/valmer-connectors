from __future__ import annotations

import datetime as dt
import math
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

APP_DIR = Path(__file__).resolve().parent
REPO_ROOT = APP_DIR.parent.parent
for path in (APP_DIR, REPO_ROOT):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from msm_pricing.analytics.spreads import (
    build_pair_history_frame as canonical_pair_history_frame,
)
from msm_pricing.analytics.spreads import (
    build_spread_series,
    dv01_hedge_ratio,
    ornstein_uhlenbeck_forecast_cone,
    spread_zscore,
)
from scipy.stats import norm

from valmer_connectors.analytics import spread_market_data

AVAILABLE_LISTS = {
    "M Bonos": [
        "M_BONOS_240905",
        "M_BONOS_241205",
        "M_BONOS_250306",
        "M_BONOS_260305",
        "M_BONOS_260903",
        "M_BONOS_270304",
        "M_BONOS_270603",
        "M_BONOS_280302",
        "M_BONOS_290301",
        "M_BONOS_290531",
        "M_BONOS_300228",
        "M_BONOS_310529",
        "M_BONOS_320415",
        "M_BONOS_330526",
        "M_BONOS_341123",
        "M_BONOS_360221",
        "M_BONOS_361120",
        "M_BONOS_381118",
        "M_BONOS_421113",
        "M_BONOS_471107",
        "M_BONOS_530731",
    ]
}


def configure_page(title: str, *, layout: str = "wide") -> None:
    st.set_page_config(page_title=title, layout=layout)
    st.title(title)


@st.cache_resource(show_spinner="Initializing Valmer runtime")
def bootstrap_runtime_once() -> object:
    from valmer_connectors.instruments.bootstrap import bootstrap_runtime

    return bootstrap_runtime(seed_static_rows=False)


def display_error_list(errors: Iterable[Mapping[str, Any]] | None) -> None:
    rows = [dict(error) for error in errors or []]
    if rows:
        st.error(f"{len(rows)} issue(s) found.")
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)


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


def _query_param(name: str, default: str) -> str:
    value = st.query_params.get(name, default)
    if isinstance(value, list):
        return str(value[0]) if value else default
    return str(value)


def _plot_z_heatmap(matrix: pd.DataFrame, *, z_cap: float) -> go.Figure:
    figure = px.imshow(
        matrix,
        color_continuous_scale="RdBu_r",
        zmin=-float(z_cap),
        zmax=float(z_cap),
        aspect="auto",
        text_auto=".2f",
        title="Spread Z-Score Matrix",
    )
    return figure


def _plot_pair_history(history: pd.DataFrame) -> go.Figure:
    if history.empty:
        return go.Figure()
    figure = px.line(history, x="time_index", y="spread", title="Historical Spread")
    return figure


def _plot_forecast_cone(cone: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if cone.empty:
        return figure
    figure.add_trace(go.Scatter(x=cone["step"], y=cone["upper"], mode="lines", name="Upper"))
    figure.add_trace(go.Scatter(x=cone["step"], y=cone["mean"], mode="lines", name="Mean"))
    figure.add_trace(go.Scatter(x=cone["step"], y=cone["lower"], mode="lines", name="Lower"))
    figure.update_layout(title="Forecast Cone", xaxis_title="Business Days", yaxis_title="Spread")
    return figure


def _metrics_frame(metrics: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for label in ("asset_a", "asset_b"):
        row = dict(metrics.get(label, {}))
        row["leg"] = label
        rows.append(row)
    return pd.DataFrame(rows)


def _build_spread_z_matrix(
    yields: pd.DataFrame,
    rows: list[str],
    cols: list[str],
    window: int,
) -> pd.DataFrame:
    matrix = pd.DataFrame(index=rows, columns=cols, dtype=float)
    for asset_a in rows:
        for asset_b in cols:
            if asset_a == asset_b:
                matrix.loc[asset_a, asset_b] = np.nan
                continue
            spread = _spread_series(yields, asset_a, asset_b).tail(int(window))
            z_score = spread_zscore(spread, ddof=0, min_observations=int(window))
            matrix.loc[asset_a, asset_b] = np.nan if z_score is None else z_score
    return matrix


def _spread_series(yields: pd.DataFrame, asset_a: str, asset_b: str) -> pd.Series:
    if yields.empty or asset_a not in yields.columns or asset_b not in yields.columns:
        return pd.Series(dtype=float, name="spread")
    try:
        spread = build_spread_series(
            yields[asset_a],
            yields[asset_b],
            hedge_ratio=1.0,
            name="spread",
            leg_a_name=asset_a,
            leg_b_name=asset_b,
        )
    except ValueError:
        return pd.Series(dtype=float, name="spread")
    return spread.values


def _pair_history_frame(yields: pd.DataFrame, asset_a: str, asset_b: str) -> pd.DataFrame:
    if yields.empty or asset_a not in yields.columns or asset_b not in yields.columns:
        return _empty_pair_history_frame()
    try:
        frame = canonical_pair_history_frame(
            yields[asset_a],
            yields[asset_b],
            hedge_ratio=1.0,
            leg_a_name=asset_a,
            leg_b_name=asset_b,
        )
    except ValueError:
        return _empty_pair_history_frame()
    frame = frame.rename(columns={"leg_a": "yield_a", "leg_b": "yield_b"}).reset_index(
        names="time_index"
    )
    frame["asset_a"] = asset_a
    frame["asset_b"] = asset_b
    return frame[["time_index", "asset_a", "asset_b", "yield_a", "yield_b", "spread"]]


def _pair_metrics(
    asset_a: str,
    asset_b: str,
    market_snapshot: pd.DataFrame,
    *,
    horizon_days: int,
) -> dict[str, Any]:
    rows = _snapshot_by_identifier(market_snapshot)
    leg_a = _leg_metrics(asset_a, rows.get(asset_a, {}), horizon_days=horizon_days)
    leg_b = _leg_metrics(asset_b, rows.get(asset_b, {}), horizon_days=horizon_days)
    try:
        hedge_ratio = dv01_hedge_ratio(float(leg_a.get("dv01")), float(leg_b.get("dv01")))
    except (TypeError, ValueError):
        hedge_ratio = np.nan
    return {
        "asset_a": leg_a,
        "asset_b": leg_b,
        "hedge_ratio": hedge_ratio,
        "spread": (
            leg_a["current_yield"] - leg_b["current_yield"]
            if _finite(leg_a["current_yield"]) and _finite(leg_b["current_yield"])
            else np.nan
        ),
    }


def _leg_metrics(
    asset_identifier: str,
    row: dict[str, Any],
    *,
    horizon_days: int,
) -> dict[str, Any]:
    dirty_price = _float_or_nan(row.get("dirty_price"))
    duration = _first_finite(row.get("macaulay_duration"), row.get("duration"))
    modified_duration = _float_or_nan(row.get("duration"))
    current_yield = _float_or_nan(row.get("yield_rate"))
    dv01 = _calculate_dv01(dirty_price=dirty_price, modified_duration=modified_duration)
    carry_roll_down = (
        dirty_price * current_yield * float(horizon_days) / 360.0
        if _finite(dirty_price) and _finite(current_yield)
        else np.nan
    )
    return {
        "asset_identifier": asset_identifier,
        "current_yield": current_yield,
        "dirty_price": dirty_price,
        "dv01": dv01,
        "macaulay_duration": duration,
        "modified_duration": modified_duration,
        "carry_roll_down": carry_roll_down,
    }


def _calculate_dv01(*, dirty_price: float, modified_duration: float) -> float:
    if not _finite(dirty_price) or not _finite(modified_duration):
        return np.nan
    return float(dirty_price) * float(modified_duration) * 0.0001


def _arch_available() -> bool:
    try:
        import arch  # noqa: F401
    except Exception:
        return False
    return True


def _forecast_cone_ar1_garch(
    pair: pd.Series,
    horizon: int,
    confidence: float = 0.68,
    use_t: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    series = _clean_series(pair)
    if len(series) < 30:
        return _forecast_cone_ou(
            series,
            horizon,
            confidence,
            method="ou_fallback_insufficient_data",
        )
    try:
        from arch import arch_model

        dist = "StudentsT" if use_t else "normal"
        model = arch_model(series, mean="ARX", lags=1, vol="GARCH", p=1, q=1, dist=dist)
        result = model.fit(disp="off")
        forecast = result.forecast(horizon=int(horizon), reindex=False)
        mean = forecast.mean.iloc[-1].to_numpy(dtype=float)
        variance = forecast.variance.iloc[-1].to_numpy(dtype=float)
        z_value = norm.ppf((1.0 + float(confidence)) / 2.0)
        std = np.sqrt(np.maximum(variance, 0.0))
        frame = pd.DataFrame(
            {
                "step": np.arange(1, int(horizon) + 1),
                "mean": mean,
                "lower": mean - z_value * std,
                "upper": mean + z_value * std,
            }
        )
        return frame, {"method": "ar1_garch", "arch_available": True, "confidence": confidence}
    except Exception as exc:
        frame, diagnostics = _forecast_cone_ou(
            series,
            horizon,
            confidence,
            method="ou_fallback",
        )
        diagnostics["garch_error"] = str(exc)
        return frame, diagnostics


def _forecast_cone_ou(
    pair: pd.Series,
    horizon: int,
    confidence: float = 0.68,
    *,
    method: str = "ou",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    series = _clean_series(pair)
    if len(series) < 3:
        empty = pd.DataFrame(columns=["step", "mean", "lower", "upper"])
        return empty, {"method": method, "error": "insufficient data", "confidence": confidence}
    z_value = norm.ppf((1.0 + float(confidence)) / 2.0)
    try:
        cone = ornstein_uhlenbeck_forecast_cone(
            series,
            horizon=int(horizon),
            std_multipliers=(z_value,),
        )
    except ValueError as exc:
        empty = pd.DataFrame(columns=["step", "mean", "lower", "upper"])
        return empty, {"method": method, "error": str(exc), "confidence": confidence}

    label = _std_multiplier_label(z_value)
    frame = cone.reset_index().rename(
        columns={
            "horizon": "step",
            "expected": "mean",
            f"lower_{label}": "lower",
            f"upper_{label}": "upper",
        }
    )
    return (
        frame[["step", "mean", "lower", "upper"]],
        {"method": method, "confidence": confidence},
    )


def _snapshot_by_identifier(frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if frame.empty or "asset_identifier" not in frame.columns:
        return {}
    return {
        str(row["asset_identifier"]): dict(row)
        for row in frame.to_dict(orient="records")
        if row.get("asset_identifier")
    }


def _first_finite(*values: Any) -> float:
    for value in values:
        numeric = _float_or_nan(value)
        if _finite(numeric):
            return numeric
    return np.nan


def _float_or_nan(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return np.nan
    return numeric if math.isfinite(numeric) else np.nan


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _clean_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()


def _std_multiplier_label(multiplier: float) -> str:
    if float(multiplier).is_integer():
        return f"{int(multiplier)}std"
    return f"{str(multiplier).replace('.', '_')}std"


def _empty_pair_history_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["time_index", "asset_a", "asset_b", "yield_a", "yield_b", "spread"]
    )


def main() -> None:
    configure_page("Spread Analysis")
    st.caption("M Bono yield spread z-scores, pair history, hedge ratios, and forecast cones.")

    available_lists = AVAILABLE_LISTS
    list_names = list(available_lists)
    st.sidebar.header("Universe")
    row_list_name = st.sidebar.selectbox("List A", options=list_names, index=0)
    col_list_name = st.sidebar.selectbox("List B", options=list_names, index=0)
    z_window = st.sidebar.number_input("Z-window", min_value=20, max_value=1000, value=180)
    horizon = st.sidebar.number_input("Forecast horizon", min_value=10, max_value=504, value=90)
    max_rows = st.sidebar.number_input("Max rows", min_value=1, max_value=100, value=50)
    max_cols = st.sidebar.number_input("Max columns", min_value=1, max_value=100, value=50)
    z_cap = st.sidebar.number_input("Heatmap abs cap", min_value=0.5, max_value=10.0, value=3.0)
    start_date = st.sidebar.date_input(
        "Start date",
        value=spread_market_data.default_start_date().date(),
    )
    end_date = st.sidebar.date_input("End date", value=dt.date.today())
    confidence = st.sidebar.slider("Forecast confidence", min_value=0.50, max_value=0.99, value=0.68)
    model_options = ["OU"]
    if _arch_available():
        model_options.insert(0, "AR(1)+GARCH")
    forecast_model = st.sidebar.selectbox("Forecast model", options=model_options)
    load = st.sidebar.button("Load spread data", width="stretch")

    rows = available_lists[row_list_name][: int(max_rows)]
    cols = available_lists[col_list_name][: int(max_cols)]
    selected_a = _query_param("asset_a", rows[0])
    selected_b = _query_param("asset_b", cols[1] if len(cols) > 1 else cols[0])
    selected_a = st.sidebar.selectbox(
        "Asset A",
        options=rows,
        index=rows.index(selected_a) if selected_a in rows else 0,
    )
    selected_b = st.sidebar.selectbox(
        "Asset B",
        options=cols,
        index=cols.index(selected_b) if selected_b in cols else min(1, len(cols) - 1),
    )
    st.query_params["asset_a"] = selected_a
    st.query_params["asset_b"] = selected_b

    if "spread_outputs" not in st.session_state:
        st.session_state.spread_outputs = {}
    if load:
        errors: list[dict[str, Any]] = []
        identifiers = list(dict.fromkeys([*rows, *cols, selected_a, selected_b]))
        try:
            bootstrap_runtime_once()
            yields = spread_market_data.fetch_yield_history(
                identifiers,
                start_date=pd.Timestamp(start_date, tz="UTC"),
                end_date=pd.Timestamp(end_date, tz="UTC"),
            )
            snapshot = spread_market_data.fetch_market_snapshot(
                [selected_a, selected_b],
                as_of=pd.Timestamp(end_date, tz="UTC"),
            )
        except Exception as exc:
            yields = pd.DataFrame()
            snapshot = pd.DataFrame()
            errors.append({"stage": "spread_market_data", "message": str(exc)})
        st.session_state.spread_outputs = {
            "yields": yields,
            "snapshot": snapshot,
            "errors": errors,
        }

    outputs = st.session_state.spread_outputs
    if not outputs:
        st.info("Use the sidebar load action to fetch Valmer yield history.")
        if "AR(1)+GARCH" not in model_options:
            st.warning("GARCH is disabled because the optional arch dependency is unavailable.")
        return

    errors = outputs.get("errors", [])
    display_error_list(errors)
    yields = outputs.get("yields", pd.DataFrame())
    snapshot = outputs.get("snapshot", pd.DataFrame())
    if not isinstance(yields, pd.DataFrame) or yields.empty:
        st.warning("No yield history is available for the selected universe and date range.")
        return

    z_matrix = _build_spread_z_matrix(yields, rows, cols, int(z_window))
    pair_history = _pair_history_frame(yields, selected_a, selected_b)
    pair_spread = _spread_series(yields, selected_a, selected_b)
    metrics = _pair_metrics(
        selected_a,
        selected_b,
        snapshot if isinstance(snapshot, pd.DataFrame) else pd.DataFrame(),
        horizon_days=int(horizon),
    )
    if forecast_model == "AR(1)+GARCH":
        cone, diagnostics = _forecast_cone_ar1_garch(
            pair_spread,
            horizon=int(horizon),
            confidence=float(confidence),
        )
    else:
        cone, diagnostics = _forecast_cone_ou(
            pair_spread,
            horizon=int(horizon),
            confidence=float(confidence),
        )

    heatmap_tab, pair_tab, forecast_tab, diagnostics_tab = st.tabs(
        ["Z-Score Heatmap", "Selected Pair", "Forecast Cone", "Diagnostics"]
    )

    with heatmap_tab:
        st.plotly_chart(_plot_z_heatmap(z_matrix, z_cap=float(z_cap)), width="stretch")
        with st.expander("Numeric Matrix"):
            st.dataframe(z_matrix, width="stretch")

    with pair_tab:
        st.subheader(f"{selected_a} - {selected_b}")
        metric_columns = st.columns(3)
        metric_columns[0].metric("Current Spread", f"{metrics.get('spread', float('nan')):,.4f}")
        metric_columns[1].metric("DV01 Hedge Ratio", f"{metrics.get('hedge_ratio', float('nan')):,.4f}")
        metric_columns[2].metric("History Rows", len(pair_history))
        st.dataframe(_metrics_frame(metrics), width="stretch", hide_index=True)
        st.plotly_chart(_plot_pair_history(pair_history), width="stretch")
        download_dataframe_button(pair_history, f"{selected_a}_{selected_b}_spread.csv")

    with forecast_tab:
        st.plotly_chart(_plot_forecast_cone(cone), width="stretch")
        st.dataframe(cone, width="stretch", hide_index=True)

    with diagnostics_tab:
        st.json(diagnostics)
        if "AR(1)+GARCH" not in model_options:
            st.warning("GARCH is disabled because the optional arch dependency is unavailable.")
        st.dataframe(snapshot, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
