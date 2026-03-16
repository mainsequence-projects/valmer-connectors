from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

import mainsequence.client as msc
from mainsequence.client.models_tdag import DataNodeStorage
from mainsequence.dashboards.streamlit.components import (
    sidebar_asset_single_select,
    sidebar_logged_user_username,
)
from mainsequence.tdag import APIDataNode
from src.data_nodes.nodes import ImportValmer, MexDerTIIE28Zero
from src.instruments.bootstrap import (
    OPTIONAL_EXTERNAL_CURVE_CONST,
    REQUIRED_CURVE_CONSTS,
    REQUIRED_INDEX_CONSTS,
)

VECTOR_NODE_IDENTIFIER = "vector_de_precios_valmer"
STANDALONE_CURVE_NODE_IDENTIFIER = "valmer_mexder_tiie28_zero_curve"
CURVE_CONST_NAME = REQUIRED_CURVE_CONSTS[0]

NUMERIC_VECTOR_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "dirty_price",
    "clean_price",
    "yield_rate",
    "daily_change_pct",
    "weekly_change_pct",
    "duration",
    "monetary_duration",
    "macaulay_duration",
    "convexity",
    "adjusted_face_value",
)

DATETIME_VECTOR_COLUMNS = (
    "time_index",
    "valuation_date",
    "issue_date",
    "maturity_date",
    "uh_date",
    "max_price_date",
    "min_price_date",
)


@dataclass
class QueryResult:
    data: pd.DataFrame
    error: str | None = None
    storage_hash: str | None = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def inject_dashboard_css() -> None:
    st.markdown(
        """
        <style>
        :root {
            --valmer-panel-bg: linear-gradient(
                180deg,
                rgba(16, 22, 34, 0.90),
                rgba(9, 14, 24, 0.96)
            );
            --valmer-panel-border: rgba(124, 145, 180, 0.16);
            --valmer-panel-shadow: 0 18px 48px rgba(0, 0, 0, 0.28);
            --valmer-text-strong: #f5f7fb;
            --valmer-text-muted: #a7b2c3;
            --valmer-accent-warm: #e07a39;
            --valmer-accent-cool: #0f6e82;
        }
        .valmer-shell {
            background:
                radial-gradient(circle at top left, rgba(191, 86, 40, 0.18), transparent 30%),
                radial-gradient(circle at top right, rgba(8, 90, 109, 0.16), transparent 28%),
                linear-gradient(180deg, rgba(11, 16, 25, 0.92), rgba(8, 12, 20, 0.98));
            border: 1px solid var(--valmer-panel-border);
            box-shadow: var(--valmer-panel-shadow);
            border-radius: 22px;
            padding: 1.35rem 1.5rem;
            margin-bottom: 1.15rem;
            overflow: hidden;
        }
        .valmer-shell h1 {
            color: var(--valmer-text-strong);
            margin: 0.35rem 0 0.45rem 0;
            letter-spacing: -0.03em;
        }
        .valmer-shell p {
            margin: 0;
            max-width: 52rem;
            color: var(--valmer-text-muted);
            line-height: 1.55;
        }
        .valmer-kpi {
            position: relative;
            overflow: hidden;
            border-radius: 18px;
            padding: 1rem 1rem 1.1rem 1rem;
            border: 1px solid var(--valmer-panel-border);
            background: var(--valmer-panel-bg);
            box-shadow: 0 14px 36px rgba(0, 0, 0, 0.22);
            backdrop-filter: blur(10px);
            min-height: 7.8rem;
        }
        .valmer-kpi::before {
            content: "";
            position: absolute;
            inset: 0 0 auto 0;
            height: 3px;
            background: linear-gradient(90deg, var(--valmer-accent-warm), var(--valmer-accent-cool));
            opacity: 0.95;
        }
        .valmer-kpi h4 {
            margin: 0 0 0.5rem 0;
            font-size: 0.8rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: rgba(232, 238, 246, 0.66);
        }
        .valmer-kpi strong {
            color: var(--valmer-text-strong);
            font-size: 1.9rem;
            display: block;
            margin-bottom: 0.35rem;
            letter-spacing: -0.04em;
        }
        .valmer-kpi div {
            color: var(--valmer-text-muted);
            line-height: 1.45;
        }
        .valmer-badge {
            display: inline-block;
            padding: 0.28rem 0.62rem;
            border-radius: 999px;
            background: rgba(224, 122, 57, 0.16);
            border: 1px solid rgba(224, 122, 57, 0.14);
            color: #ef9d6a;
            font-size: 0.78rem;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_title(title: str, subtitle: str) -> None:
    inject_dashboard_css()
    st.markdown(
        f"""
        <div class="valmer-shell">
          <span class="valmer-badge">Valmer Monitoring</span>
          <h1>{title}</h1>
          <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_cards(cards: list[tuple[str, str, str]]) -> None:
    cols = st.columns(len(cards))
    for col, (label, value, detail) in zip(cols, cards):
        col.markdown(
            f"""
            <div class="valmer-kpi">
              <h4>{label}</h4>
              <strong>{value}</strong>
              <div>{detail}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_empty_header(_: object) -> None:
    return None


def _ensure_flat_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if isinstance(frame.index, pd.MultiIndex) or frame.index.name is not None:
        return frame.reset_index()
    return frame.copy()


def _prepare_vector_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = _ensure_flat_frame(frame)
    for col in DATETIME_VECTOR_COLUMNS:
        if col in frame.columns:
            frame[col] = pd.to_datetime(frame[col], utc=True, errors="coerce")
    for col in NUMERIC_VECTOR_COLUMNS:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    if "unique_identifier" in frame.columns:
        frame["unique_identifier"] = frame["unique_identifier"].astype("string")
    return frame


def _query_node_by_identifier(
    node_identifier: str,
    *,
    lookback_days: int,
    unique_identifier_list: list[str] | None = None,
    columns: list[str] | None = None,
) -> QueryResult:
    start_date = utc_now() - timedelta(days=lookback_days)
    try:
        frame, storage = DataNodeStorage.get_data_between_dates_from_node_identifier(
            node_identifier=node_identifier,
            start_date=start_date,
            end_date=utc_now(),
            great_or_equal=True,
            less_or_equal=True,
            unique_identifier_list=unique_identifier_list,
            columns=columns,
        )
        return QueryResult(
            data=_prepare_vector_frame(frame),
            storage_hash=getattr(storage, "storage_hash", None),
        )
    except Exception as exc:
        return QueryResult(data=pd.DataFrame(), error=str(exc))


@st.cache_data(ttl=300, show_spinner=False)
def load_vector_history(lookback_days: int = 14) -> QueryResult:
    return _query_node_by_identifier(VECTOR_NODE_IDENTIFIER, lookback_days=lookback_days)


def latest_vector_snapshot(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "unique_identifier" not in frame.columns or "time_index" not in frame.columns:
        return frame.copy()
    latest_idx = frame.groupby("unique_identifier")["time_index"].idxmax()
    return frame.loc[latest_idx].sort_values("time_index", ascending=False).reset_index(drop=True)


def render_sidebar_context(
    latest_frame: pd.DataFrame | None = None,
    *,
    allow_asset_lookup: bool = True,
) -> str | None:
    selected_uid: str | None = None

    try:
        sidebar_logged_user_username(
            label="Authenticated user",
            show_organization=True,
        )
    except Exception as exc:
        with st.sidebar:
            st.caption(f"User lookup unavailable: {exc}")

    if allow_asset_lookup:
        try:
            asset = sidebar_asset_single_select(
                title="Find MainSequence asset",
                key_prefix="valmer_asset",
            )
            if asset is not None:
                selected_uid = getattr(asset, "unique_identifier", None)
        except Exception as exc:
            with st.sidebar:
                st.caption(f"Asset selector unavailable: {exc}")

    with st.sidebar:
        if selected_uid and st.session_state.get("valmer_focus_uid") in (None, ""):
            st.session_state["valmer_focus_uid"] = selected_uid

        manual_uid = st.text_input(
            "Focus Valmer UID",
            key="valmer_focus_uid",
            help="Optional direct filter for one Valmer unique_identifier in the current snapshot.",
        ).strip()
        if manual_uid:
            selected_uid = manual_uid

        if latest_frame is not None and not latest_frame.empty and "time_index" in latest_frame.columns:
            latest_obs = latest_frame["time_index"].max()
            st.caption(f"Latest snapshot observation: {latest_obs}")
            if selected_uid and "unique_identifier" in latest_frame.columns:
                in_snapshot = latest_frame["unique_identifier"].astype("string").eq(selected_uid).any()
                if in_snapshot:
                    st.success(f"{selected_uid} is present in the current snapshot.")
                else:
                    st.info(f"{selected_uid} is not present in the current snapshot.")

    return selected_uid


def _query_assets(unique_identifiers: list[str]) -> dict[str, object]:
    if not unique_identifiers:
        return {}

    assets: dict[str, object] = {}
    batch_size = 250
    for start in range(0, len(unique_identifiers), batch_size):
        batch = unique_identifiers[start : start + batch_size]
        batch_assets = msc.Asset.query(unique_identifier__in=batch, per_page=batch_size)
        assets.update({asset.unique_identifier: asset for asset in batch_assets})
    return assets


@st.cache_data(ttl=300, show_spinner=False)
def load_pricing_health(lookback_days: int = 14) -> dict[str, object]:
    history = load_vector_history(lookback_days=lookback_days)
    if history.error:
        return {"error": history.error}

    latest = latest_vector_snapshot(history.data)
    if latest.empty:
        return {"latest": latest, "target": latest, "missing_pricing": []}

    target = ImportValmer._get_target_bonds(latest.copy())
    target_uids = target["unique_identifier"].dropna().astype("string").unique().tolist()
    assets = _query_assets(target_uids)

    missing_pricing: list[str] = []
    for uid in target_uids:
        asset = assets.get(uid)
        pricing_detail = getattr(asset, "current_pricing_detail", None) if asset else None
        if pricing_detail is None or getattr(pricing_detail, "instrument_dump", None) is None:
            missing_pricing.append(uid)

    return {
        "latest": latest,
        "target": target,
        "missing_pricing": missing_pricing,
    }


def _load_standard_curve_history(lookback_days: int = 30) -> QueryResult:
    try:
        instrument_configuration = msc.InstrumentsConfiguration.filter()[0]
        if instrument_configuration.discount_curves_storage_node is None:
            raise RuntimeError("Instruments configuration is missing discount_curves_storage_node.")
        curve_uid = msc.Constant.get_value(name=CURVE_CONST_NAME)
        node = APIDataNode.build_from_table_id(table_id=instrument_configuration.discount_curves_storage_node)
        frame = node.get_ranged_data_per_asset(
            range_descriptor={
                curve_uid: {
                    "start_date": utc_now() - timedelta(days=lookback_days),
                    "start_date_operand": ">=",
                    "end_date": utc_now(),
                    "end_date_operand": "<=",
                }
            }
        ).reset_index()
        if "time_index" in frame.columns:
            frame["time_index"] = pd.to_datetime(frame["time_index"], utc=True, errors="coerce")
        return QueryResult(data=frame, storage_hash=node.storage_hash)
    except Exception as exc:
        return QueryResult(data=pd.DataFrame(), error=str(exc))


@st.cache_data(ttl=300, show_spinner=False)
def load_curve_health(lookback_days: int = 30) -> dict[str, QueryResult]:
    return {
        "standard": _load_standard_curve_history(lookback_days=lookback_days),
        "standalone": _query_node_by_identifier(
            STANDALONE_CURVE_NODE_IDENTIFIER,
            lookback_days=lookback_days,
        ),
    }


def latest_curve_points(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "curve" not in frame.columns:
        return pd.DataFrame()

    latest = frame.sort_values("time_index").iloc[-1]
    payload = latest["curve"]
    if isinstance(payload, dict):
        points = payload
    else:
        points = MexDerTIIE28Zero.decompress_string_to_curve(payload)
    points_frame = (
        pd.Series(points, name="zero_rate")
        .rename_axis("days_to_maturity")
        .reset_index()
        .sort_values("days_to_maturity")
    )
    points_frame["zero_rate"] = pd.to_numeric(points_frame["zero_rate"], errors="coerce")
    return points_frame


def source_activity_counts(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "time_index" not in frame.columns or "unique_identifier" not in frame.columns:
        return pd.DataFrame()
    counts = (
        frame.assign(day=frame["time_index"].dt.date)
        .groupby("day")["unique_identifier"]
        .nunique()
        .rename("asset_count")
        .reset_index()
    )
    return counts


def category_breakdown(frame: pd.DataFrame, column: str, *, top_n: int = 12) -> pd.DataFrame:
    if frame.empty or column not in frame.columns:
        return pd.DataFrame()
    return (
        frame[column]
        .astype("string")
        .fillna("Unknown")
        .value_counts(dropna=False)
        .head(top_n)
        .rename_axis(column)
        .reset_index(name="count")
    )


def top_movers(frame: pd.DataFrame, *, limit: int = 10) -> pd.DataFrame:
    if frame.empty or "daily_change_pct" not in frame.columns:
        return pd.DataFrame()
    movers = frame.copy()
    movers["abs_daily_change"] = movers["daily_change_pct"].abs()
    movers = movers.sort_values("abs_daily_change", ascending=False)
    cols = [
        col
        for col in (
            "unique_identifier",
            "full_name",
            "sector",
            "dirty_price",
            "yield_rate",
            "daily_change_pct",
            "weekly_change_pct",
        )
        if col in movers.columns
    ]
    return movers[cols].head(limit)


def selected_asset_history(frame: pd.DataFrame, unique_identifier: str | None) -> pd.DataFrame:
    if not unique_identifier or frame.empty or "unique_identifier" not in frame.columns:
        return pd.DataFrame()
    return (
        frame[frame["unique_identifier"].astype("string") == unique_identifier]
        .sort_values("time_index")
        .reset_index(drop=True)
    )


def selected_asset_snapshot(frame: pd.DataFrame, unique_identifier: str | None) -> pd.Series | None:
    history = selected_asset_history(frame, unique_identifier)
    if history.empty:
        return None
    return history.iloc[-1]


def runtime_notes() -> list[str]:
    notes = [f"Curve constant wired locally: {', '.join(REQUIRED_CURVE_CONSTS)}."]
    notes.append(f"Pricing index specs wired locally: {', '.join(REQUIRED_INDEX_CONSTS)}.")
    notes.append(
        f"CETE pricing registrations are enabled only when `{OPTIONAL_EXTERNAL_CURVE_CONST}` exists in the runtime."
    )
    return notes
